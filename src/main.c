/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * ElephantDPDK demo entry point.
 *
 * Pipeline (per RX lcore, no locks on the fast path):
 *
 *   RXQ -> Flow Key/Hash Extraction -> NitroSketch -> Elephant Flag
 *       (mbuf dynfield, udata64-style) -> Hash Dispatcher -> Ring -> Worker
 *
 * Lcore layout: main lcore runs stats/control, the next n_rx_queues
 * lcores poll the RX queues, the remaining n_workers lcores drain the
 * dispatch rings.
 */

#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ring.h>

#include "app_ctx.h"
#include "elephant_common.h"
#include "nitrosketch/nitrosketch.h"
#include "dispatcher/dispatcher.h"
#include "rx/rx.h"
#include "worker/worker.h"

int edpdk_logtype;

static void
signal_handler(int signum)
{
	if (signum == SIGINT || signum == SIGTERM) {
		printf("\nSignal %d received, preparing to exit...\n", signum);
		app_force_quit_set();
	}
}

/* ------------------------- argument parsing ------------------------- */

static void
print_usage(const char *prog)
{
	printf("Usage: %s [EAL options] -- [app options]\n"
	       "App options:\n"
	       "  -p, --port <id>        NIC port id (default 0)\n"
	       "  -q, --rx-queues <n>    number of RX queues / RX lcores (default 1)\n"
	       "  -w, --workers <n>      number of workers (default 2)\n"
	       "  -t, --threshold <b>    per-flow elephant threshold in bytes (default 1MiB)\n"
	       "  -s, --synthetic        software traffic generator, no NIC needed\n"
	       "  -i, --stats-period <s> stats print interval in seconds (default 2)\n"
	       "  -h, --help             show this help\n",
	       prog);
}

static int
parse_app_args(int argc, char **argv, struct app_config *cfg)
{
	static const struct option longopts[] = {
		{"port",         required_argument, NULL, 'p'},
		{"rx-queues",    required_argument, NULL, 'q'},
		{"workers",      required_argument, NULL, 'w'},
		{"threshold",    required_argument, NULL, 't'},
		{"synthetic",    no_argument,       NULL, 's'},
		{"stats-period", required_argument, NULL, 'i'},
		{"help",         no_argument,       NULL, 'h'},
		{NULL, 0, NULL, 0}
	};
	int opt;
	const char *prog = (argc > 0 && argv[0] != NULL) ? argv[0]
							 : "elephantdpdk";

	while ((opt = getopt_long(argc, argv, "p:q:w:t:si:h", longopts,
				   NULL)) != -1) {
		long v;

		switch (opt) {
		case 'p':
			cfg->port_id = (uint16_t)atoi(optarg);
			break;
		case 'q':
			v = atol(optarg);
			if (v < 1 || v > ELEPHANT_MAX_RX_QUEUES) {
				fprintf(stderr, "invalid rx queue count: %s\n",
					optarg);
				return -1;
			}
			cfg->n_rx_queues = (uint16_t)v;
			break;
		case 'w':
			v = atol(optarg);
			if (v < 1 || v > ELEPHANT_MAX_WORKERS) {
				fprintf(stderr, "invalid worker count: %s\n",
					optarg);
				return -1;
			}
			cfg->n_workers = (uint16_t)v;
			break;
		case 't':
			v = atol(optarg);
			if (v < 1) {
				fprintf(stderr, "invalid threshold: %s\n",
					optarg);
				return -1;
			}
			cfg->elephant_threshold = (uint64_t)v;
			break;
		case 's':
			cfg->synthetic = true;
			break;
		case 'i':
			v = atol(optarg);
			if (v < 1) {
				fprintf(stderr, "invalid stats period: %s\n",
					optarg);
				return -1;
			}
			cfg->stats_period_s = (unsigned)v;
			break;
		case 'h':
			print_usage(prog);
			exit(0);
		default:
			print_usage(prog);
			return -1;
		}
	}
	return 0;
}

/* ----------------------------- port setup ---------------------------- */

static int
port_init(uint16_t port, uint16_t n_rx_queues, struct rte_mempool *pool)
{
	struct rte_eth_dev_info dev_info;
	struct rte_eth_conf port_conf;
	struct rte_eth_link link;
	uint16_t rx_desc = ELEPHANT_RX_DESC_DEFAULT;
	int ret;

	ret = rte_eth_dev_info_get(port, &dev_info);
	if (ret != 0) {
		RTE_LOG(ERR, EDPDK, "rte_eth_dev_info_get(port=%u): %s\n",
			port, rte_strerror(-ret));
		return ret;
	}

	if (n_rx_queues > dev_info.max_rx_queues) {
		RTE_LOG(ERR, EDPDK,
			"port %u supports at most %u RX queues (asked %u)\n",
			port, dev_info.max_rx_queues, n_rx_queues);
		return -1;
	}

	memset(&port_conf, 0, sizeof(port_conf));
	if (n_rx_queues > 1) {
		uint64_t rss_hf = ETH_RSS_IP | ETH_RSS_TCP | ETH_RSS_UDP;

		rss_hf &= dev_info.flow_type_rss_offloads;
		if (rss_hf == 0) {
			RTE_LOG(WARNING, EDPDK,
				"port %u: no supported RSS offloads, "
				"flows will hash to queue 0 only\n", port);
		}
		port_conf.rxmode.mq_mode = ETH_MQ_RSS;
		port_conf.rx_adv_conf.rss_conf.rss_hf = rss_hf;
	}

	ret = rte_eth_dev_configure(port, n_rx_queues, 0, &port_conf);
	if (ret != 0) {
		RTE_LOG(ERR, EDPDK, "rte_eth_dev_configure: %s\n",
			rte_strerror(-ret));
		return ret;
	}

	if (dev_info.rx_desc_lim.nb_max != 0 &&
	    rx_desc > dev_info.rx_desc_lim.nb_max)
		rx_desc = dev_info.rx_desc_lim.nb_max;

	int socket = rte_socket_id();
	for (uint16_t q = 0; q < n_rx_queues; q++) {
		ret = rte_eth_rx_queue_setup(port, q, rx_desc, socket, NULL,
					     pool);
		if (ret != 0) {
			RTE_LOG(ERR, EDPDK,
				"rte_eth_rx_queue_setup(port=%u q=%u): %s\n",
				port, q, rte_strerror(-ret));
			return ret;
		}
	}

	ret = rte_eth_dev_start(port);
	if (ret != 0) {
		RTE_LOG(ERR, EDPDK, "rte_eth_dev_start: %s\n",
			rte_strerror(-ret));
		return ret;
	}

	ret = rte_eth_promiscuous_enable(port);
	if (ret != 0) {
		RTE_LOG(WARNING, EDPDK,
			"failed to enable promiscuous mode on port %u\n",
			port);
	}

	rte_eth_link_get_nowait(port, &link);
	RTE_LOG(INFO, EDPDK,
		"port %u up: %s, speed %u Mbps, %u RX queue(s)\n",
		port, link.link_status == ETH_LINK_UP ? "up" : "down",
		link.link_speed, n_rx_queues);
	return 0;
}

/* ------------------------------- stats ------------------------------- */

static void
print_stats(const struct elephant_rx_ctx *rx_ctxs, uint16_t n_rx,
	    const struct elephant_dispatcher *disp,
	    const struct elephant_worker *workers, uint16_t n_workers,
	    double interval_s, uint64_t *prev_rx_pkts)
{
	uint64_t pkts = 0, bytes = 0, epkts = 0, eflows = 0;
	uint64_t wpkts = 0, wepkts = 0, webytes = 0;

	for (uint16_t q = 0; q < n_rx; q++) {
		const struct elephant_rx_stats *s = &rx_ctxs[q].stats;

		pkts += s->packets;
		bytes += s->bytes;
		epkts += s->elephant_packets;
		eflows += s->elephant_flows;
	}
	for (uint16_t w = 0; w < n_workers; w++) {
		const struct elephant_worker_stats *s = &workers[w].stats;

		wpkts += s->packets;
		wepkts += s->elephant_packets;
		webytes += s->elephant_bytes;
	}

	double mb = (double)bytes / (1.0 * 1024 * 1024);
	double epct = pkts ? 100.0 * (double)epkts / (double)pkts : 0.0;
	uint64_t d_pkts = pkts - *prev_rx_pkts;
	double pps = interval_s > 0.0 ? (double)d_pkts / interval_s : 0.0;

	*prev_rx_pkts = pkts;

	printf("---- RX: %" PRIu64 " pkts (%.1f Mpps, %.1f MB) | "
	       "elephant: %" PRIu64 " pkts (%.2f%%) in %" PRIu64
	       " flow(s) | workers consumed %" PRIu64 " pkts "
	       "(elephant %" PRIu64 ", %.1f MB) | drops %" PRIu64 " ----\n",
	       pkts, pps / 1e6, mb, epkts, epct, eflows,
	       wpkts, wepkts, (double)webytes / (1.0 * 1024 * 1024),
	       elephant_dispatcher_drops(disp));
	fflush(stdout);
}

/* -------------------------------- main -------------------------------- */

int
main(int argc, char *argv[])
{
	struct app_config cfg;
	struct rte_mempool *pool = NULL;
	struct elephant_dispatcher *disp = NULL;
	struct elephant_rx_ctx *rx_ctxs = NULL;
	struct elephant_worker *workers = NULL;
	unsigned int rx_lcores[ELEPHANT_MAX_RX_QUEUES];
	unsigned int worker_lcores[ELEPHANT_MAX_WORKERS];
	unsigned int lcore;
	int ret;

	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	/* EAL init */
	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "invalid EAL arguments\n");
	argc -= ret;
	argv += ret;

	edpdk_logtype = rte_log_register_type_and_pick_level("elephantdpdk",
							      RTE_LOG_INFO);

	/* Register the per-packet metadata dynfield (udata64 successor). */
	if (elephant_dynfield_init() != 0)
		rte_exit(EXIT_FAILURE, "dynfield registration failed\n");

	app_config_init_defaults(&cfg);
	if (parse_app_args(argc, argv, &cfg) != 0)
		rte_exit(EXIT_FAILURE, "invalid application arguments\n");

	RTE_LOG(INFO, EDPDK,
		"ElephantDPDK demo: mode=%s rx_queues=%u workers=%u "
		"threshold=%" PRIu64 " bytes\n",
		cfg.synthetic ? "synthetic" : "NIC", cfg.n_rx_queues,
		cfg.n_workers, cfg.elephant_threshold);

	/* Assign lcore roles: main + n_rx_queues RX lcores + n_workers. */
	unsigned int n_rx = 0, n_wk = 0;

	RTE_LCORE_FOREACH(lcore) {
		if (lcore == rte_get_main_lcore())
			continue;
		if (n_rx < cfg.n_rx_queues)
			rx_lcores[n_rx++] = lcore;
		else if (n_wk < cfg.n_workers)
			worker_lcores[n_wk++] = lcore;
	}
	if (n_rx < cfg.n_rx_queues || n_wk < cfg.n_workers)
		rte_exit(EXIT_FAILURE,
			 "not enough lcores: need %u RX + %u worker lcores "
			 "besides the main lcore\n",
			 cfg.n_rx_queues, cfg.n_workers);

	/* Shared mempool (single socket demo). */
	pool = rte_pktmbuf_pool_create("eleph_pool", ELEPHANT_NB_MBUF,
				       ELEPHANT_MEMPOOL_CACHE,
				       0, RTE_MBUF_DEFAULT_BUF_SIZE,
				       rte_socket_id());
	if (pool == NULL)
		rte_exit(EXIT_FAILURE, "cannot create mbuf pool\n");

	/* Port setup (skipped in synthetic mode). */
	if (!cfg.synthetic) {
		if (rte_eth_dev_count_avail() == 0)
			rte_exit(EXIT_FAILURE,
				 "no Ethernet ports found "
				 "(use --synthetic for a software demo)\n");
		if (port_init(cfg.port_id, cfg.n_rx_queues, pool) != 0)
			rte_exit(EXIT_FAILURE, "port %u init failed\n",
				 cfg.port_id);
	}

	/* Dispatcher + worker contexts. */
	disp = rte_zmalloc("disp", sizeof(*disp), RTE_CACHE_LINE_SIZE,
			   rte_socket_id());
	workers = rte_zmalloc("workers", sizeof(*workers) * cfg.n_workers,
			       RTE_CACHE_LINE_SIZE, rte_socket_id());
	rx_ctxs = rte_zmalloc("rx_ctxs", sizeof(*rx_ctxs) * cfg.n_rx_queues,
			       RTE_CACHE_LINE_SIZE, rte_socket_id());
	if (disp == NULL || workers == NULL || rx_ctxs == NULL)
		rte_exit(EXIT_FAILURE, "out of memory for contexts\n");

	ret = elephant_dispatcher_init(disp, cfg.n_workers,
				       cfg.n_rx_queues == 1, rte_socket_id());
	if (ret != 0)
		rte_exit(EXIT_FAILURE, "dispatcher init failed\n");

	for (uint16_t q = 0; q < cfg.n_rx_queues; q++) {
		uint64_t seed = 0x1234567890abcdefULL +
				(uint64_t)rx_lcores[q] * 0x100000001b3ULL;

		ret = elephant_rx_ctx_init(&rx_ctxs[q], &cfg, cfg.port_id, q,
					   pool, disp, seed);
		if (ret != 0)
			rte_exit(EXIT_FAILURE, "rx ctx init failed\n");
	}

	for (uint16_t w = 0; w < cfg.n_workers; w++) {
		workers[w].id = w;
		workers[w].ring = disp->rings[w];
	}

	RTE_LOG(INFO, EDPDK, "starting pipeline (Ctrl-C to quit)\n");

	/* Launch lcores. */
	for (uint16_t q = 0; q < cfg.n_rx_queues; q++)
		rte_eal_remote_launch(elephant_rx_lcore_main,
				      &rx_ctxs[q], rx_lcores[q]);
	for (uint16_t w = 0; w < cfg.n_workers; w++)
		rte_eal_remote_launch(elephant_worker_lcore_main,
				      &workers[w], worker_lcores[w]);

	/* Stats loop on the main lcore. */
	uint64_t tsc_hz = rte_get_tsc_hz();
	uint64_t last_tsc = rte_rdtsc();
	uint64_t prev_rx_pkts = 0;

	while (!app_force_quit()) {
		for (unsigned i = 0; i < cfg.stats_period_s * 10 &&
		     !app_force_quit(); i++)
			rte_delay_us_sleep(100 * 1000);

		uint64_t now = rte_rdtsc();
		double dt = (double)(now - last_tsc) / (double)tsc_hz;

		print_stats(rx_ctxs, cfg.n_rx_queues, disp, workers,
			    cfg.n_workers, dt, &prev_rx_pkts);
		last_tsc = now;
	}

	/* Shutdown: wait for all lcores, then final report. */
	RTE_LOG(INFO, EDPDK, "shutting down...\n");
	rte_eal_mp_wait_lcore();

	printf("============ ElephantDPDK final report ============\n");
	for (uint16_t q = 0; q < cfg.n_rx_queues; q++) {
		const struct elephant_rx_stats *s = &rx_ctxs[q].stats;

		printf("RX q%u: %" PRIu64 " pkts, %" PRIu64 " bytes, "
		       "%" PRIu64 " elephant pkts, %" PRIu64
		       " elephant flows\n",
		       q, s->packets, s->bytes, s->elephant_packets,
		       s->elephant_flows);
	}
	for (uint16_t w = 0; w < cfg.n_workers; w++) {
		const struct elephant_worker_stats *s = &workers[w].stats;

		printf("Worker %u: %" PRIu64 " pkts, %" PRIu64
		       " elephant pkts, %" PRIu64 " elephant bytes\n",
		       w, s->packets, s->elephant_packets,
		       s->elephant_bytes);
	}
	printf("Dispatcher drops: %" PRIu64 "\n",
	       elephant_dispatcher_drops(disp));
	printf("===================================================\n");

	for (uint16_t q = 0; q < cfg.n_rx_queues; q++)
		elephant_rx_ctx_free(&rx_ctxs[q]);
	elephant_dispatcher_free(disp);
	rte_free(rx_ctxs);
	rte_free(workers);
	rte_free(disp);
	rte_mempool_free(pool);

	return 0;
}
