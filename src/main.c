/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * QLF(Queue Load Forecasting)实验入口 —— feature/qlf 分支。
 *
 * 目标: 预测 dispatcher -> worker 的 rte_ring 何时被打爆(溢出),
 * 在受控实验场景下对比多种预测算法的性能。
 *
 * 数据通路(生产 -> 消费):
 *   RX lcore(合成流量, 令牌桶限速: 初始 pps + 线性爬升)
 *     -> flow hash -> dispatcher -> worker ring
 *     -> worker(模拟 DPI: payload 扫描 + 忙等, 消费能力 = 1/dpi_us)
 *
 * 实验控制与测量(main lcore):
 *   - 独立 monitor lcore 每 sample_us 采样各 ring depth 与
 *     enq/deq 计数器快照, 计算 arrival/service/growth 三指标,
 *     运行全部预测算法
 *   - dispatcher per-ring drop 计数器上升沿 = 真实溢出时刻
 *   - 每次溢出结算一个 trial: 各算法 MAE / bias / 首警提前量
 *   - 退出时输出算法性能汇总表(可另存 CSV 全量数据)
 *
 * lcore 布局: main(实验控制) + n_rx_queues 个 RX + n_workers 个 worker。
 */

#include <errno.h>
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
#include <rte_version.h>

#include "app_ctx.h"
#include "elephant_common.h"
#include "dispatcher/dispatcher.h"
#include "rx/rx.h"
#include "worker/worker.h"
#include "monitor/ringmon.h"
#include "predict/predict.h"

int edpdk_logtype;

/*
 * DPDK 21.11 将 ethdev 的 ETH_* 常量更名为 RTE_ETH_*(旧名保留为
 * deprecated 别名)。此处按版本选名, 保证 20.11 ~ 最新可编译。
 */
#if RTE_VERSION < RTE_VERSION_NUM(21, 11, 0, 0)
#define EDPDK_MQ_RX_RSS    ETH_MQ_RX_RSS
#define EDPDK_RSS_OFFLOADS (ETH_RSS_IP | ETH_RSS_TCP | ETH_RSS_UDP)
#define EDPDK_LINK_UP      ETH_LINK_UP
#else
#define EDPDK_MQ_RX_RSS    RTE_ETH_MQ_RX_RSS
#define EDPDK_RSS_OFFLOADS (RTE_ETH_RSS_IP | RTE_ETH_RSS_TCP | RTE_ETH_RSS_UDP)
#define EDPDK_LINK_UP      RTE_ETH_LINK_UP
#endif

static void
signal_handler(int signum)
{
	if (signum == SIGINT || signum == SIGTERM) {
		printf("\nSignal %d received, preparing to exit...\n", signum);
		app_force_quit_set();
	}
}

/* ------------------------- 参数解析 ------------------------- */

enum {
	OPT_DPI_US = 1000,
	OPT_GEN_PPS,
	OPT_GEN_RAMP,
	OPT_GEN_SAWTOOTH,
	OPT_SAMPLE_US,
	OPT_PRED_WINDOW,
	OPT_CSV,
};

static void
print_usage(const char *prog)
{
	printf("Usage: %s [EAL options] -- [app options]\n"
	       "App options (QLF experiment):\n"
	       "  -p, --port <id>          NIC port id (default 0)\n"
	       "  -q, --rx-queues <n>      number of RX queues / RX lcores (default 1)\n"
	       "  -w, --workers <n>        number of workers (default 2)\n"
	       "  -s, --synthetic          software traffic generator, no NIC needed\n"
	       "  -i, --stats-period <s>   run-status print interval in seconds (default 2)\n"
	       "      --dpi-us <us>         worker DPI processing time per packet (default 10)\n"
	       "                            worker capacity ~= workers / dpi_us pps\n"
	       "      --gen-pps <pps>       initial synthetic rate per RX lcore (default 100000)\n"
	       "                            0 with no ramp/sawtooth = unlimited stress\n"
	       "      --gen-ramp <pps/s>    rate ramp (default 10000)\n"
	       "      --gen-sawtooth <s>    reset the ramp every s seconds: repeated fill/drain\n"
	       "                            cycles, multiple overflow trials in one run (default 0)\n"
	       "      --sample-us <us>      ring sampling interval on the monitor lcore\n"
	       "                            (default 100)\n"
	       "      --pred-window <n>    prediction fitting window in samples (default 512)\n"
	       "      --csv <path>          write samples + predictions to a CSV file\n"
	       "  -h, --help               show this help\n",
	       prog);
}

static int
parse_app_args(int argc, char **argv, struct app_config *cfg)
{
	static const struct option longopts[] = {
		{"port",         required_argument, NULL, 'p'},
		{"rx-queues",    required_argument, NULL, 'q'},
		{"workers",      required_argument, NULL, 'w'},
		{"synthetic",    no_argument,       NULL, 's'},
		{"stats-period", required_argument, NULL, 'i'},
		{"dpi-us",       required_argument, NULL, OPT_DPI_US},
		{"gen-pps",      required_argument, NULL, OPT_GEN_PPS},
		{"gen-ramp",     required_argument, NULL, OPT_GEN_RAMP},
		{"gen-sawtooth", required_argument, NULL, OPT_GEN_SAWTOOTH},
		{"sample-us",    required_argument, NULL, OPT_SAMPLE_US},
		{"pred-window",  required_argument, NULL, OPT_PRED_WINDOW},
		{"csv",          required_argument, NULL, OPT_CSV},
		{"help",         no_argument,       NULL, 'h'},
		{NULL, 0, NULL, 0}
	};
	int opt;
	const char *prog = (argc > 0 && argv[0] != NULL) ? argv[0]
							 : "elephantdpdk";

	while ((opt = getopt_long(argc, argv, "p:q:w:si:h", longopts,
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
		case OPT_DPI_US:
			v = atol(optarg);
			if (v < 0 || v > 10000) {
				fprintf(stderr, "invalid dpi-us: %s\n",
					optarg);
				return -1;
			}
			cfg->dpi_us = (uint32_t)v;
			break;
		case OPT_GEN_PPS:
			v = atol(optarg);
			if (v < 0) {
				fprintf(stderr, "invalid gen-pps: %s\n",
					optarg);
				return -1;
			}
			cfg->gen_pps = (uint32_t)v;
			break;
		case OPT_GEN_RAMP:
			v = atol(optarg);
			if (v < 0) {
				fprintf(stderr, "invalid gen-ramp: %s\n",
					optarg);
				return -1;
			}
			cfg->gen_ramp = (uint32_t)v;
			break;
		case OPT_GEN_SAWTOOTH:
			v = atol(optarg);
			if (v < 0) {
				fprintf(stderr, "invalid gen-sawtooth: %s\n",
					optarg);
				return -1;
			}
			cfg->gen_sawtooth_s = (uint32_t)v;
			break;
		case OPT_SAMPLE_US:
			v = atol(optarg);
			if (v < 1 || v > 1000000) {
				fprintf(stderr, "invalid sample-us: %s\n",
					optarg);
				return -1;
			}
			cfg->sample_us = (uint32_t)v;
			break;
		case OPT_PRED_WINDOW:
			v = atol(optarg);
			if (v < 4 || (uint32_t)v > RINGMON_SAMPLE_CAP) {
				fprintf(stderr, "invalid pred-window: %s\n",
					optarg);
				return -1;
			}
			cfg->pred_window = (uint32_t)v;
			break;
		case OPT_CSV:
			snprintf(cfg->csv_path, sizeof(cfg->csv_path),
				 "%s", optarg);
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

/* ------------------------- 端口初始化(NIC 模式) ------------------------- */

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
		uint64_t rss_hf = EDPDK_RSS_OFFLOADS;

		rss_hf &= dev_info.flow_type_rss_offloads;
		if (rss_hf == 0) {
			RTE_LOG(WARNING, EDPDK,
				"port %u: no supported RSS offloads, "
				"flows will hash to queue 0 only\n", port);
		}
		port_conf.rxmode.mq_mode = EDPDK_MQ_RX_RSS;
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
		port, link.link_status == EDPDK_LINK_UP ? "up" : "down",
		link.link_speed, n_rx_queues);
	return 0;
}

/* ------------------------- 运行状态打印 ------------------------- */

static void
print_run_status(struct ringmon *mons, uint16_t n_rings,
		 struct elephant_worker *workers,
		 struct elephant_rx_ctx *rx_ctxs, uint16_t n_rx,
		 double dt_s, uint64_t *last_wpkts, uint64_t *last_rxpkts)
{
	uint64_t rx_pkts = 0;

	for (uint16_t q = 0; q < n_rx; q++)
		rx_pkts += rx_ctxs[q].stats.packets;

	double gen_mpps = dt_s > 0.0 ?
		(double)(rx_pkts - *last_rxpkts) / dt_s / 1e6 : 0.0;

	*last_rxpkts = rx_pkts;

	printf("---- gen %.2f Mpps | ", gen_mpps);
	for (uint16_t w = 0; w < n_rings; w++) {
		const struct ringmon_metrics *m = &mons[w].metrics;
		uint64_t wpkts = workers[w].stats.packets;
		double w_mpps = dt_s > 0.0 ?
			(double)(wpkts - last_wpkts[w]) / dt_s / 1e6 : 0.0;

		last_wpkts[w] = wpkts;

		printf("ring%u[%u%% arr%.2fM svc%.2fM grow%+.2fM%s]%s",
		       w, m->depth_pct,
		       m->arrival_pps / 1e6, m->service_pps / 1e6,
		       m->growth_pps / 1e6,
		       m->at_risk ? " RISK" : "",
		       w + 1 < n_rings ? " " : " | ");
	}

	/* rate 预测取自 monitor 的最新预测 */
	const struct ringmon_pred *p =
		&mons[0].preds[(mons[0].pred_head + RINGMON_PRED_CAP - 1) %
			       RINGMON_PRED_CAP];

	printf("ttf:");
	for (uint32_t a = 0; a < RINGMON_ALGO_COUNT; a++) {
		if (p->valid_mask & (1u << a))
			printf(" %s=%.0fms", ringmon_algo_name(a),
			       p->ttf_ms[a]);
		else
			printf(" %s=-", ringmon_algo_name(a));
	}
	printf(" | drops %" PRIu64 " ----\n",
	       elephant_dispatcher_drops(mons[0].disp));
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
	struct ringmon *mons = NULL;
	FILE *csv = NULL;
	unsigned int rx_lcores[ELEPHANT_MAX_RX_QUEUES];
	unsigned int worker_lcores[ELEPHANT_MAX_WORKERS];
	unsigned int lcore;
	int ret;

	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	/* EAL 初始化 */
	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "invalid EAL arguments\n");
	argc -= ret;
	argv += ret;

	edpdk_logtype = rte_log_register("elephantdpdk");
	if (edpdk_logtype < 0) {
		edpdk_logtype = RTE_LOGTYPE_USER1;
		RTE_LOG(WARNING, EDPDK,
			"logtype registration failed; using USER1\n");
	} else {
		rte_log_set_level((uint32_t)edpdk_logtype, RTE_LOG_INFO);
	}

	/* mbuf dynfield(dispatcher 路由用的 flow hash 载体) */
	if (elephant_dynfield_init() != 0)
		rte_exit(EXIT_FAILURE, "dynfield registration failed\n");

	app_config_init_defaults(&cfg);
	if (parse_app_args(argc, argv, &cfg) != 0)
		rte_exit(EXIT_FAILURE, "invalid application arguments\n");

	RTE_LOG(INFO, EDPDK,
		"QLF experiment: mode=%s rx_queues=%u workers=%u | "
		"producer: pps=%u ramp=%u/s sawtooth=%us | "
		"consumer: dpi=%u us/pkt (capacity ~= %.2f Mpps) | "
		"monitor: sampling every %u us, window=%u\n",
		cfg.synthetic ? "synthetic" : "NIC", cfg.n_rx_queues,
		cfg.n_workers, cfg.gen_pps, cfg.gen_ramp,
		cfg.gen_sawtooth_s, cfg.dpi_us,
		cfg.dpi_us > 0 ?
		(double)cfg.n_workers / (double)cfg.dpi_us : 0.0,
		cfg.sample_us, cfg.pred_window);

	/* lcore 角色分配: main + n_rx 个 RX + n_worker 个 worker + 1 个 monitor */
	unsigned int n_rx = 0, n_wk = 0;
	unsigned int monitor_lcore = RTE_MAX_LCORE;

	RTE_LCORE_FOREACH(lcore) {
		if (lcore == rte_get_main_lcore())
			continue;
		if (n_rx < cfg.n_rx_queues)
			rx_lcores[n_rx++] = lcore;
		else if (n_wk < cfg.n_workers)
			worker_lcores[n_wk++] = lcore;
		else if (monitor_lcore == RTE_MAX_LCORE)
			monitor_lcore = lcore;
	}
	if (n_rx < cfg.n_rx_queues || n_wk < cfg.n_workers ||
	    monitor_lcore == RTE_MAX_LCORE)
		rte_exit(EXIT_FAILURE,
			 "not enough lcores: need %u RX + %u worker + 1 "
			 "monitor lcore besides the main lcore\n",
			 cfg.n_rx_queues, cfg.n_workers);

	/* 共享 mempool(单 socket demo) */
	pool = rte_pktmbuf_pool_create("eleph_pool", ELEPHANT_NB_MBUF,
				       ELEPHANT_MEMPOOL_CACHE,
				       0, RTE_MBUF_DEFAULT_BUF_SIZE,
				       rte_socket_id());
	if (pool == NULL)
		rte_exit(EXIT_FAILURE, "cannot create mbuf pool\n");

	/* 端口初始化(synthetic 模式跳过) */
	if (!cfg.synthetic) {
		if (rte_eth_dev_count_avail() == 0)
			rte_exit(EXIT_FAILURE,
				 "no Ethernet ports found "
				 "(use --synthetic for a software demo)\n");
		if (port_init(cfg.port_id, cfg.n_rx_queues, pool) != 0)
			rte_exit(EXIT_FAILURE, "port %u init failed\n",
				 cfg.port_id);
	}

	/* 各上下文分配 */
	disp = rte_zmalloc_socket("disp", sizeof(*disp),
				   RTE_CACHE_LINE_SIZE, rte_socket_id());
	workers = rte_zmalloc_socket("workers",
				      sizeof(*workers) * cfg.n_workers,
				      RTE_CACHE_LINE_SIZE, rte_socket_id());
	rx_ctxs = rte_zmalloc_socket("rx_ctxs",
				      sizeof(*rx_ctxs) * cfg.n_rx_queues,
				      RTE_CACHE_LINE_SIZE, rte_socket_id());
	mons = rte_zmalloc_socket("mons",
				  sizeof(*mons) * cfg.n_workers,
				  RTE_CACHE_LINE_SIZE, rte_socket_id());
	if (disp == NULL || workers == NULL || rx_ctxs == NULL ||
	    mons == NULL)
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
		workers[w].dpi_us = cfg.dpi_us;
		workers[w].ring = disp->rings[w];
		workers[w].deq_cntr = &disp->cntrs[w].deq_pkts;
	}

	for (uint16_t w = 0; w < cfg.n_workers; w++) {
		ret = ringmon_init(&mons[w], disp->rings[w], disp, w,
				   cfg.pred_window);
		if (ret != 0)
			rte_exit(EXIT_FAILURE, "ringmon init failed\n");
	}

	/* 可选 CSV 输出(由 monitor lcore 写) */
	if (cfg.csv_path[0] != '\0') {
		csv = fopen(cfg.csv_path, "w");
		if (csv == NULL) {
			RTE_LOG(WARNING, EDPDK,
				"cannot open %s for writing: %s\n",
				cfg.csv_path, strerror(errno));
		} else {
			fprintf(csv,
				"t_ms,ring,depth,depth_pct,enq_pkts,deq_pkts,"
				"arrival_pps,service_pps,growth_pps,"
				"ttf_rate,ttf_linreg,ttf_ewma,ttf_holt\n");
		}
	}

	RTE_LOG(INFO, EDPDK, "starting pipeline (Ctrl-C to quit)\n");

	/* 启动各 lcore: RX + worker + monitor(采样/预测/结算) */
	struct ringmon_set mon_set = {
		.mons = mons,
		.n_mons = cfg.n_workers,
		.sample_us = cfg.sample_us,
		.csv = csv,
	};

	for (uint16_t q = 0; q < cfg.n_rx_queues; q++)
		rte_eal_remote_launch(elephant_rx_lcore_main,
				      &rx_ctxs[q], rx_lcores[q]);
	for (uint16_t w = 0; w < cfg.n_workers; w++)
		rte_eal_remote_launch(elephant_worker_lcore_main,
				      &workers[w], worker_lcores[w]);
	rte_eal_remote_launch(ringmon_lcore_main, &mon_set, monitor_lcore);

	/* main lcore 主循环: 只负责运行状态打印 */
	uint64_t tsc_hz = rte_get_tsc_hz();
	uint64_t stats_ticks = (uint64_t)cfg.stats_period_s * tsc_hz;
	uint64_t next_stats = rte_rdtsc() + stats_ticks;
	uint64_t last_wpkts[ELEPHANT_MAX_WORKERS] = {0};
	uint64_t last_rxpkts = 0;

	while (!app_force_quit()) {
		uint64_t now = rte_rdtsc();

		if (now >= next_stats) {
			next_stats = now + stats_ticks;
			print_run_status(mons, cfg.n_workers, workers,
					 rx_ctxs, cfg.n_rx_queues,
					 (double)stats_ticks / (double)tsc_hz,
					 last_wpkts, &last_rxpkts);
		}

		rte_delay_us_sleep(500);
	}

	/* 收尾: 等待全部 lcore 退出 */
	RTE_LOG(INFO, EDPDK, "shutting down...\n");
	rte_eal_mp_wait_lcore();

	/* 最终实验报告 */
	uint64_t total_rx = 0, total_w = 0;

	for (uint16_t q = 0; q < cfg.n_rx_queues; q++)
		total_rx += rx_ctxs[q].stats.packets;
	for (uint16_t w = 0; w < cfg.n_workers; w++)
		total_w += workers[w].stats.packets;

	printf("\n============ QLF 实验最终报告 ============\n");
	printf("生产: %" PRIu64 " pkts | 消费: %" PRIu64 " pkts | "
	       "drops: %" PRIu64 "\n",
	       total_rx, total_w, elephant_dispatcher_drops(disp));

	/* 跨 ring 聚合各算法指标 */
	double sum_mae[RINGMON_ALGO_COUNT] = {0.0};
	double sum_bias[RINGMON_ALGO_COUNT] = {0.0};
	double sum_mae_near[RINGMON_ALGO_COUNT] = {0.0};
	double sum_lead[RINGMON_ALGO_COUNT] = {0.0};
	uint32_t n_pred[RINGMON_ALGO_COUNT] = {0};
	uint32_t n_near[RINGMON_ALGO_COUNT] = {0};
	uint32_t n_lead[RINGMON_ALGO_COUNT] = {0};
	uint32_t trials = 0;

	for (uint16_t w = 0; w < cfg.n_workers; w++) {
		trials += mons[w].trials;
		for (uint32_t a = 0; a < RINGMON_ALGO_COUNT; a++) {
			sum_mae[a] += mons[w].sum_mae_ms[a];
			sum_bias[a] += mons[w].sum_bias_ms[a];
			sum_mae_near[a] += mons[w].sum_mae_near_ms[a];
			sum_lead[a] += mons[w].sum_lead_ms[a];
			n_pred[a] += mons[w].n_pred[a];
			n_near[a] += mons[w].n_pred_near[a];
			n_lead[a] += mons[w].n_valid_trial[a];
		}
	}

	for (uint16_t w = 0; w < cfg.n_workers; w++)
		printf("ring %u: 溢出 trial %u 次, 持续溢出样本内 drop 计数 %"
		       PRIu64 "\n", w, mons[w].trials, mons[w].last_drops);

	printf("\n算法性能汇总(共 %u 个 trial):\n", trials);
	printf("%-8s %8s %10s %10s %14s %14s\n",
	       "algo", "n_pred", "MAE(ms)", "bias(ms)",
	       "nearMAE(ms)", "first_lead(ms)");
	for (uint32_t a = 0; a < RINGMON_ALGO_COUNT; a++) {
		printf("%-8s %8u %10.1f %10.1f %14.1f %14.1f\n",
		       ringmon_algo_name(a), n_pred[a],
		       n_pred[a] > 0 ? sum_mae[a] / (double)n_pred[a] : -1.0,
		       n_pred[a] > 0 ? sum_bias[a] / (double)n_pred[a] : -1.0,
		       n_near[a] > 0 ?
		       sum_mae_near[a] / (double)n_near[a] : -1.0,
		       n_lead[a] > 0 ?
		       sum_lead[a] / (double)n_lead[a] : -1.0);
	}
	printf("===========================================\n");

	if (csv != NULL) {
		fclose(csv);
		printf("CSV 已写入 %s\n", cfg.csv_path);
	}

	for (uint16_t w = 0; w < cfg.n_workers; w++)
		ringmon_free(&mons[w]);
	elephant_dispatcher_free(disp);
	rte_free(mons);
	rte_free(rx_ctxs);
	rte_free(workers);
	rte_free(disp);
	rte_mempool_free(pool);

	return 0;
}
