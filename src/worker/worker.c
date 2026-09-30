/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <rte_common.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_mbuf.h>
#include <rte_ring.h>

#include "worker.h"
#include "app_ctx.h"

static inline void
worker_process_burst(struct elephant_worker *w,
		     struct rte_mbuf **pkts, uint16_t n)
{
	uint64_t pkts_ = 0, bytes = 0, epkts = 0, ebytes = 0;

	for (uint16_t i = 0; i < n; i++) {
		struct rte_mbuf *m = pkts[i];
		uint64_t len = m->pkt_len;

		pkts_++;
		bytes += len;
		if (elephant_mbuf_is_elephant(m)) {
			epkts++;
			ebytes += len;
			/*
			 * Elephant fast-path handling goes here. The flow
			 * hash is available via elephant_mbuf_flow_hash(m).
			 */
		}
		rte_pktmbuf_free(m);
	}

	w->stats.packets += pkts_;
	w->stats.bytes += bytes;
	w->stats.elephant_packets += epkts;
	w->stats.elephant_bytes += ebytes;
}

int
elephant_worker_lcore_main(void *arg)
{
	struct elephant_worker *w = arg;
	struct rte_mbuf *pkts[ELEPHANT_BURST_MAX];
	unsigned int lcore_id = rte_lcore_id();

	RTE_LOG(INFO, EDPDK,
		"worker %u on lcore %u: draining ring '%s'\n",
		w->id, lcore_id, w->ring->name);

	while (likely(!app_force_quit())) {
		uint16_t n = (uint16_t)rte_ring_dequeue_burst(
			w->ring, (void **)pkts, ELEPHANT_BURST_MAX, NULL);
		if (n == 0)
			continue;
		worker_process_burst(w, pkts, n);
	}

	/* Final drain so the pipeline ends empty. */
	uint16_t n;
	while ((n = (uint16_t)rte_ring_dequeue_burst(
			w->ring, (void **)pkts, ELEPHANT_BURST_MAX, NULL)) > 0)
		worker_process_burst(w, pkts, n);

	RTE_LOG(INFO, EDPDK, "worker %u on lcore %u: exiting\n",
		w->id, lcore_id);
	return 0;
}
