/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <string.h>

#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_mbuf.h>
#include <rte_ring.h>

#include "worker.h"
#include "app_ctx.h"

/* 模拟 DPI: 对 payload 做一次特征扫描 + 可配置忙等 */
static inline void
worker_dpi_process(struct elephant_worker *w, struct rte_mbuf *m)
{
	uint32_t len = m->data_len;

	if (len > 0) {
		uint8_t *data = rte_pktmbuf_mtod(m, uint8_t *);

		(void)memchr(data, 0x42, len);
	}
	if (w->dpi_us > 0)
		rte_delay_us(w->dpi_us);
}

static inline void
worker_process_burst(struct elephant_worker *w,
		     struct rte_mbuf **pkts, uint16_t n)
{
	uint64_t bytes = 0;

	for (uint16_t i = 0; i < n; i++) {
		bytes += pkts[i]->pkt_len;
		worker_dpi_process(w, pkts[i]);
		rte_pktmbuf_free(pkts[i]);
	}

	w->stats.packets += n;
	w->stats.bytes += bytes;
}

int
elephant_worker_lcore_main(void *arg)
{
	struct elephant_worker *w = arg;
	struct rte_mbuf *pkts[ELEPHANT_BURST_MAX];
	unsigned int lcore_id = rte_lcore_id();

	RTE_LOG(INFO, EDPDK,
		"worker %u on lcore %u: draining ring '%s', dpi=%u us/pkt "
		"(capacity ~= %.2f Mpps)\n",
		w->id, lcore_id, w->ring->name, w->dpi_us,
		w->dpi_us > 0 ? 1.0 / (double)w->dpi_us : 0.0);

	while (likely(!app_force_quit())) {
		uint16_t n = (uint16_t)rte_ring_dequeue_burst(
			w->ring, (void **)pkts, ELEPHANT_BURST_MAX, NULL);

		if (n == 0)
			continue;
		__atomic_fetch_add(w->deq_cntr, n, __ATOMIC_RELAXED);
		worker_process_burst(w, pkts, n);
	}

	/* 退出前把 ring 排空 */
	uint16_t n;

	while ((n = (uint16_t)rte_ring_dequeue_burst(
			w->ring, (void **)pkts, ELEPHANT_BURST_MAX, NULL)) > 0) {
		__atomic_fetch_add(w->deq_cntr, n, __ATOMIC_RELAXED);
		worker_process_burst(w, pkts, n);
	}

	RTE_LOG(INFO, EDPDK, "worker %u on lcore %u: exiting\n",
		w->id, lcore_id);
	return 0;
}
