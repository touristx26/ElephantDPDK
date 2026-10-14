/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <string.h>

#include <rte_errno.h>
#include <rte_mbuf_dyn.h>

#include "app_ctx.h"

#include "elephant_common.h"

volatile int g_force_quit = 0;

int elephant_dynfield_offset = -1;

int
elephant_dynfield_init(void)
{
	static const struct rte_mbuf_dynfield meta_desc = {
		.name = "elephantdpdk_flow_meta",
		.size = sizeof(uint64_t),
		.align = __alignof__(uint64_t),
	};

	elephant_dynfield_offset = rte_mbuf_dynfield_register(&meta_desc);
	if (elephant_dynfield_offset < 0) {
		RTE_LOG(ERR, EDPDK,
			"failed to register mbuf dynfield: %s\n",
			rte_strerror(rte_errno));
		return -1;
	}
	RTE_LOG(INFO, EDPDK, "flow metadata dynfield offset = %d\n",
		elephant_dynfield_offset);
	return 0;
}

void
app_force_quit_set(void)
{
	__atomic_store_n(&g_force_quit, 1, __ATOMIC_RELAXED);
}

void
app_config_init_defaults(struct app_config *cfg)
{
	memset(cfg, 0, sizeof(*cfg));
	cfg->port_id = 0;
	cfg->n_rx_queues = 1;
	cfg->n_workers = 2;
	cfg->synthetic = false;

	/*
	 * 默认实验参数(2 worker, 1 RX lcore):
	 *   消费能力 = 2 / 10us = 200K pps
	 *   生产速率 100K -> 200K 爬升(10K/s), 约 10s 越过消费能力,
	 *   再约 1s 填满 4096 深的 ring -> 约 11s 发生第一次溢出
	 */
	cfg->dpi_us = 10;
	cfg->gen_pps = 100000;
	cfg->gen_ramp = 10000;
	cfg->gen_sawtooth_s = 0;

	cfg->sample_us = 100;
	cfg->pred_window = 512;
	cfg->csv_path[0] = '\0';
	cfg->stats_period_s = 2;
}
