/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <rte_errno.h>
#include <rte_mbuf_dyn.h>

#include "app_ctx.h"

#include "elephant_common.h"

volatile int g_force_quit = 0;
volatile uint32_t g_epoch = 0;

int elephant_dynfield_offset = -1;

int elephant_dynfield_init(void)
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
	cfg->port_id = 0;
	cfg->n_rx_queues = 1;
	cfg->n_workers = 2;
	cfg->elephant_threshold = ELEPHANT_DEFAULT_THRESHOLD;
	cfg->synthetic = false;
	cfg->time_window = false;
	cfg->time_window_s = 10;
	cfg->stats_period_s = 2;
}
