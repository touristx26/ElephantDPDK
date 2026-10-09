/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * RX fast path (per RX lcore, owns one RX queue):
 *
 *   rte_eth_rx_burst (or synthetic generator)
 *     -> flow key extraction (Ethernet/VLAN/IPv4/IPv6 + TCP/UDP ports)
 *     -> 63-bit flow hash
 *     -> NitroSketch update + estimate (per-lcore, lock-free)
 *     -> elephant flag + flow hash stored in the mbuf dynfield (udata64-style)
 *     -> hash dispatcher -> worker ring
 */

#ifndef _RX_H_
#define _RX_H_

#include <stdint.h>
#include <stdbool.h>

#include <rte_mbuf.h>
#include <rte_mempool.h>

#include "app_ctx.h"
#include "elephant_common.h"
#include "nitrosketch/nitrosketch.h"
#include "dispatcher/dispatcher.h"

/*
 * Time-window mode: dual-sketch rotation.
 * Each RX lcore owns two sketches, [epoch & 1] is the current
 * (active, write) sketch; the other is the retired snapshot (read-only)
 * of the previous window. The main lcore periodically increments a
 * global epoch (broadcast via atomic add), and each RX lcore observes
 * the epoch change at burst granularity: it clears the retired sketch
 * and swaps roles. Estimation reads the CURRENT sketch (cumulative-so-far
 * within the window). A slow trickle (keepalive-like) flow never
 * accumulates enough bytes within a single window to be declared elephant.
 */
struct elephant_rx_ctx {
	uint16_t port_id;
	uint16_t queue_id;
	uint64_t elephant_threshold;
	bool synthetic;
	bool time_window;
	uint32_t lcore_epoch;       /* last observed global epoch         */
	struct rte_mempool *pool;
	struct elephant_dispatcher *disp;
	struct nitrosketch sketch[2];  /* [0]=current, [1]=retired (rotated) */

	uint64_t lcg;
	uint64_t announced[256];

	struct elephant_rx_stats stats;
};

int elephant_rx_ctx_init(struct elephant_rx_ctx *ctx,
			 const struct app_config *cfg,
			 uint16_t port_id, uint16_t queue_id,
			 struct rte_mempool *pool,
			 struct elephant_dispatcher *disp,
			 uint64_t seed);
void elephant_rx_ctx_free(struct elephant_rx_ctx *ctx);

/** RX lcore main loop (run via rte_eal_remote_launch). */
int elephant_rx_lcore_main(void *arg);

#endif /* _RX_H_ */
