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

/* Direct-mapped table to avoid re-announcing the same elephant flow. */
#define RX_ANNOUNCE_TABLE 256u

struct elephant_rx_stats {
	uint64_t packets;
	uint64_t bytes;
	uint64_t elephant_packets;
	uint64_t elephant_flows;   /* distinct flows announced at least once */
} __rte_cache_aligned;

struct elephant_rx_ctx {
	/* configuration (read-only after init) */
	uint16_t port_id;
	uint16_t queue_id;
	uint64_t elephant_threshold;
	bool synthetic;
	struct rte_mempool *pool;
	struct elephant_dispatcher *disp;

	/* per-lcore state (no sharing => no locks) */
	struct nitrosketch sketch;
	uint64_t lcg;              /* synthetic-mode RNG state */
	uint64_t announced[RX_ANNOUNCE_TABLE];

	/* statistics (written by this lcore, read by main) */
	struct elephant_rx_stats stats;
};

/**
 * Initialize an RX context (including its private sketch).
 * @seed should be unique per lcore.
 */
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
