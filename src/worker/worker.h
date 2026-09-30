/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * Workers are the consumers of the dispatch rings. Each worker lcore
 * owns exactly one ring (single consumer) and drains it in bursts.
 *
 * In this demo, "handling" an elephant flow means reading the flag
 * from the mbuf metadata dynfield and keeping per-flow-class statistics.
 * A real application would branch here (e.g. steer the packet to a slow
 * path, a different TX queue, or a capture buffer) without touching any
 * RX-lcore state.
 */

#ifndef _WORKER_H_
#define _WORKER_H_

#include <stdint.h>

#include <rte_ring.h>
#include <rte_mbuf.h>

#include "elephant_common.h"

struct elephant_worker_stats {
	uint64_t packets;          /* total packets consumed          */
	uint64_t bytes;            /* total bytes consumed            */
	uint64_t elephant_packets; /* packets carrying the elephant flag */
	uint64_t elephant_bytes;
} __rte_cache_aligned;

struct elephant_worker {
	uint32_t id;
	struct rte_ring *ring;
	struct elephant_worker_stats stats;
};

/**
 * Worker lcore main loop (run via rte_eal_remote_launch).
 * Consumes its ring until the global force_quit flag is set, then
 * drains the ring once more and returns 0.
 */
int elephant_worker_lcore_main(void *arg);

#endif /* _WORKER_H_ */
