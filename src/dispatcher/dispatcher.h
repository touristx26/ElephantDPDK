/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * Hash dispatcher: maps each packet's flow hash to a worker ring.
 *
 * Fast-path properties:
 *   - Lock-free rte_ring (SP/SC when there is a single RX lcore,
 *     MP/MC otherwise — still lock-free, CAS based).
 *   - Same-flow packets always land on the same worker (flow affinity).
 *   - Packets are staged per-worker on the stack and enqueued in
 *     bulks, so the ring is touched once per worker per burst.
 */

#ifndef _DISPATCHER_H_
#define _DISPATCHER_H_

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include <rte_ring.h>
#include <rte_mbuf.h>

#include "elephant_common.h"

struct elephant_dispatcher {
	struct rte_ring *rings[ELEPHANT_MAX_WORKERS];
	uint32_t n_rings;
	uint32_t ring_mask;      /* valid only when n_rings is a power of 2 */
	bool power_of_two;
	/* Cold path: dropped packets due to full rings (atomic, relaxed). */
	uint64_t drops[ELEPHANT_MAX_WORKERS];
};

/**
 * Create n_workers rings.
 *
 * @param n_workers  number of workers, in [1, ELEPHANT_MAX_WORKERS];
 *                   if not a power of two a modulo is used instead of a mask
 * @param single_producer  true when exactly one RX lcore feeds the rings,
 *                        enabling SP/SC (cheapest) ring mode
 * @param socket  NUMA node for ring allocation
 */
int elephant_dispatcher_init(struct elephant_dispatcher *d,
			      uint32_t n_workers, bool single_producer,
			      int socket);

void elephant_dispatcher_free(struct elephant_dispatcher *d);

uint64_t elephant_dispatcher_drops(const struct elephant_dispatcher *d);

static inline uint32_t
elephant_dispatch_target(const struct elephant_dispatcher *d,
			 uint64_t flow_hash)
{
	if (d->power_of_two)
		return (uint32_t)(flow_hash >> 32) & d->ring_mask;
	return (uint32_t)(flow_hash >> 32) % d->n_rings;
}

/**
 * Dispatch a burst of packets. Packets already carry the flow hash and
 * elephant flag in the mbuf metadata dynfield (set by the RX fast path).
 *
 * Returns the number of packets dropped because a ring was full.
 */
static inline uint16_t
elephant_dispatch_burst(struct elephant_dispatcher *d,
		       struct rte_mbuf **pkts, uint16_t n)
{
	/* Small per-call staging area; sized for the worst case. */
	struct rte_mbuf *stage[ELEPHANT_MAX_WORKERS][ELEPHANT_BURST_MAX];
	uint16_t n_staged[ELEPHANT_MAX_WORKERS];
	uint16_t drops = 0;

	memset(n_staged, 0, sizeof(n_staged));

	/* Pass 1: group packets by target worker (pure cache-local work). */
	for (uint16_t i = 0; i < n; i++) {
		uint32_t w = elephant_dispatch_target(
			d, elephant_mbuf_flow_hash(pkts[i]));
		stage[w][n_staged[w]++] = pkts[i];
	}

	/* Pass 2: one bulk enqueue per worker. */
	for (uint32_t w = 0; w < d->n_rings; w++) {
		if (n_staged[w] == 0)
			continue;
		uint16_t enq = (uint16_t)rte_ring_enqueue_burst(
			d->rings[w], (void *const *)stage[w], n_staged[w],
			NULL);
		if (enq < n_staged[w]) {
			uint16_t dropped = n_staged[w] - enq;
			drops += dropped;
			__atomic_fetch_add(&d->drops[w], dropped,
					   __ATOMIC_RELAXED);
			for (uint16_t k = enq; k < n_staged[w]; k++)
				rte_pktmbuf_free(stage[w][k]);
		}
	}

	return drops;
}

#endif /* _DISPATCHER_H_ */
