/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * NitroSketch: per-lcore sketch for elephant-flow byte counting.
 *
 * Reference: "NitroSketch: Robust and General Sketch-based Measurement
 * in Software Switches" (SIGCOMM 2019), https://github.com/yindaz/NitroSketch
 *
 * Key property for the data path: NO locks, NO shared state. Each RX
 * lcore owns its own sketch instance in cache-aligned memory.
 */

#ifndef _NITROSKETCH_H_
#define _NITROSKETCH_H_

#include <stdint.h>
#include <stdbool.h>

#include <rte_common.h>
#include <rte_memory.h>

#include "elephant_common.h"

#define NS_MAX_ROWS       8
#define NS_DEFAULT_ROWS   4
#define NS_DEFAULT_COLS   1024   /* must be a power of two */
#define NS_DEFAULT_RATE   0.90   /* geometric decay of per-row sampling rate */

struct nitrosketch {
	uint32_t rows;
	uint32_t cols;
	uint32_t col_mask;
	/* Per-row sampling rate p_j = base_rate^j (row 0 always updates). */
	double   rate[NS_MAX_ROWS];
	double   inv_rate[NS_MAX_ROWS];
	double   potential[NS_MAX_ROWS];
	uint64_t seed[NS_MAX_ROWS];
	uint64_t *counters;   /* rows * cols counters, in bytes */
} __rte_cache_aligned;

/**
 * Allocate and initialize a sketch.
 *
 * @param rows  number of rows, in [1, NS_MAX_ROWS]
 * @param cols  columns per row, must be a power of two
 * @param base_rate  geometric sampling decay, (0, 1]
 * @param seed  per-instance seed (must differ across lcores)
 * @return 0 on success, -1 on error (counters freed on error)
 */
int ns_init(struct nitrosketch *ns, uint32_t rows, uint32_t cols,
	    double base_rate, uint64_t seed);

/** Free the sketch counters (safe to call twice). */
void ns_free(struct nitrosketch *ns);

/**
 * Fast-path update. Adds pkt_len bytes to the flow's counters using
 * geometric-interarrival sampling: row j is updated with probability
 * p_j = base_rate^j, implemented with a floating-point "potential".
 * Row 0 (p_0 = 1.0) is always updated, so the estimate never relies on
 * sampled rows alone.
 */
static inline void
ns_update(struct nitrosketch *ns, uint64_t flow_hash, uint32_t pkt_len)
{
	for (uint32_t j = 0; j < ns->rows; j++) {
		ns->potential[j] += ns->rate[j];
		if (ns->potential[j] >= 1.0) {
			uint64_t idx =
				elephant_fmix64(flow_hash ^ ns->seed[j]) &
				ns->col_mask;
			ns->counters[(uint64_t)j * ns->cols + idx] += pkt_len;
			ns->potential[j] -= 1.0;
		}
	}
}

/**
 * Fast-path query: minimum-variance estimate of the flow's total bytes.
 * Returns 0 when the flow has never been updated.
 */
static inline uint64_t
ns_estimate(const struct nitrosketch *ns, uint64_t flow_hash)
{
	uint64_t est = UINT64_MAX;

	for (uint32_t j = 0; j < ns->rows; j++) {
		uint64_t idx =
			elephant_fmix64(flow_hash ^ ns->seed[j]) & ns->col_mask;
		uint64_t c = ns->counters[(uint64_t)j * ns->cols + idx];
		uint64_t e = (uint64_t)((double)c * ns->inv_rate[j]);

		if (e < est)
			est = e;
	}
	return est == UINT64_MAX ? 0 : est;
}

#endif /* _NITROSKETCH_H_ */
