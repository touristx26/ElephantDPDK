/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <errno.h>
#include <math.h>
#include <string.h>

#include <rte_log.h>
#include <rte_malloc.h>

#include "nitrosketch.h"

int
ns_init(struct nitrosketch *ns, uint32_t rows, uint32_t cols,
	double base_rate, uint64_t seed)
{
	if (ns == NULL || rows == 0 || rows > NS_MAX_ROWS ||
	    cols == 0 || (cols & (cols - 1)) != 0 ||
	    base_rate <= 0.0 || base_rate > 1.0) {
		RTE_LOG(ERR, EDPDK, "invalid NitroSketch parameters\n");
		return -1;
	}

	memset(ns, 0, sizeof(*ns));
	ns->rows = rows;
	ns->cols = cols;
	ns->col_mask = cols - 1;

	size_t sz = (size_t)rows * cols * sizeof(uint64_t);
	ns->counters = rte_zmalloc_socket("ns_counters", sz,
					  RTE_CACHE_LINE_SIZE,
					  SOCKET_ID_ANY);
	if (ns->counters == NULL) {
		RTE_LOG(ERR, EDPDK, "failed to allocate %zu bytes for sketch\n",
			sz);
		return -1;
	}

	for (uint32_t j = 0; j < rows; j++) {
		ns->rate[j] = pow(base_rate, (double)j);
		ns->inv_rate[j] = 1.0 / ns->rate[j];
		ns->potential[j] = 0.0;
		/* Per-row salt so that row indices are decorrelated. */
		ns->seed[j] = seed * 0x9e3779b97f4a7c15ULL +
			      (uint64_t)j * 0xbf58476d1ce4e5b9ULL;
		ns->seed[j] = elephant_fmix64(ns->seed[j]);
	}

	RTE_LOG(INFO, EDPDK,
		"nitrosketch: rows=%u cols=%u base_rate=%.2f "
		"(row0 always sampled, row%u rate=%.4f)\n",
		rows, cols, base_rate, rows - 1, ns->rate[rows - 1]);
	return 0;
}

void
ns_free(struct nitrosketch *ns)
{
	if (ns == NULL)
		return;
	rte_free(ns->counters);
	ns->counters = NULL;
}
