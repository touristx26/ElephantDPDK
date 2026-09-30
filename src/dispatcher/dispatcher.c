/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <stdio.h>

#include <rte_log.h>
#include <rte_ring.h>

#include "dispatcher.h"

int
elephant_dispatcher_init(struct elephant_dispatcher *d,
			  uint32_t n_workers, bool single_producer,
			  int socket)
{
	if (d == NULL || n_workers == 0 || n_workers > ELEPHANT_MAX_WORKERS) {
		RTE_LOG(ERR, EDPDK, "invalid worker count %u\n", n_workers);
		return -1;
	}

	memset(d, 0, sizeof(*d));
	d->n_rings = n_workers;
	d->power_of_two = (n_workers & (n_workers - 1)) == 0;
	if (d->power_of_two)
		d->ring_mask = n_workers - 1;

	unsigned flags = 0;
	if (single_producer)
		flags |= RING_F_SP_ENQ | RING_F_SC_DEQ;

	char name[RTE_RING_NAMESIZE];
	for (uint32_t w = 0; w < n_workers; w++) {
		snprintf(name, sizeof(name), "wk_ring_%u", w);
		d->rings[w] = rte_ring_create(name, ELEPHANT_RING_SIZE,
					      socket, flags);
		if (d->rings[w] == NULL) {
			RTE_LOG(ERR, EDPDK, "failed to create ring %s\n", name);
			elephant_dispatcher_free(d);
			return -1;
		}
	}

	RTE_LOG(INFO, EDPDK,
		"dispatcher: %u worker ring(s) of depth %u, mode %s\n",
		n_workers, ELEPHANT_RING_SIZE,
		single_producer ? "SP/SC" : "MP/MC");
	return 0;
}

void
elephant_dispatcher_free(struct elephant_dispatcher *d)
{
	if (d == NULL)
		return;
	for (uint32_t w = 0; w < d->n_rings; w++) {
		rte_ring_free(d->rings[w]);
		d->rings[w] = NULL;
	}
	d->n_rings = 0;
}

uint64_t
elephant_dispatcher_drops(const struct elephant_dispatcher *d)
{
	uint64_t total = 0;

	for (uint32_t w = 0; w < d->n_rings; w++)
		total += __atomic_load_n(&d->drops[w], __ATOMIC_RELAXED);
	return total;
}
