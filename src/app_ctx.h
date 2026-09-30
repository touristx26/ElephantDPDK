/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * Shared application context: runtime configuration and the global
 * force_quit flag (set from the signal handler in main.c, polled by
 * all lcore main loops).
 */

#ifndef _APP_CTX_H_
#define _APP_CTX_H_

#include <stdint.h>
#include <stdbool.h>

struct app_config {
	uint16_t port_id;         /* NIC port used in real-NIC mode      */
	uint16_t n_rx_queues;      /* number of RX queues (and RX lcores) */
	uint16_t n_workers;        /* number of worker rings/lcores       */
	uint64_t elephant_threshold; /* per-flow bytes before elephant   */
	bool synthetic;            /* generate flows in software (no NIC) */
	unsigned stats_period_s;  /* stats print interval, main lcore    */
};

/* Set in app_ctx.c, written by the signal handler / main lcore. */
extern volatile int g_force_quit;

static inline bool
app_force_quit(void)
{
	return __atomic_load_n(&g_force_quit, __ATOMIC_RELAXED);
}

void app_force_quit_set(void);

/* Fill config with defaults. */
void app_config_init_defaults(struct app_config *cfg);

#endif /* _APP_CTX_H_ */
