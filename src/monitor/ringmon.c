/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <inttypes.h>
#include <math.h>
#include <stdio.h>

#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_ring.h>

#include "ringmon.h"
#include "app_ctx.h"

static inline double
tsc_to_ms(uint64_t tsc, uint64_t tsc_hz)
{
	return (double)tsc / (double)tsc_hz * 1000.0;
}

static inline double
tsc_to_s(uint64_t tsc, uint64_t tsc_hz)
{
	return (double)tsc / (double)tsc_hz;
}

int
ringmon_init(struct ringmon *mon, struct rte_ring *ring,
	     struct elephant_dispatcher *disp, uint32_t ring_idx,
	     uint32_t pred_window)
{
	memset(mon, 0, sizeof(*mon));
	mon->ring = ring;
	mon->disp = disp;
	mon->cntrs = &disp->cntrs[ring_idx];
	mon->ring_idx = ring_idx;
	mon->ring_capacity = rte_ring_get_capacity(ring);
	mon->pred_window = pred_window;
	mon->last_sample_tsc = rte_rdtsc();
	mon->trial_start_tsc = mon->last_sample_tsc;
	mon->prev_enq = __atomic_load_n(&mon->cntrs->enq_pkts,
					__ATOMIC_RELAXED);
	mon->prev_deq = __atomic_load_n(&mon->cntrs->deq_pkts,
					__ATOMIC_RELAXED);
	mon->last_drops = __atomic_load_n(&disp->drops[ring_idx],
					  __ATOMIC_RELAXED);

	int socket = rte_socket_id();

	mon->samples = rte_zmalloc_socket("rm_samples",
		RINGMON_SAMPLE_CAP * sizeof(*mon->samples),
		RTE_CACHE_LINE_SIZE, socket);
	mon->preds = rte_zmalloc_socket("rm_preds",
		RINGMON_PRED_CAP * sizeof(*mon->preds),
		RTE_CACHE_LINE_SIZE, socket);
	mon->win_t_ms = rte_zmalloc_socket("rm_wint",
		pred_window * sizeof(double), 0, socket);
	mon->win_depth = rte_zmalloc_socket("rm_wind",
		pred_window * sizeof(uint32_t), 0, socket);
	if (mon->samples == NULL || mon->preds == NULL ||
	    mon->win_t_ms == NULL || mon->win_depth == NULL) {
		ringmon_free(mon);
		return -1;
	}
	return 0;
}

void
ringmon_free(struct ringmon *mon)
{
	if (mon == NULL)
		return;
	rte_free(mon->samples);
	rte_free(mon->preds);
	rte_free(mon->win_t_ms);
	rte_free(mon->win_depth);
	mon->samples = NULL;
	mon->preds = NULL;
	mon->win_t_ms = NULL;
	mon->win_depth = NULL;
}

static inline double
rate_ttf_ms(uint32_t capacity, uint32_t depth, double growth_pps)
{
	if (capacity <= depth || growth_pps <= 1e-9)
		return -1.0;	/* 无增长趋势或已满: 无有效预测 */
	return (double)(capacity - depth) / growth_pps * 1000.0;
}

void
ringmon_sample(struct ringmon *mon, uint64_t now)
{
	uint64_t tsc_hz = rte_get_tsc_hz();
	uint64_t enq = __atomic_load_n(&mon->cntrs->enq_pkts,
					__ATOMIC_RELAXED);
	uint64_t deq = __atomic_load_n(&mon->cntrs->deq_pkts,
					__ATOMIC_RELAXED);
	uint32_t depth = rte_ring_count(mon->ring);
	double dt_s = tsc_to_s(now - mon->last_sample_tsc, tsc_hz);

	mon->last_sample_tsc = now;

	/* ---- 三核心指标: counter 差值 -> 速率 ---- */
	double arrival = dt_s > 0.0 ?
			 (double)(enq - mon->prev_enq) / dt_s : 0.0;
	double service = dt_s > 0.0 ?
			 (double)(deq - mon->prev_deq) / dt_s : 0.0;
	double growth = arrival - service;

	mon->prev_enq = enq;
	mon->prev_deq = deq;

	mon->metrics.depth = depth;
	mon->metrics.depth_pct = mon->ring_capacity > 0 ?
		(uint32_t)(100.0 * (double)depth /
			   (double)mon->ring_capacity) : 0;
	mon->metrics.arrival_pps = arrival;
	mon->metrics.service_pps = service;
	mon->metrics.growth_pps = growth;

	/* ---- depth 历史记录(legacy 算法拟合输入) ---- */
	struct ringmon_sample *s = &mon->samples[mon->sample_head];

	s->tsc = now;
	s->depth = depth;
	mon->sample_head = (mon->sample_head + 1) % RINGMON_SAMPLE_CAP;
	if (mon->sample_len < RINGMON_SAMPLE_CAP)
		mon->sample_len++;

	/* ---- 全部预测算法 ---- */
	struct ringmon_pred *p = &mon->preds[mon->pred_head];

	p->tsc = now;
	p->valid_mask = 0;

	/* rate: 本周期速率差直接外推(当前阶段核心算法) */
	double ttf = rate_ttf_ms(mon->ring_capacity, depth, growth);

	if (ttf >= 0.0) {
		p->ttf_ms[RINGMON_ALGO_RATE] = ttf;
		p->valid_mask |= 1u << RINGMON_ALGO_RATE;
	}

	/* linreg / ewma / holt: depth 序列拟合(对照算法) */
	if (mon->sample_len >= 2) {
		uint32_t w = mon->pred_window;

		if (w > mon->sample_len)
			w = mon->sample_len;

		uint32_t start = (mon->sample_head + RINGMON_SAMPLE_CAP - w) %
				RINGMON_SAMPLE_CAP;
		double t0 = tsc_to_ms(mon->samples[start].tsc, tsc_hz);

		for (uint32_t i = 0; i < w; i++) {
			uint32_t idx = (start + i) % RINGMON_SAMPLE_CAP;

			mon->win_t_ms[i] =
				tsc_to_ms(mon->samples[idx].tsc, tsc_hz) - t0;
			mon->win_depth[i] = mon->samples[idx].depth;
		}

		if (pred_linreg(mon->win_t_ms, mon->win_depth, w,
			       mon->ring_capacity,
			       &p->ttf_ms[PRED_LINREG]) == 0)
			p->valid_mask |= 1u << PRED_LINREG;
		if (pred_ewma(mon->win_t_ms, mon->win_depth, w,
			      mon->ring_capacity,
			      &p->ttf_ms[PRED_EWMA]) == 0)
			p->valid_mask |= 1u << PRED_EWMA;
		if (pred_holt(mon->win_t_ms, mon->win_depth, w,
			      mon->ring_capacity,
			      &p->ttf_ms[PRED_HOLT]) == 0)
			p->valid_mask |= 1u << PRED_HOLT;
	}

	mon->pred_head = (mon->pred_head + 1) % RINGMON_PRED_CAP;
	if (mon->pred_len < RINGMON_PRED_CAP)
		mon->pred_len++;

	/* ---- 满载风险判断: depth / arrival / service 三指标 ---- */
	double ttf_ms = (p->valid_mask & (1u << RINGMON_ALGO_RATE)) ?
			p->ttf_ms[RINGMON_ALGO_RATE] : -1.0;

	mon->metrics.at_risk =
		mon->metrics.depth_pct >= RINGMON_RISK_DEPTH_PCT &&
		growth > 0.0 &&
		ttf_ms >= 0.0 && ttf_ms <= RINGMON_RISK_TTF_MS;

	if (mon->metrics.at_risk &&
	    tsc_to_ms(now - mon->last_alert_tsc, tsc_hz) >=
	    RINGMON_ALERT_INT_MS) {
		mon->last_alert_tsc = now;
		printf("[ring %u] ALERT: depth=%u/%u (%u%%), "
		       "arrival=%.0f pps, service=%.0f pps, growth=%+.0f pps, "
		       "ttf=%.0f ms\n",
		       mon->ring_idx, depth, mon->ring_capacity,
		       mon->metrics.depth_pct, arrival, service, growth,
		       ttf_ms);
		fflush(stdout);
	}
}

bool
ringmon_check_overflow(struct ringmon *mon)
{
	uint64_t drops = __atomic_load_n(&mon->disp->drops[mon->ring_idx],
					 __ATOMIC_RELAXED);
	bool increased = drops > mon->last_drops;

	if (increased)
		mon->last_drops = drops;

	/* 上升沿才算新事件: 持续溢出阶段不重复结算 trial */
	bool event = increased && !mon->in_overflow;

	mon->in_overflow = increased;
	return event;
}

void
ringmon_finish_trial(struct ringmon *mon, uint64_t now)
{
	uint64_t tsc_hz = rte_get_tsc_hz();
	double t_ofl = tsc_to_ms(now, tsc_hz);

	double mae[RINGMON_ALGO_COUNT] = {0.0};
	double bias[RINGMON_ALGO_COUNT] = {0.0};
	double mae_near[RINGMON_ALGO_COUNT] = {0.0};
	double first_lead[RINGMON_ALGO_COUNT];
	uint32_t n[RINGMON_ALGO_COUNT] = {0};
	uint32_t n_near[RINGMON_ALGO_COUNT] = {0};

	for (uint32_t a = 0; a < RINGMON_ALGO_COUNT; a++)
		first_lead[a] = -1.0;

	uint32_t cnt = mon->pred_len;
	uint32_t start = (mon->pred_head + RINGMON_PRED_CAP - cnt) %
			 RINGMON_PRED_CAP;

	for (uint32_t i = 0; i < cnt; i++) {
		const struct ringmon_pred *p =
			&mon->preds[(start + i) % RINGMON_PRED_CAP];

		if (p->tsc <= mon->trial_start_tsc)
			continue;

		double t = tsc_to_ms(p->tsc, tsc_hz);
		double lead = t_ofl - t;

		for (uint32_t a = 0; a < RINGMON_ALGO_COUNT; a++) {
			if (!(p->valid_mask & (1u << a)))
				continue;

			double err = (t + p->ttf_ms[a]) - t_ofl;

			mae[a] += fabs(err);
			bias[a] += err;
			n[a]++;
			if (lead <= RINGMON_NEAR_MS) {
				mae_near[a] += fabs(err);
				n_near[a]++;
			}
			if (first_lead[a] < 0.0 || lead > first_lead[a])
				first_lead[a] = lead;
		}
	}

	printf("[ring %u] OVERFLOW trial #%u @ %.0f ms:",
	       mon->ring_idx, mon->trials + 1, t_ofl);
	for (uint32_t a = 0; a < RINGMON_ALGO_COUNT; a++) {
		if (n[a] > 0)
			printf(" %s[n=%u MAE=%.0fms nearMAE=%.0fms bias=%+.0fms lead=%.0fms]",
			       ringmon_algo_name(a), n[a], mae[a] / n[a],
			       n_near[a] > 0 ? mae_near[a] / n_near[a] : -1.0,
			       bias[a] / n[a], first_lead[a]);
		else
			printf(" %s[n=0]", ringmon_algo_name(a);
	}
	printf("\n");
	fflush(stdout);

	for (uint32_t a = 0; a < RINGMON_ALGO_COUNT; a++) {
		mon->sum_mae_ms[a] += mae[a];
		mon->sum_bias_ms[a] += bias[a];
		mon->sum_mae_near_ms[a] += mae_near[a];
		mon->n_pred[a] += n[a];
		mon->n_pred_near[a] += n_near[a];
		if (n[a] > 0) {
			mon->sum_lead_ms[a] += first_lead[a];
			mon->n_valid_trial[a]++;
		}
	}
	mon->trials++;
	mon->overflow_events++;
	mon->trial_start_tsc = now;

	mon->pred_len = 0;
	mon->pred_head = 0;
}

/* CSV 行(由 monitor lcore 写, glibc FILE* 内部有锁) */
static void
ringmon_csv_row(struct ringmon *mon, uint64_t start_tsc, uint64_t now,
		FILE *csv)
{
	const struct ringmon_pred *p =
		&mon->preds[(mon->pred_head + RINGMON_PRED_CAP - 1) %
			    RINGMON_PRED_CAP];
	uint64_t tsc_hz = rte_get_tsc_hz();

	fprintf(csv, "%.2f,%u,%u,%u,%" PRIu64 ",%" PRIu64 ",%.0f,%.0f,%+.0f,"
		"%.1f,%.1f,%.1f,%.1f\n",
		tsc_to_ms(now - start_tsc, tsc_hz),
		mon->ring_idx, mon->metrics.depth, mon->metrics.depth_pct,
		mon->prev_enq, mon->prev_deq,
		mon->metrics.arrival_pps, mon->metrics.service_pps,
		mon->metrics.growth_pps,
		(p->valid_mask & (1u << RINGMON_ALGO_RATE)) ?
		p->ttf_ms[RINGMON_ALGO_RATE] : -1.0,
		(p->valid_mask & (1u << PRED_LINREG)) ?
		p->ttf_ms[PRED_LINREG] : -1.0,
		(p->valid_mask & (1u << PRED_EWMA)) ?
		p->ttf_ms[PRED_EWMA] : -1.0,
		(p->valid_mask & (1u << PRED_HOLT)) ?
		p->ttf_ms[PRED_HOLT] : -1.0);
}

int
ringmon_lcore_main(void *arg)
{
	struct ringmon_set *set = arg;
	uint64_t tsc_hz = rte_get_tsc_hz();
	uint64_t period = (uint64_t)((double)set->sample_us *
				      (double)tsc_hz / 1e6);

	if (period == 0)
		period = 1;

	uint64_t start = rte_rdtsc();
	uint64_t next = start + period;
	unsigned int lcore_id = rte_lcore_id();

	RTE_LOG(INFO, EDPDK,
		"monitor on lcore %u: sampling %u ring(s) every %u us\n",
		lcore_id, set->n_mons, set->sample_us);

	while (likely(!app_force_quit())) {
		uint64_t now = rte_rdtsc();

		if (now < next)
			continue;	/* TSC 忙等至采样边界(精确周期) */

		/* 落后多个周期时追帧, 防止漂移 */
		while (now >= next)
			next += period;

		for (uint32_t i = 0; i < set->n_mons; i++) {
			struct ringmon *mon = &set->mons[i];

			ringmon_sample(mon, now);

			if (ringmon_check_overflow(mon))
				ringmon_finish_trial(mon, now);

			if (set->csv != NULL)
				ringmon_csv_row(mon, start, now, set->csv);
		}
	}

	RTE_LOG(INFO, EDPDK, "monitor on lcore %u: exiting\n", lcore_id);
	return 0;
}
