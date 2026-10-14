/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <math.h>
#include <stdbool.h>

#include "predict.h"

const char *pred_algo_names[PRED_ALGO_COUNT] = {
	"linreg", "ewma", "holt",
};

/* 由填充速率导出 TTF。占用率已达容量时 TTF=0(立即打爆)。 */
static int
ttf_from_rate(uint32_t capacity, uint32_t occ_now, double rate_per_ms,
	      double *ttf_ms)
{
	if (capacity <= occ_now) {
		*ttf_ms = 0.0;
		return 0;
	}
	if (rate_per_ms <= 1e-9)
		return -1;	/* 无上升趋势, 不存在打爆语义 */
	*ttf_ms = (double)(capacity - occ_now) / rate_per_ms;
	return 0;
}

int
pred_linreg(const double *t_ms, const uint32_t *occ, uint32_t n,
	    uint32_t capacity, double *ttf_ms)
{
	double sx = 0.0, sy = 0.0, sxx = 0.0, sxy = 0.0;

	for (uint32_t i = 0; i < n; i++) {
		double x = t_ms[i];
		double y = (double)occ[i];

		sx += x;
		sy += y;
		sxx += x * x;
		sxy += x * y;
	}

	double denom = (double)n * sxx - sx * sx;
	if (denom <= 1e-12)
		return -1;

	double slope = ((double)n * sxy - sx * sy) / denom;
	return ttf_from_rate(capacity, occ[n - 1], slope, ttf_ms);
}

int
pred_ewma(const double *t_ms, const uint32_t *occ, uint32_t n,
	  uint32_t capacity, double *ttf_ms)
{
	const double alpha = 0.3;	/* 平滑系数: 越大越跟随近期 */
	double ewma = 0.0;
	bool have = false;

	for (uint32_t i = 1; i < n; i++) {
		double dt = t_ms[i] - t_ms[i - 1];

		if (dt <= 0.0)
			continue;
		double rate = ((double)occ[i] - (double)occ[i - 1]) / dt;
		ewma = have ? alpha * rate + (1.0 - alpha) * ewma : rate;
		have = true;
	}
	if (!have)
		return -1;

	return ttf_from_rate(capacity, occ[n - 1], ewma, ttf_ms);
}

int
pred_holt(const double *t_ms, const uint32_t *occ, uint32_t n,
	  uint32_t capacity, double *ttf_ms)
{
	const double alpha = 0.5;	/* 水平平滑 */
	const double beta = 0.25;	/* 趋势平滑 */

	if (n < 2)
		return -1;

	double level = (double)occ[0];
	double dt0 = t_ms[1] - t_ms[0];
	double trend = dt0 > 0.0 ?
		       ((double)occ[1] - level) / dt0 : 0.0;

	for (uint32_t i = 1; i < n; i++) {
		double dt = t_ms[i] - t_ms[i - 1];

		if (dt <= 0.0)
			continue;
		double new_level = alpha * (double)occ[i] +
				   (1.0 - alpha) * (level + trend * dt);
		trend = beta * (new_level - level) / dt +
			(1.0 - beta) * trend;
		level = new_level;
	}

	return ttf_from_rate(capacity, occ[n - 1], trend, ttf_ms);
}
