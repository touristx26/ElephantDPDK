/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * Ring 溢出预测监视器 —— 运行在独立 monitor lcore 上, 不触碰 fast path。
 *
 * 采样(默认 100us 周期, TSC 忙等保证精度):
 *   depth   = rte_ring_count(ring)
 *   enq/deq = dispatcher per-ring 单调计数器快照
 *
 * 相邻采样周期差值得到三个核心指标:
 *   arrival_rate = Δenq / Δt   (入队速率, pkts/s)
 *   service_rate = Δdeq / Δt   (出队速率, pkts/s)
 *   growth_rate  = arrival - service (净增长率)
 *
 * 预测算法(全部记入预测日志, 溢出时结算误差):
 *   rate   : TTF = (capacity - depth) / growth_rate
 *            —— 基于本周期速率差的直接外推(本阶段核心)
 *   linreg : 对 depth 序列做最小二乘拟合斜率外推(对照)
 *   ewma   : depth 增速指数平滑外推(对照)
 *   holt   : depth 序列 Holt 双指数平滑(对照)
 *
 * 满载风险判断(depth / arrival / service 三指标):
 *   risk = depth_pct >= RISK_DEPTH_PCT
 *          && growth_rate > 0
 *          && ttf_rate <= RISK_TTF_MS
 *   命中时打印限频 ALERT。
 *
 * Ground truth: dispatcher per-ring drop 计数器上升沿 = 真实溢出时刻。
 */

#ifndef _RINGMON_H_
#define _RINGMON_H_

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include <rte_ring.h>

#include "dispatcher/dispatcher.h"
#include "predict/predict.h"

#define RINGMON_SAMPLE_CAP   16384u /* depth 历史(100us 采样约 1.6s) */
#define RINGMON_PRED_CAP     16384u /* 预测日志(100us 采样约 1.6s)    */
#define RINGMON_NEAR_MS      2000.0 /* "近端"误差统计窗口(ms)          */

/* 满载风险判断阈值 */
#define RINGMON_RISK_DEPTH_PCT   80.0  /* depth 水位百分比下限          */
#define RINGMON_RISK_TTF_MS      500.0 /* rate 预测 TTF 上限(ms)        */
#define RINGMON_ALERT_INT_MS     200.0 /* ALERT 打印限频(ms)           */

/* 预测算法总数 = predict.h 三种 + rate(基于计数器速率差) */
#define RINGMON_ALGO_COUNT   (PRED_ALGO_COUNT + 1)
#define RINGMON_ALGO_RATE    PRED_ALGO_COUNT

static inline const char *
ringmon_algo_name(uint32_t a)
{
	return a == RINGMON_ALGO_RATE ? "rate" : pred_algo_names[a];
}

struct ringmon_sample {
	uint64_t tsc;
	uint32_t depth;
};

struct ringmon_pred {
	uint64_t tsc;
	double ttf_ms[RINGMON_ALGO_COUNT];
	uint8_t valid_mask;   /* bit i: 算法 i 在此时刻给出了有效预测 */
};

/* 最新一次采样的核心指标(状态打印用) */
struct ringmon_metrics {
	uint32_t depth;
	uint32_t depth_pct;      /* 0-100 */
	double arrival_pps;      /* 入队速率 pkts/s */
	double service_pps;     /* 出队速率 pkts/s */
	double growth_pps;      /* arrival - service */
	bool at_risk;           /* 三指标风险判断结果 */
};

struct ringmon {
	/* 静态配置 */
	struct rte_ring *ring;
	struct elephant_dispatcher *disp;
	struct ring_cntrs *cntrs;     /* 指向 disp->cntrs[ring_idx] */
	uint32_t ring_idx;
	uint32_t ring_capacity;
	uint32_t pred_window;          /* 拟合窗口(样本数) */

	/* 采样状态 */
	uint64_t last_sample_tsc;
	uint64_t prev_enq;             /* 上次采样的 enq 计数器快照 */
	uint64_t prev_deq;
	uint64_t last_alert_tsc;

	/* depth 历史(循环缓冲, legacy 算法拟合输入) */
	struct ringmon_sample *samples;
	uint32_t sample_head;
	uint32_t sample_len;

	/* 预测日志(循环缓冲, trial 结算输入) */
	struct ringmon_pred *preds;
	uint32_t pred_head;
	uint32_t pred_len;

	/* 拟合窗口临时数组 */
	double *win_t_ms;
	uint32_t *win_depth;

	/* 溢出事件(上升沿)检测 */
	uint64_t last_drops;
	bool in_overflow;
	uint64_t overflow_events;
	uint64_t trial_start_tsc;

	/* 最新指标快照(main lcore 状态打印只读) */
	struct ringmon_metrics metrics;

	/* 跨 trial 累计指标 */
	double sum_mae_ms[RINGMON_ALGO_COUNT];
	double sum_bias_ms[RINGMON_ALGO_COUNT];
	double sum_mae_near_ms[RINGMON_ALGO_COUNT];
	double sum_lead_ms[RINGMON_ALGO_COUNT];
	uint32_t n_pred[RINGMON_ALGO_COUNT];
	uint32_t n_pred_near[RINGMON_ALGO_COUNT];
	uint32_t n_valid_trial[RINGMON_ALGO_COUNT];
	uint32_t trials;
};

/* monitor lcore 的工作集(独立线程参数) */
struct ringmon_set {
	struct ringmon *mons;
	uint32_t n_mons;
	uint32_t sample_us;    /* 采样周期(微秒) */
	FILE *csv;             /* 可选 CSV 输出, NULL = 不写 */
};

int ringmon_init(struct ringmon *mon, struct rte_ring *ring,
		 struct elephant_dispatcher *disp, uint32_t ring_idx,
		 uint32_t pred_window);
void ringmon_free(struct ringmon *mon);

/** 采一次: depth + enq/deq 快照 -> 速率指标 -> 全部预测算法 -> 风险判断。 */
void ringmon_sample(struct ringmon *mon, uint64_t now_tsc);

/** drop 计数器上升沿检测; true = 出现新的溢出事件。 */
bool ringmon_check_overflow(struct ringmon *mon);

/** 结算当前 trial 的预测误差并开始下一个 trial。 */
void ringmon_finish_trial(struct ringmon *mon, uint64_t now_tsc);

/** monitor lcore 主循环(经 rte_eal_remote_launch 启动): 周期采样 + 溢出结算。 */
int ringmon_lcore_main(void *arg);

#endif /* _RINGMON_H_ */
