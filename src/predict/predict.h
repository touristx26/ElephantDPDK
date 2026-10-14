/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * Ring 打满时间(TTF, time-to-full)预测算法集。
 *
 * 输入: 一段时间窗口内的 ring 占用率采样序列
 *   (t_ms[i], occ[i]), i = 0..n-1, 按时间升序。
 * 输出: 预测的剩余打满时间(毫秒)。
 *
 * 三种算法:
 *   - linreg: 最小二乘拟合占用率斜率, TTF = 剩余空间 / 斜率
 *   - ewma:   对瞬时填充速率做指数加权平均, TTF = 剩余空间 / 平滑速率
 *   - holt:   Holt 双指数平滑(水平+趋势), TTF = (容量-水平) / 趋势
 *
 * 所有算法只在存在上升趋势时给出有效预测, 否则返回 -1
 * (占用率下降或平稳时不存在"将被打爆"的预测语义)。
 */

#ifndef _PREDICT_H_
#define _PREDICT_H_

#include <stdint.h>

#define PRED_ALGO_COUNT 3

enum pred_algo {
	PRED_LINREG = 0,
	PRED_EWMA,
	PRED_HOLT,
};

extern const char *pred_algo_names[PRED_ALGO_COUNT];

/**
 * 各算法统一签名。
 *
 * @param t_ms    采样时刻(毫秒, 相对窗口首样本), 升序
 * @param occ     各时刻 ring 占用条目数
 * @param n       样本数(>= 2)
 * @param capacity ring 总容量
 * @param ttf_ms  输出: 预测的打满剩余时间(毫秒)
 * @return 0 成功; -1 无有效预测(无上升趋势)
 */
int pred_linreg(const double *t_ms, const uint32_t *occ, uint32_t n,
		uint32_t capacity, double *ttf_ms);
int pred_ewma(const double *t_ms, const uint32_t *occ, uint32_t n,
	      uint32_t capacity, double *ttf_ms);
int pred_holt(const double *t_ms, const uint32_t *occ, uint32_t n,
	      uint32_t capacity, double *ttf_ms);

#endif /* _PREDICT_H_ */
