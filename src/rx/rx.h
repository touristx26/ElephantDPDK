/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * RX lcore(feature/qlf 分支): 受控流量生产者。
 *
 *   rte_eth_rx_burst 或 合成流量发生器(令牌桶限速)
 *     -> Flow Key 提取 + 哈希(保留流亲和分发)
 *     -> mbuf 打标(仅存 flow hash, 供 dispatcher 路由)
 *     -> hash dispatcher -> worker ring
 *
 * 合成流量的速率模型(令牌桶):
 *   rate(t) = gen_pps + gen_ramp * elapsed
 *   锯齿模式(gen_sawtooth_s > 0)下 elapsed 取模周期,
 *   形成周期性的"填充-溢出-排空"循环, 单次运行产生多个 trial。
 *   gen_pps=0 且无 ramp/sawtooth 时为全速压力模式(行为不受限)。
 */

#ifndef _RX_H_
#define _RX_H_

#include <stdint.h>
#include <stdbool.h>

#include <rte_mbuf.h>
#include <rte_mempool.h>

#include "app_ctx.h"
#include "elephant_common.h"
#include "dispatcher/dispatcher.h"

struct elephant_rx_stats {
	uint64_t packets;
	uint64_t bytes;
} __rte_cache_aligned;

struct elephant_rx_ctx {
	/* 配置(初始化后只读) */
	uint16_t port_id;
	uint16_t queue_id;
	bool synthetic;
	struct rte_mempool *pool;
	struct elephant_dispatcher *disp;

	/* 合成流量负载控制(令牌桶, 仅 synthetic 模式生效) */
	bool rate_control;       /* false = 全速, 忽略 pps 配置 */
	uint32_t gen_pps;        /* 初始速率(包/秒, 每 RX lcore) */
	uint32_t gen_ramp;       /* 速率爬升(包/秒²) */
	uint32_t gen_sawtooth_s; /* 锯齿周期(秒), 0 = 单调爬升 */
	double gen_tokens;
	uint64_t gen_last_tsc;
	uint64_t start_tsc;

	/* per-lcore 私有状态 */
	uint64_t lcg;            /* 合成模式 RNG 状态 */

	struct elephant_rx_stats stats;
};

int elephant_rx_ctx_init(struct elephant_rx_ctx *ctx,
			 const struct app_config *cfg,
			 uint16_t port_id, uint16_t queue_id,
			 struct rte_mempool *pool,
			 struct elephant_dispatcher *disp,
			 uint64_t seed);

/** RX lcore 主循环(经 rte_eal_remote_launch 启动)。 */
int elephant_rx_lcore_main(void *arg);

#endif /* _RX_H_ */
