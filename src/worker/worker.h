/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * Worker: dispatcher ring 的消费者, 模拟 DPI 场景的真实消费。
 *
 * 每个包执行两部分工作:
 *   1. 对 payload 做一遍模式扫描(memchr, 近似 DPI 特征匹配的访存行为)
 *   2. 可配置的确定性忙等(rte_delay_us), 模拟深度检测的固定开销
 *
 * 单个 worker 的消费能力 ≈ 1 / dpi_us 包每秒
 * (dpi_us=10 时约 100K pps)。N 个 worker 的总消费能力 ≈ N/dpi_us。
 * 通过 --dpi-us 控制消费能力, 与 --gen-pps/--gen-ramp 的生产速率
 * 配合, 构造 ring 被打爆的受控实验。
 */

#ifndef _WORKER_H_
#define _WORKER_H_

#include <stdint.h>

#include <rte_ring.h>
#include <rte_mbuf.h>

#include "elephant_common.h"

struct elephant_worker_stats {
	uint64_t packets;   /* 已消费包数 */
	uint64_t bytes;     /* 已消费字节数 */
} __rte_cache_aligned;

struct elephant_worker {
	uint32_t id;
	uint32_t dpi_us;    /* 每包模拟 DPI 处理时间(微秒) */
	struct rte_ring *ring;
	uint64_t *deq_cntr;  /* 指向 dispatcher 的 per-ring deq_pkts 计数器 */
	struct elephant_worker_stats stats;
};

/** Worker lcore 主循环(经 rte_eal_remote_launch 启动)。 */
int elephant_worker_lcore_main(void *arg);

#endif /* _WORKER_H_ */
