/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * 共享应用上下文: 运行时配置与 force_quit 标志。
 * feature/qlf 分支: ring 打满时间预测实验的参数。
 */

#ifndef _APP_CTX_H_
#define _APP_CTX_H_

#include <stdint.h>
#include <stdbool.h>

struct app_config {
	uint16_t port_id;             /* NIC 端口(NIC 模式)                */
	uint16_t n_rx_queues;         /* RX 队列数(= RX lcore 数)          */
	uint16_t n_workers;           /* worker 数(= ring 数)              */
	bool synthetic;               /* 合成流量模式(无需网卡)            */

	/* 消费侧(worker) */
	uint32_t dpi_us;              /* 每包模拟 DPI 处理时间(微秒)       */

	/* 生产侧(合成流量令牌桶) */
	uint32_t gen_pps;             /* 初始速率(包/秒, 每 RX lcore)      */
	uint32_t gen_ramp;            /* 速率爬升(包/秒^2)                 */
	uint32_t gen_sawtooth_s;      /* 锯齿周期(秒), 0 = 单调爬升        */

	/* 监视/预测(独立 monitor lcore) */
	uint32_t sample_us;           /* ring 采样周期(微秒)             */
	uint32_t pred_window;         /* 预测拟合窗口(采样数)              */

	/* 输出 */
	char csv_path[256];           /* CSV 输出路径, 空 = 不写文件       */
	unsigned stats_period_s;      /* 运行状态打印间隔(秒)              */
};

/* 信号处理器 / main lcore 写入, 各 lcore 主循环轮询。 */
extern volatile int g_force_quit;

static inline bool
app_force_quit(void)
{
	return __atomic_load_n(&g_force_quit, __ATOMIC_RELAXED);
}

void app_force_quit_set(void);

/* 填充默认配置。 */
void app_config_init_defaults(struct app_config *cfg);

#endif /* _APP_CTX_H_ */
