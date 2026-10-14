/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <inttypes.h>
#include <math.h>
#include <string.h>
#include <netinet/in.h>

#include <rte_byteorder.h>
#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_mbuf.h>
#include <rte_udp.h>

#include "rx.h"
#include "app_ctx.h"

/* ---------- 合成流量(无 NIC 的实验模式) ---------- */

#define SYNTH_HEAVY_FLOWS  4     /* 流 0..3: 1400 字节包   */
#define SYNTH_MED_FLOWS    12    /* 流 4..15: 512 字节包   */
#define SYNTH_TOTAL_FLOWS  64    /* 流 16..63: 64 字节包   */

struct synth_pkt_spec {
	uint16_t len;
	uint16_t flow_id;
};

static inline uint64_t
lcg_next(uint64_t *s)
{
	*s = *s * 6364136223846793005ULL + 1442695040888963407ULL;
	return *s;
}

/* 加权选流: 25% 重流 / 25% 中流 / 50% 鼠流 */
static inline struct synth_pkt_spec
synth_next_flow(struct elephant_rx_ctx *ctx)
{
	struct synth_pkt_spec spec;
	uint64_t r = lcg_next(&ctx->lcg);

	switch (r & 3) {
	case 0:
		spec.flow_id = (uint16_t)(lcg_next(&ctx->lcg) % SYNTH_HEAVY_FLOWS);
		spec.len = 1400;
		break;
	case 1:
		spec.flow_id = SYNTH_HEAVY_FLOWS +
			       (uint16_t)(lcg_next(&ctx->lcg) %
					  (SYNTH_MED_FLOWS - SYNTH_HEAVY_FLOWS));
		spec.len = 512;
		break;
	default:
		spec.flow_id = SYNTH_MED_FLOWS +
			       (uint16_t)(lcg_next(&ctx->lcg) %
					  (SYNTH_TOTAL_FLOWS - SYNTH_MED_FLOWS));
		spec.len = 64;
		break;
	}
	return spec;
}

/* 在 mbuf 数据区构造一个合成包(只填流提取所需的头部) */
static uint16_t
synth_generate_burst(struct elephant_rx_ctx *ctx, struct rte_mbuf **pkts,
		     uint16_t max)
{
	uint16_t n = 0;

	for (uint16_t i = 0; i < max; i++) {
		struct synth_pkt_spec spec = synth_next_flow(ctx);
		struct rte_mbuf *m = rte_pktmbuf_alloc(ctx->pool);

		if (m == NULL)
			break;

		uint8_t *data = rte_pktmbuf_mtod(m, uint8_t *);
		struct rte_ether_hdr *eth = (void *)data;
		struct rte_ipv4_hdr *ip = (void *)(data + sizeof(*eth));
		struct rte_udp_hdr *udp =
			(void *)(data + sizeof(*eth) + sizeof(*ip));

		memset(eth, 0, sizeof(*eth));
		eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

		memset(ip, 0, sizeof(*ip));
		ip->version_ihl = 0x45; /* IPv4, IHL = 5 (20 字节) */
		ip->total_length = rte_cpu_to_be_16(spec.len -
						    sizeof(*eth));
		ip->next_proto_id = IPPROTO_UDP;
		ip->src_addr = rte_cpu_to_be_32(RTE_IPV4(10, 0,
				(uint8_t)(spec.flow_id >> 8),
				(uint8_t)spec.flow_id));
		ip->dst_addr = rte_cpu_to_be_32(RTE_IPV4(10, 1,
				(uint8_t)(spec.flow_id >> 8),
				(uint8_t)spec.flow_id));

		memset(udp, 0, sizeof(*udp));
		udp->src_port = rte_cpu_to_be_16(1024 + spec.flow_id);
		udp->dst_port = rte_cpu_to_be_16(80);
		udp->dgram_len = rte_cpu_to_be_16((uint16_t)(spec.len -
				sizeof(*eth) - sizeof(*ip)));
		udp->dgram_cksum = 0; /* 管线不校验 */

		m->port = ctx->port_id;
		m->pkt_len = spec.len;
		m->data_len = spec.len;
		pkts[n++] = m;
	}
	return n;
}

/*
 * 令牌桶: 按当前瞬时速率累积令牌, 返回本次允许生成的包数。
 * 全速模式(rate_control == false)直接放行整个 burst。
 */
static uint32_t
synth_tokens_available(struct elephant_rx_ctx *ctx)
{
	if (!ctx->rate_control)
		return ELEPHANT_BURST_MAX;

	uint64_t tsc_hz = rte_get_tsc_hz();
	uint64_t now = rte_rdtsc();
	double dt_s = (double)(now - ctx->gen_last_tsc) / (double)tsc_hz;

	ctx->gen_last_tsc = now;

	double rate = (double)ctx->gen_pps;
	if (ctx->gen_ramp > 0 || ctx->gen_sawtooth_s > 0) {
		double elapsed = (double)(now - ctx->start_tsc) /
				 (double)tsc_hz;

		if (ctx->gen_sawtooth_s > 0)
			elapsed = fmod(elapsed, (double)ctx->gen_sawtooth_s);
		rate += (double)ctx->gen_ramp * elapsed;
	}

	ctx->gen_tokens += rate * dt_s;
	/* 突发上限: 空闲后不会一次性补发过多 */
	if (ctx->gen_tokens > (double)ELEPHANT_BURST_MAX * 2.0)
		ctx->gen_tokens = (double)ELEPHANT_BURST_MAX * 2.0;

	return (uint32_t)ctx->gen_tokens;
}

/* ---------- Flow Key 提取(fast path, 仅供分发亲和) ---------- */

/*
 * 从包头提取归一化的流键。返回 false 表示非 IP 流量
 * (仍会往下分发, 只是哈希退化为 mbuf 指针哈希)。
 */
static bool
rx_extract_flow(const struct rte_mbuf *m, struct elephant_flow_key *key)
{
	uint32_t offset = sizeof(struct rte_ether_hdr);
	const struct rte_ether_hdr *eth;

	if (m->data_len < offset)
		return false;

	eth = rte_pktmbuf_mtod(m, const struct rte_ether_hdr *);
	uint16_t et = rte_be_to_cpu_16(eth->ether_type);

	if (et == RTE_ETHER_TYPE_VLAN) {
		const struct rte_vlan_hdr *vlan;

		offset += sizeof(*vlan);
		if (m->data_len < offset)
			return false;
		vlan = (const struct rte_vlan_hdr *)(eth + 1);
		et = rte_be_to_cpu_16(vlan->eth_proto);
	}

	memset(key, 0, sizeof(*key));
	key->ethertype = et;

	uint16_t proto;
	const uint8_t *l4 = NULL;

	if (et == RTE_ETHER_TYPE_IPV4) {
		const struct rte_ipv4_hdr *ip;

		if (m->data_len < offset + sizeof(*ip))
			return false;
		ip = (const struct rte_ipv4_hdr *)
			((const uint8_t *)eth + offset);
		uint32_t ihl = (ip->version_ihl & RTE_IPV4_HDR_IHL_MASK) * 4;

		if (ihl < sizeof(*ip) || m->data_len < offset + ihl)
			return false;

		/* memcpy: L3 头偏移 14 字节, 未必 4 字节对齐 */
		memcpy(&key->src_ip[0], &ip->src_addr, sizeof(uint32_t));
		memcpy(&key->dst_ip[0], &ip->dst_addr, sizeof(uint32_t));
		proto = ip->next_proto_id;
		l4 = (const uint8_t *)ip + ihl;
	} else if (et == RTE_ETHER_TYPE_IPV6) {
		const struct rte_ipv6_hdr *ip6;

		if (m->data_len < offset + sizeof(*ip6))
			return false;
		ip6 = (const struct rte_ipv6_hdr *)
			((const uint8_t *)eth + offset);

		memcpy(&key->src_ip, ip6->src_addr, sizeof(key->src_ip));
		memcpy(&key->dst_ip, ip6->dst_addr, sizeof(key->dst_ip));
		key->is_ipv6 = 1;
		proto = ip6->proto;
		l4 = (const uint8_t *)ip6 + sizeof(*ip6);
	} else {
		return false;
	}

	key->proto = (uint8_t)proto;
	if ((proto == IPPROTO_TCP || proto == IPPROTO_UDP) &&
	    m->data_len >= (uint32_t)(l4 - (const uint8_t *)eth) + 4) {
		key->src_port = rte_be_to_cpu_16(*(const rte_be16_t *)l4);
		key->dst_port =
			rte_be_to_cpu_16(*(const rte_be16_t *)(l4 + 2));
	}
	return true;
}

/*
 * 每个 burst 的核心处理:
 *   flow 提取/哈希 -> mbuf 打标(仅哈希) -> dispatcher 分发
 */
static inline void
rx_process_burst(struct elephant_rx_ctx *ctx,
		 struct rte_mbuf **pkts, uint16_t n)
{
	uint64_t bytes = 0;

	for (uint16_t i = 0; i < n; i++) {
		struct rte_mbuf *m = pkts[i];
		struct elephant_flow_key key;
		uint64_t h;

		if (rx_extract_flow(m, &key))
			h = elephant_flow_hash(&key);
		else
			h = elephant_fmix64((uint64_t)(uintptr_t)m);

		elephant_mbuf_mark(m, h, false);
		bytes += m->pkt_len;
	}

	elephant_dispatch_burst(ctx->disp, pkts, n);

	ctx->stats.packets += n;
	ctx->stats.bytes += bytes;
}

int
elephant_rx_lcore_main(void *arg)
{
	struct elephant_rx_ctx *ctx = arg;
	struct rte_mbuf *pkts[ELEPHANT_BURST_MAX];
	unsigned int lcore_id = rte_lcore_id();

	RTE_LOG(INFO, EDPDK,
		"rx q%u on lcore %u: %s mode, dispatcher with %u ring(s)\n",
		ctx->queue_id, lcore_id,
		ctx->synthetic ? "synthetic" : "NIC",
		ctx->disp->n_rings);

	while (likely(!app_force_quit())) {
		uint16_t n = 0;

		if (ctx->synthetic) {
			uint32_t allowed = synth_tokens_available(ctx);

			if (allowed > 0) {
				n = synth_generate_burst(ctx, pkts,
							 (uint16_t)allowed);
				ctx->gen_tokens -= n;
			}
		} else {
			n = rte_eth_rx_burst(ctx->port_id, ctx->queue_id,
					     pkts, ELEPHANT_BURST_MAX);
		}
		if (unlikely(n == 0))
			continue;
		rx_process_burst(ctx, pkts, n);
	}

	RTE_LOG(INFO, EDPDK, "rx q%u on lcore %u: exiting\n",
		ctx->queue_id, lcore_id);
	return 0;
}

int
elephant_rx_ctx_init(struct elephant_rx_ctx *ctx,
		     const struct app_config *cfg,
		     uint16_t port_id, uint16_t queue_id,
		     struct rte_mempool *pool,
		     struct elephant_dispatcher *disp,
		     uint64_t seed)
{
	memset(ctx, 0, sizeof(*ctx));
	ctx->port_id = port_id;
	ctx->queue_id = queue_id;
	ctx->synthetic = cfg->synthetic;
	ctx->pool = pool;
	ctx->disp = disp;
	ctx->lcg = seed ^ 0xd1b54a32d192ed03ULL;

	ctx->gen_pps = cfg->gen_pps;
	ctx->gen_ramp = cfg->gen_ramp;
	ctx->gen_sawtooth_s = cfg->gen_sawtooth_s;
	ctx->rate_control = cfg->gen_pps > 0 || cfg->gen_ramp > 0 ||
			    cfg->gen_sawtooth_s > 0;
	ctx->start_tsc = rte_rdtsc();
	ctx->gen_last_tsc = ctx->start_tsc;
	ctx->gen_tokens = ELEPHANT_BURST_MAX; /* 开局快速填充管道 */
	return 0;
}
