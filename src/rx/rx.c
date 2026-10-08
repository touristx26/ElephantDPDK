/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors */

#include <inttypes.h>
#include <string.h>
#include <netinet/in.h>

#include <rte_byteorder.h>
#include <rte_common.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_mbuf.h>
#include <rte_udp.h>

#include "rx.h"
#include "app_ctx.h"

/* ---------- Synthetic traffic (demo mode without a NIC) ---------- */

#define SYNTH_HEAVY_FLOWS  4     /* flows 0..3: 1400-byte packets   */
#define SYNTH_MED_FLOWS    12    /* flows 4..15: 512-byte packets   */
#define SYNTH_TOTAL_FLOWS  64    /* flows 16..63: 64-byte packets   */

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

/* Weighted flow selection: 25% heavy, 25% medium, 50% mice. */
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

/*
 * Build one synthetic packet in the mbuf's data area. Only the headers
 * needed by flow extraction are filled in.
 */
static uint16_t
synth_generate_burst(struct elephant_rx_ctx *ctx, struct rte_mbuf **pkts)
{
	uint16_t n = 0;

	for (uint16_t i = 0; i < ELEPHANT_BURST_MAX; i++) {
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
		ip->version_ihl = 0x45; /* IPv4, IHL = 5 (20 bytes) */
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
		udp->dgram_cksum = 0; /* not validated by the pipeline */

		m->port = ctx->port_id;
		m->pkt_len = spec.len;
		m->data_len = spec.len;
		pkts[n++] = m;
	}
	return n;
}

/* ---------- Flow key extraction (fast path) ---------- */

/*
 * Extract a normalized flow key from the packet headers.
 * Returns false for non-IP traffic (still dispatched, but untracked).
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

		/* memcpy: L3 sits at a 14-byte offset, not necessarily
		 * 4-byte aligned. */
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
		return false; /* non-IP: forwarded, untracked */
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

/* Announce a flow the first time it crosses the elephant threshold. */
static bool
rx_announce_elephant(struct elephant_rx_ctx *ctx, uint64_t flow_hash,
		     uint64_t est)
{
	uint32_t i = flow_hash & (RX_ANNOUNCE_TABLE - 1);

	if (ctx->announced[i] == flow_hash)
		return false;

	ctx->announced[i] = flow_hash;
	ctx->stats.elephant_flows++;
	RTE_LOG(INFO, EDPDK,
		"rx q%u: ELEPHANT flow hash=0x%012" PRIx64
		" est=%" PRIu64 " bytes (threshold=%" PRIu64 ")\n",
		ctx->queue_id, flow_hash, est, ctx->elephant_threshold);
	return true;
}

/*
 * Core per-burst pipeline:
 *   key/hash -> NitroSketch -> elephant flag (dynfield) -> dispatcher
 */
static inline void
rx_process_burst(struct elephant_rx_ctx *ctx,
		 struct rte_mbuf **pkts, uint16_t n)
{
	uint64_t bytes = 0;
	uint64_t elephant_pkts = 0;

	for (uint16_t i = 0; i < n; i++) {
		struct rte_mbuf *m = pkts[i];
		struct elephant_flow_key key;
		uint64_t h;
		bool is_elephant = false;

		if (rx_extract_flow(m, &key)) {
			h = elephant_flow_hash(&key);
			ns_update(&ctx->sketch, h, m->pkt_len);
			uint64_t est = ns_estimate(&ctx->sketch, h);

			is_elephant = est >= ctx->elephant_threshold;
			if (is_elephant) {
				elephant_pkts++;
				rx_announce_elephant(ctx, h, est);
			}
		} else {
			/* Non-IP: spread by a per-packet hash, no flag. */
			h = elephant_fmix64((uint64_t)(uintptr_t)m);
		}

		elephant_mbuf_mark(m, h, is_elephant);
		bytes += m->pkt_len;
	}

	elephant_dispatch_burst(ctx->disp, pkts, n);

	ctx->stats.packets += n;
	ctx->stats.bytes += bytes;
	ctx->stats.elephant_packets += elephant_pkts;
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
		uint16_t n;

		if (ctx->synthetic)
			n = synth_generate_burst(ctx, pkts);
		else
			n = rte_eth_rx_burst(ctx->port_id, ctx->queue_id,
					     pkts, ELEPHANT_BURST_MAX);
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
	ctx->elephant_threshold = cfg->elephant_threshold;
	ctx->synthetic = cfg->synthetic;
	ctx->pool = pool;
	ctx->disp = disp;
	ctx->lcg = seed ^ 0xd1b54a32d192ed03ULL;

	if (ns_init(&ctx->sketch, NS_DEFAULT_ROWS, NS_DEFAULT_COLS,
		    NS_DEFAULT_RATE, seed) < 0)
		return -1;
	return 0;
}

void
elephant_rx_ctx_free(struct elephant_rx_ctx *ctx)
{
	if (ctx == NULL)
		return;
	ns_free(&ctx->sketch);
}
