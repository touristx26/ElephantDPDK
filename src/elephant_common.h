/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2026 ElephantDPDK contributors
 *
 * Common definitions for the elephant-flow detection pipeline:
 *
 *   RXQ -> Flow Key/Hash Extraction -> NitroSketch -> Elephant Flag
 *       -> Hash Dispatcher -> Ring -> Worker
 */

#ifndef _ELEPHANT_COMMON_H_
#define _ELEPHANT_COMMON_H_

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include <rte_common.h>
#include <rte_log.h>
#include <rte_mbuf.h>
#include <rte_mbuf_dyn.h>

/* Custom logtype, registered in main.c. */
extern int edpdk_logtype;
#define EDPDK edpdk_logtype

#define ELEPHANT_BURST_MAX        32    /* max RX / ring burst size          */
#define ELEPHANT_RING_SIZE        4096  /* per-worker ring depth (power of 2) */
#define ELEPHANT_MAX_WORKERS      16
#define ELEPHANT_MAX_RX_QUEUES    8
#define ELEPHANT_MEMPOOL_CACHE    256
#define ELEPHANT_NB_MBUF          8192
#define ELEPHANT_RX_DESC_DEFAULT  1024

/* Default per-flow byte threshold before a flow is declared elephant. */
#define ELEPHANT_DEFAULT_THRESHOLD (1ULL << 20)  /* 1 MiB */

/*
 * Per-packet flow metadata.
 *
 * NOTE: the historic rte_mbuf->udata64 field was REMOVED in DPDK 20.11
 * (replaced by dynamic fields). We register a 64-bit dynfield with the
 * same semantics; main() calls elephant_dynfield_init() after EAL init.
 *
 * Layout:
 *   bit 63    : elephant flow indicator (set by the RX fast path)
 *   bits 0-62 : 63-bit flow hash (usable by workers for flow affinity)
 */
#define ELEPHANT_FLAG_MASK        (1ULL << 63)
#define ELEPHANT_HASH_MASK        (ELEPHANT_FLAG_MASK - 1)

/** Offset of the registered dynfield, initialized by main(). */
extern int elephant_dynfield_offset;

/** Register the per-packet metadata dynfield (after EAL init). */
int elephant_dynfield_init(void);

/** Accessor for the per-packet metadata word. */
#define ELEPHANT_META(m) \
	RTE_MBUF_DYNFIELD((m), elephant_dynfield_offset, uint64_t *)

static inline void
elephant_mbuf_mark(struct rte_mbuf *m, uint64_t flow_hash, bool is_elephant)
{
	*ELEPHANT_META(m) = (flow_hash & ELEPHANT_HASH_MASK) |
			    (is_elephant ? ELEPHANT_FLAG_MASK : 0);
}

static inline bool
elephant_mbuf_is_elephant(struct rte_mbuf *m)
{
	return (*ELEPHANT_META(m) & ELEPHANT_FLAG_MASK) != 0;
}

static inline uint64_t
elephant_mbuf_flow_hash(struct rte_mbuf *m)
{
	return *ELEPHANT_META(m) & ELEPHANT_HASH_MASK;
}

/*
 * Flow key: compact 5-tuple. IPv4 uses ip[0] words only; IPv6 uses all
 * four words per address. The struct is zero-initialized before use so
 * hashing always sees deterministic bytes.
 */
struct elephant_flow_key {
	uint32_t src_ip[4];
	uint32_t dst_ip[4];
	uint16_t src_port;
	uint16_t dst_port;
	uint16_t ethertype;
	uint8_t  proto;
	uint8_t  is_ipv6;
};

/* MurmurHash3 64-bit finalizer. */
static inline uint64_t
elephant_fmix64(uint64_t k)
{
	k ^= k >> 33;
	k *= 0xff51afd7ed558ccdULL;
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53ULL;
	k ^= k >> 33;
	return k;
}

/* Hash the (zero-initialized) flow key down to 63 usable bits. */
static inline uint64_t
elephant_flow_hash(const struct elephant_flow_key *key)
{
	uint64_t h1 = (uint64_t)key->src_ip[0] << 32 | key->src_ip[1];
	uint64_t h2 = (uint64_t)key->src_ip[2] << 32 | key->src_ip[3];
	uint64_t h3 = (uint64_t)key->dst_ip[0] << 32 | key->dst_ip[1];
	uint64_t h4 = (uint64_t)key->dst_ip[2] << 32 | key->dst_ip[3];
	uint64_t h5 = (uint64_t)key->src_port << 48 | (uint64_t)key->dst_port << 32 |
		      (uint64_t)key->ethertype << 16 | (uint64_t)key->proto << 8 |
		      key->is_ipv6;

	uint64_t h = elephant_fmix64(h1);
	h ^= elephant_fmix64(h2) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
	h ^= elephant_fmix64(h3) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
	h ^= elephant_fmix64(h4) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
	h ^= elephant_fmix64(h5) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);

	/* Clear the top bit: it is reserved for the elephant flag. */
	return elephant_fmix64(h) & ELEPHANT_HASH_MASK;
}

#endif /* _ELEPHANT_COMMON_H_ */
