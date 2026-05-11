// SPDX-FileCopyrightText: 2023 Mathieu Desnoyers <mathieu.desnoyers@efficios.com>
//
// SPDX-License-Identifier: MIT

#include <stdint.h>
#include <rseq/rseq.h>
#include <rseq/compiler.h>

#include "tap.h"

struct {
	uint8_t v_u8;
	int8_t v_s8;
	uint16_t v_u16;
	int16_t v_s16;
	uint32_t v_u32;
	int32_t v_s32;
#if RSEQ_BITS_PER_LONG == 64
	uint64_t v_u64;
	int64_t v_s64;
#endif
	void *p;
	volatile int vol_int;
	const int const_int;
} load_s = {
	.v_u8 = 0x11,
	.v_s8 = -(0x11),
	.v_u16 = 0x1122,
	.v_s16 = -(0x1122),
	.v_u32 = (0x11223344),
	.v_s32 = -(0x11223344),
#if RSEQ_BITS_PER_LONG == 64
	.v_u64 = 0x1122334455667788ULL,
	.v_s64 = -(0x1122334455667788LL),
	.p = (void *)0x1122334455667788ULL,
#else
	.p = (void *)0x11223344,
#endif
	.vol_int = -(0x11223344),
	.const_int = -(0x11223344),
};

static
void test_load_acquire(void)
{
	ok(rseq_smp_load_acquire(&load_s.v_u8) == 0x11, "load-acquire u8");
	ok(rseq_smp_load_acquire(&load_s.v_s8) == -(0x11), "load-acquire s8");
	ok(rseq_smp_load_acquire(&load_s.v_u16) == 0x1122, "load-acquire u16");
	ok(rseq_smp_load_acquire(&load_s.v_s16) == -(0x1122), "load-acquire s16");
	ok(rseq_smp_load_acquire(&load_s.v_u32) == 0x11223344, "load-acquire u32");
	ok(rseq_smp_load_acquire(&load_s.v_s32) == -(0x11223344), "load-acquire s32");
#if RSEQ_BITS_PER_LONG == 64
	ok(rseq_smp_load_acquire(&load_s.v_u64) == 0x1122334455667788ULL, "load-acquire u64");
	ok(rseq_smp_load_acquire(&load_s.v_s64) == -(0x1122334455667788LL), "load-acquire s64");
	ok(rseq_smp_load_acquire(&load_s.p) == (void *)0x1122334455667788ULL, "load-acquire pointer");
#else
	ok(rseq_smp_load_acquire(&load_s.p) == (void *)0x11223344, "load-acquire pointer");
#endif
	ok(rseq_smp_load_acquire(&load_s.vol_int) == -(0x11223344), "load-acquire volatile int");
	ok(rseq_smp_load_acquire(&load_s.const_int) == -(0x11223344), "load-acquire const int");
}

struct {
	uint8_t v_u8;
	int8_t v_s8;
	uint16_t v_u16;
	int16_t v_s16;
	uint32_t v_u32;
	int32_t v_s32;
#if RSEQ_BITS_PER_LONG == 64
	uint64_t v_u64;
	int64_t v_s64;
#endif
	void *p;
	volatile int vol_int;
} store_s;

static
void test_store_release(void)
{
	rseq_smp_store_release(&store_s.v_u8, 0x11);
	ok(store_s.v_u8 == 0x11, "store-release u8");
	rseq_smp_store_release(&store_s.v_s8, -(0x11));
	ok(store_s.v_s8 == -(0x11), "store-release s8");
	rseq_smp_store_release(&store_s.v_u16, 0x1122);
	ok(store_s.v_u16 == 0x1122, "store-release u16");
	rseq_smp_store_release(&store_s.v_s16, -(0x1122));
	ok(store_s.v_s16 == -(0x1122), "store-release s16");
	rseq_smp_store_release(&store_s.v_u32, 0x11223344);
	ok(store_s.v_u32 == 0x11223344, "store-release u32");
	rseq_smp_store_release(&store_s.v_s32, -(0x11223344));
	ok(store_s.v_s32 == -(0x11223344), "store-release s32");
#if RSEQ_BITS_PER_LONG == 64
	rseq_smp_store_release(&store_s.v_u64, 0x1122334455667788ULL);
	ok(store_s.v_u64 == 0x1122334455667788ULL, "store-release u64");
	rseq_smp_store_release(&store_s.v_s64, -(0x1122334455667788LL));
	ok(store_s.v_s64 == -(0x1122334455667788LL), "store-release s64");
	rseq_smp_store_release(&store_s.p, (void *)0x1122334455667788ULL);
	ok(store_s.p == (void *)0x1122334455667788ULL, "store-release pointer");
#else
	rseq_smp_store_release(&store_s.p, (void *)0x11223344);
	ok(store_s.p == (void *)0x11223344, "store-release pointer");
#endif
	rseq_smp_store_release(&store_s.vol_int, -(0x11223344));
	ok(store_s.vol_int == -(0x11223344), "store-release volatile int");
}

int main(void)
{
	plan_no_plan();
	test_load_acquire();
	test_store_release();
	exit(exit_status());
}
