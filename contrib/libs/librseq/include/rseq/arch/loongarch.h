/* SPDX-License-Identifier: MIT */
/* SPDX-FileCopyrightText: 2024 Michael Jeanson <mjeanson@efficios.com> */
/* SPDX-FileCopyrightText: 2023 Huang Pei <huangpei@loongson.cn> */
/* SPDX-FileCopyrightText: 2023 Loongson Technology Corporation Limited */
/* SPDX-FileCopyrightText: 2016-2022 Mathieu Desnoyers <mathieu.desnoyers@efficios.com> */
/* SPDX-FileCopyrightText: 2018 MIPS Tech LLC */
/* SPDX-FileCopyrightText: 2018 Paul Burton <paul.burton@mips.com> */

/*
 * rseq/arch/loongarch.h
 */

#ifndef _RSEQ_RSEQ_H
#error "Never use <rseq/arch/loongarch.h> directly; include <rseq/rseq.h> instead."
#endif

/*
 * RSEQ_ASM_*() macro helpers are internal to the librseq headers. Those
 * are not part of the public API.
 */

#if (RSEQ_BITS_PER_LONG != 64)
# error unsupported RSEQ_BITS_PER_LONG
#endif

/*
 * RSEQ_SIG use "break 0x10" instruction.
 */

#define RSEQ_SIG	0x002a0010

/*
 * Refer to the Linux kernel memory model (LKMM) for documentation of
 * the memory barriers.
 */

/* CPU memory barrier. */
#define rseq_smp_mb()	__asm__ __volatile__ ("dbar 0x10" ::: "memory")
/* CPU read memory barrier */
#define rseq_smp_rmb()	__asm__ __volatile__ ("dbar 0x15" ::: "memory")
/* CPU write memory barrier */
#define rseq_smp_wmb()	__asm__ __volatile__ ("dbar 0x1a" ::: "memory")

/* Acquire: One-way permeable barrier. */
#define rseq_smp_load_acquire(p)					\
__extension__ ({							\
	rseq_unqual_scalar_typeof(*(p)) ____p1 = RSEQ_READ_ONCE(*(p));	\
	__asm__ __volatile__("dbar 0x14" :::  "memory");		\
	____p1;								\
})

/* Acquire barrier after control dependency. */
#define rseq_smp_acquire__after_ctrl_dep()	rseq_smp_rmb()

/* Release: One-way permeable barrier. */
#define rseq_smp_store_release(p, v)					\
do {									\
	__asm__ __volatile__("dbar 0x12" :::  "memory");		\
	RSEQ_WRITE_ONCE(*(p), v);					\
} while (0)

/*
 * Helper macros to define and access a variable of long integer type.
 * Only used internally in rseq headers.
 */
#define RSEQ_ASM_LONG		".dword"
#define RSEQ_ASM_LONG_LA	"la.local"
#define RSEQ_ASM_LONG_L		"ld.d"
#define RSEQ_ASM_LONG_S		"st.d"
#define RSEQ_ASM_LONG_ADDI	"addi.d"

/*
 * Helper macros to define a variable of pointer type stored in a 64-bit
 * integer. Only used internally in rseq headers.
 */
#define RSEQ_ASM_U64_PTR(x)	".dword " x
#define RSEQ_ASM_U32(x)		".word " x

/* Temporary scratch registers. */
#define RSEQ_ASM_TMP_REG	"$r4"

/* Common architecture support macros. */
#include "rseq/arch/generic/common.h"

/* Only used in RSEQ_ASM_DEFINE_ABORT. */
#define __RSEQ_ASM_DEFINE_ABORT(label, teardown, abort_label, \
				table_label, version, flags, \
				start_ip, post_commit_offset, abort_ip) \
		".balign 32\n\t" \
		__rseq_str(table_label) ":\n\t" \
		__RSEQ_ASM_DEFINE_CS_FIELDS(version, flags, \
			start_ip, post_commit_offset, abort_ip) "\n\t" \
		RSEQ_ASM_U32(__rseq_str(RSEQ_SIG)) "\n\t" \
		__rseq_str(label) ":\n\t" \
		teardown \
		"b %l[" __rseq_str(abort_label) "]\n\t"

/*
 * Define a critical section abort handler.
 *
 *  @label:
 *    Local label to the abort handler.
 *  @teardown:
 *    Sequence of instructions to run on abort.
 *  @abort_label:
 *    C label to jump to at the end of the sequence.
 *  @table_label:
 *    Local label to the critical section descriptor copy placed near
 *    the program counter. This is done for performance reasons because
 *    computing this address is faster than accessing the program data.
 *
 * The purpose of @start_ip, @post_commit_ip, and @abort_ip are
 * documented in RSEQ_ASM_DEFINE_TABLE.
 */
#define RSEQ_ASM_DEFINE_ABORT(label, teardown, abort_label, \
			      table_label, start_ip, post_commit_ip, abort_ip) \
	__RSEQ_ASM_DEFINE_ABORT(label, teardown, abort_label, \
				table_label, 0x0, 0x0, start_ip, \
				(post_commit_ip) - (start_ip), abort_ip)

/*
 * Define a critical section teardown handler.
 *
 *  @label:
 *    Local label to the teardown handler.
 *  @teardown:
 *    Sequence of instructions to run on teardown.
 *  @target_label:
 *    C label to jump to at the end of the sequence.
 */
#define RSEQ_ASM_DEFINE_TEARDOWN(label, teardown, target_label) \
		__rseq_str(label) ":\n\t" \
		teardown \
		"b %l[" __rseq_str(target_label) "]\n\t"
/*
 * Store the address of the critical section descriptor structure at
 * @cs_label into the @rseq_cs pointer and emit the label @label, which
 * is the beginning of the sequence of consecutive assembly instructions.
 *
 *  @label:
 *    Local label to the beginning of the sequence of consecutive assembly
 *    instructions.
 *  @cs_label:
 *    Source local label to the critical section descriptor structure.
 *  @rseq_cs:
 *    Destination pointer where to store the address of the critical
 *    section descriptor structure.
 */
#define RSEQ_ASM_STORE_RSEQ_CS(label, cs_label, rseq_cs) \
		RSEQ_INJECT_ASM(1) \
		RSEQ_ASM_LONG_LA " $r4, " __rseq_str(cs_label) "\n\t" \
		RSEQ_ASM_LONG_S  " $r4, %[" __rseq_str(rseq_cs) "]\n\t" \
		__rseq_str(label) ":\n\t"

/* Jump to local label @label when @cpu_id != @current_cpu_id. */
#define RSEQ_ASM_CBNE_CPU_ID(cpu_id, current_cpu_id, label) \
		RSEQ_INJECT_ASM(2) \
		"ld.w  $r4, %[" __rseq_str(current_cpu_id) "]\n\t" \
		"bne $r4, %[" __rseq_str(cpu_id) "], " __rseq_str(label) "\n\t"

/* Per-cpu-id indexing. */

#define RSEQ_TEMPLATE_INDEX_CPU_ID
#define RSEQ_TEMPLATE_MO_RELAXED
#include "rseq/arch/loongarch/bits.h"
#undef RSEQ_TEMPLATE_MO_RELAXED

#define RSEQ_TEMPLATE_MO_RELEASE
#include "rseq/arch/loongarch/bits.h"
#undef RSEQ_TEMPLATE_MO_RELEASE
#undef RSEQ_TEMPLATE_INDEX_CPU_ID

/* Per-mm-cid indexing. */

#define RSEQ_TEMPLATE_INDEX_MM_CID
#define RSEQ_TEMPLATE_MO_RELAXED
#include "rseq/arch/loongarch/bits.h"
#undef RSEQ_TEMPLATE_MO_RELAXED

#define RSEQ_TEMPLATE_MO_RELEASE
#include "rseq/arch/loongarch/bits.h"
#undef RSEQ_TEMPLATE_MO_RELEASE
#undef RSEQ_TEMPLATE_INDEX_MM_CID

/* APIs which are not indexed. */

#define RSEQ_TEMPLATE_INDEX_NONE
#define RSEQ_TEMPLATE_MO_RELAXED
#include "rseq/arch/loongarch/bits.h"
#undef RSEQ_TEMPLATE_MO_RELAXED
#undef RSEQ_TEMPLATE_INDEX_NONE
