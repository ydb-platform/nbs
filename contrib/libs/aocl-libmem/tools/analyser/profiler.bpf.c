/*
 * Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * Self-contained BPF profiler for AOCL-LibMem functions.
 *
 * Traces 17 memory/string functions via uprobe hooks, counting calls, recording
 * size distributions (log2 histogram), and optionally checking pointer
 * alignment against a runtime-configurable boundary.
 *
 * No external headers needed - compatible with clang 7+ and kernel 4.19+.
 * Maps use the pre-BTF "section" style so older kernels work without BTF.
 */

/* ========================================================================= */
/*  BPF primitives (stable Linux UAPI, no headers required)                  */
/* ========================================================================= */

#define BPF_MAP_TYPE_ARRAY 2
#define BPF_MAP_TYPE_PERCPU_ARRAY 6

struct bpf_map_def {
    unsigned int type;
    unsigned int key_size;
    unsigned int value_size;
    unsigned int max_entries;
    unsigned int map_flags;
};

static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *) 1;

/* x86_64 pt_regs for uprobe context */
struct pt_regs {
    unsigned long r15, r14, r13, r12, rbp, rbx;
    unsigned long r11, r10, r9, r8;
    unsigned long rax, rcx, rdx, rsi, rdi;
    unsigned long orig_rax, rip, cs, rflags, rsp, ss;
};

/* x86_64 System V ABI argument registers */
#define PT_REGS_PARM1(ctx) ((ctx)->rdi)
#define PT_REGS_PARM2(ctx) ((ctx)->rsi)
#define PT_REGS_PARM3(ctx) ((ctx)->rdx)

/* ========================================================================= */
/*  Inline helpers                                                           */
/* ========================================================================= */

/* Log2 bucket (0-63) using clang builtin; works in clang 7+ BPF target */
static __attribute__((always_inline)) unsigned int log2_bucket(unsigned long long val)
{
    if (val == 0)
        return 0;
    return 64 - (unsigned int) __builtin_clzll(val);
}

/* Per-CPU counter increment (no atomics needed) */
static __attribute__((always_inline)) void increment_counter(void *map, unsigned int key)
{
    unsigned long long *v = bpf_map_lookup_elem(map, &key);
    if (v)
        (*v)++;
}

/* Per-CPU histogram bucket increment */
static __attribute__((always_inline)) void increment_histogram(void *map, unsigned long long size)
{
    unsigned int bucket = log2_bucket(size);
    unsigned long long *v = bpf_map_lookup_elem(map, &bucket);
    if (v)
        (*v)++;
}

/*
 * Dynamic alignment mask (set by userspace before probes fire).
 * mask = alignment_bytes - 1.  0 => alignment checking disabled.
 * Examples: 64B -> 0x3F, 4KB -> 0xFFF, 2MB -> 0x1FFFFF
 *
 * 32-bit cast avoids Clang BPF backend LowerBR_CC crash on 64-bit
 * conditional branches (confirmed Clang 7.x, 16.x).
 */
__attribute__((section("maps"), used)) struct bpf_map_def alignment_mask = {
    .type = BPF_MAP_TYPE_ARRAY,
    .key_size = sizeof(unsigned int),
    .value_size = sizeof(unsigned int),
    .max_entries = 1,
};

/* Returns 1 if ptr is aligned to the configured boundary, 0 otherwise.
 * Returns 0 (not aligned) when alignment checking is disabled (mask==0). */
static __attribute__((always_inline)) int check_aligned(unsigned long long ptr)
{
    unsigned int zero = 0;
    unsigned int *mask = bpf_map_lookup_elem(&alignment_mask, &zero);
    if (!mask || *mask == 0)
        return 0;
    return (((unsigned int) (ptr) & *mask) == 0);
}

/* ========================================================================= */
/*  Map definition macros                                                    */
/* ========================================================================= */

/* Per-CPU counter map (1 entry) */
#define DEFINE_COUNTER_MAP(name)                                                                                       \
    __attribute__((section("maps"), used)) struct bpf_map_def name = {                                                 \
        .type = BPF_MAP_TYPE_PERCPU_ARRAY,                                                                             \
        .key_size = sizeof(unsigned int),                                                                              \
        .value_size = sizeof(unsigned long long),                                                                      \
        .max_entries = 1,                                                                                              \
    }

/* Per-CPU histogram map (64 log2 buckets) */
#define DEFINE_HISTOGRAM_MAP(name)                                                                                     \
    __attribute__((section("maps"), used)) struct bpf_map_def name = {                                                 \
        .type = BPF_MAP_TYPE_PERCPU_ARRAY,                                                                             \
        .key_size = sizeof(unsigned int),                                                                              \
        .value_size = sizeof(unsigned long long),                                                                      \
        .max_entries = 64,                                                                                             \
    }

/*
 * Distribution function maps: call counter + size histogram.
 * Used by functions whose size argument is meaningful (mem and string-n).
 */
#define DEFINE_DIST_MAPS(func)                                                                                         \
    DEFINE_COUNTER_MAP(dist_##func);                                                                                   \
    DEFINE_HISTOGRAM_MAP(lenHist_##func)

/* Count-only function maps: call counter (no histogram). */
#define DEFINE_COUNT_MAP(func) DEFINE_COUNTER_MAP(callCount_##func)

/* Alignment maps: dual-pointer (src, dst, both) */
#define DEFINE_ALIGNMENT_MAPS_DUAL(func)                                                                               \
    DEFINE_COUNTER_MAP(aligned_src_##func);                                                                            \
    DEFINE_COUNTER_MAP(aligned_dst_##func);                                                                            \
    DEFINE_COUNTER_MAP(aligned_both_##func)

/* Alignment maps: single-pointer (src only) */
#define DEFINE_ALIGNMENT_MAPS_SINGLE(func) DEFINE_COUNTER_MAP(aligned_src_##func)

/* ========================================================================= */
/*  Uprobe handler macros                                                    */
/* ========================================================================= */

/*
 * Distribution function, dual-pointer (dst=PARM1, src=PARM2, len=PARM3).
 * Used by: memcpy, mempcpy, memmove, strncpy, strncat
 */
#define DEFINE_UPROBE_DIST_DUAL(func)                                                                                  \
    __attribute__((section("uprobe/count_" #func), used)) int count_##func(struct pt_regs *ctx)                        \
    {                                                                                                                  \
        unsigned int zero = 0;                                                                                         \
        unsigned long long len = PT_REGS_PARM3(ctx);                                                                   \
        unsigned long long dst = PT_REGS_PARM1(ctx);                                                                   \
        unsigned long long src = PT_REGS_PARM2(ctx);                                                                   \
                                                                                                                       \
        increment_counter(&dist_##func, zero);                                                                         \
        increment_histogram(&lenHist_##func, len);                                                                     \
                                                                                                                       \
        if (check_aligned(src))                                                                                        \
            increment_counter(&aligned_src_##func, zero);                                                              \
        if (check_aligned(dst))                                                                                        \
            increment_counter(&aligned_dst_##func, zero);                                                              \
        if (check_aligned(src) && check_aligned(dst))                                                                  \
            increment_counter(&aligned_both_##func, zero);                                                             \
                                                                                                                       \
        return 0;                                                                                                      \
    }

/*
 * Distribution function, dual-pointer with reversed labelling
 * (ptr1=PARM1→dst alignment, ptr2=PARM2→src alignment, len=PARM3).
 * Used by: memcmp, strncmp
 */
#define DEFINE_UPROBE_DIST_DUAL_REV(func)                                                                              \
    __attribute__((section("uprobe/count_" #func), used)) int count_##func(struct pt_regs *ctx)                        \
    {                                                                                                                  \
        unsigned int zero = 0;                                                                                         \
        unsigned long long len = PT_REGS_PARM3(ctx);                                                                   \
        unsigned long long ptr1 = PT_REGS_PARM1(ctx);                                                                  \
        unsigned long long ptr2 = PT_REGS_PARM2(ctx);                                                                  \
                                                                                                                       \
        increment_counter(&dist_##func, zero);                                                                         \
        increment_histogram(&lenHist_##func, len);                                                                     \
                                                                                                                       \
        if (check_aligned(ptr2))                                                                                       \
            increment_counter(&aligned_src_##func, zero);                                                              \
        if (check_aligned(ptr1))                                                                                       \
            increment_counter(&aligned_dst_##func, zero);                                                              \
        if (check_aligned(ptr2) && check_aligned(ptr1))                                                                \
            increment_counter(&aligned_both_##func, zero);                                                             \
                                                                                                                       \
        return 0;                                                                                                      \
    }

/*
 * Distribution function, single-pointer (ptr=PARM1, len from len_parm).
 * Used by: memset (len=PARM3), memchr (len=PARM3), strnlen (len=PARM2)
 */
#define DEFINE_UPROBE_DIST_SINGLE(func, lparm)                                                                         \
    __attribute__((section("uprobe/count_" #func), used)) int count_##func(struct pt_regs *ctx)                        \
    {                                                                                                                  \
        unsigned int zero = 0;                                                                                         \
        unsigned long long len = lparm(ctx);                                                                           \
        unsigned long long ptr = PT_REGS_PARM1(ctx);                                                                   \
                                                                                                                       \
        increment_counter(&dist_##func, zero);                                                                         \
        increment_histogram(&lenHist_##func, len);                                                                     \
                                                                                                                       \
        if (check_aligned(ptr))                                                                                        \
            increment_counter(&aligned_src_##func, zero);                                                              \
                                                                                                                       \
        return 0;                                                                                                      \
    }

/*
 * Count-only function, dual-pointer (ptr1=PARM1, ptr2=PARM2).
 * Used by: strcpy, strcmp, strcat, strstr, strspn
 */
#define DEFINE_UPROBE_COUNT_DUAL(func)                                                                                 \
    __attribute__((section("uprobe/count_" #func), used)) int count_##func(struct pt_regs *ctx)                        \
    {                                                                                                                  \
        unsigned int zero = 0;                                                                                         \
        unsigned long long ptr1 = PT_REGS_PARM1(ctx);                                                                  \
        unsigned long long ptr2 = PT_REGS_PARM2(ctx);                                                                  \
                                                                                                                       \
        increment_counter(&callCount_##func, zero);                                                                    \
                                                                                                                       \
        if (check_aligned(ptr2))                                                                                       \
            increment_counter(&aligned_src_##func, zero);                                                              \
        if (check_aligned(ptr1))                                                                                       \
            increment_counter(&aligned_dst_##func, zero);                                                              \
        if (check_aligned(ptr2) && check_aligned(ptr1))                                                                \
            increment_counter(&aligned_both_##func, zero);                                                             \
                                                                                                                       \
        return 0;                                                                                                      \
    }

/*
 * Count-only function, single-pointer (ptr=PARM1).
 * Used by: strlen, strchr
 */
#define DEFINE_UPROBE_COUNT_SINGLE(func)                                                                               \
    __attribute__((section("uprobe/count_" #func), used)) int count_##func(struct pt_regs *ctx)                        \
    {                                                                                                                  \
        unsigned int zero = 0;                                                                                         \
        unsigned long long ptr = PT_REGS_PARM1(ctx);                                                                   \
                                                                                                                       \
        increment_counter(&callCount_##func, zero);                                                                    \
                                                                                                                       \
        if (check_aligned(ptr))                                                                                        \
            increment_counter(&aligned_src_##func, zero);                                                              \
                                                                                                                       \
        return 0;                                                                                                      \
    }

/* ========================================================================= */
/*  Map instantiations                                                       */
/* ========================================================================= */

/* Distribution functions: counter + histogram */
DEFINE_DIST_MAPS(memcpy);
DEFINE_DIST_MAPS(mempcpy);
DEFINE_DIST_MAPS(memcmp);
DEFINE_DIST_MAPS(memmove);
DEFINE_DIST_MAPS(memset);
DEFINE_DIST_MAPS(memchr);
DEFINE_DIST_MAPS(strncpy);
DEFINE_DIST_MAPS(strncmp);
DEFINE_DIST_MAPS(strncat);
DEFINE_DIST_MAPS(strnlen);

/* Count-only functions: counter only */
DEFINE_COUNT_MAP(strcpy);
DEFINE_COUNT_MAP(strcmp);
DEFINE_COUNT_MAP(strcat);
DEFINE_COUNT_MAP(strlen);
DEFINE_COUNT_MAP(strchr);
DEFINE_COUNT_MAP(strstr);
DEFINE_COUNT_MAP(strspn);

/* Alignment maps: dual-pointer functions */
DEFINE_ALIGNMENT_MAPS_DUAL(memcpy);
DEFINE_ALIGNMENT_MAPS_DUAL(mempcpy);
DEFINE_ALIGNMENT_MAPS_DUAL(memcmp);
DEFINE_ALIGNMENT_MAPS_DUAL(memmove);
DEFINE_ALIGNMENT_MAPS_DUAL(strncpy);
DEFINE_ALIGNMENT_MAPS_DUAL(strncmp);
DEFINE_ALIGNMENT_MAPS_DUAL(strncat);
DEFINE_ALIGNMENT_MAPS_DUAL(strcpy);
DEFINE_ALIGNMENT_MAPS_DUAL(strcmp);
DEFINE_ALIGNMENT_MAPS_DUAL(strcat);
DEFINE_ALIGNMENT_MAPS_DUAL(strstr);
DEFINE_ALIGNMENT_MAPS_DUAL(strspn);

/* Alignment maps: single-pointer functions */
DEFINE_ALIGNMENT_MAPS_SINGLE(memset);
DEFINE_ALIGNMENT_MAPS_SINGLE(memchr);
DEFINE_ALIGNMENT_MAPS_SINGLE(strnlen);
DEFINE_ALIGNMENT_MAPS_SINGLE(strchr);
DEFINE_ALIGNMENT_MAPS_SINGLE(strlen);

/* ========================================================================= */
/*  Uprobe handler instantiations                                            */
/* ========================================================================= */

/* Distribution, dual-pointer: dst=PARM1, src=PARM2, len=PARM3 */
DEFINE_UPROBE_DIST_DUAL(memcpy);
DEFINE_UPROBE_DIST_DUAL(mempcpy);
DEFINE_UPROBE_DIST_DUAL(memmove);
DEFINE_UPROBE_DIST_DUAL(strncpy);
DEFINE_UPROBE_DIST_DUAL(strncat);

/* Distribution, dual-pointer reversed: ptr2→src, ptr1→dst, len=PARM3 */
DEFINE_UPROBE_DIST_DUAL_REV(memcmp);
DEFINE_UPROBE_DIST_DUAL_REV(strncmp);

/* Distribution, single-pointer: ptr=PARM1, len from specified register */
DEFINE_UPROBE_DIST_SINGLE(memset, PT_REGS_PARM3);
DEFINE_UPROBE_DIST_SINGLE(memchr, PT_REGS_PARM3);
DEFINE_UPROBE_DIST_SINGLE(strnlen, PT_REGS_PARM2);

/* Count-only, dual-pointer: ptr1=PARM1, ptr2=PARM2 */
DEFINE_UPROBE_COUNT_DUAL(strcpy);
DEFINE_UPROBE_COUNT_DUAL(strcmp);
DEFINE_UPROBE_COUNT_DUAL(strcat);
DEFINE_UPROBE_COUNT_DUAL(strstr);
DEFINE_UPROBE_COUNT_DUAL(strspn);

/* Count-only, single-pointer: ptr=PARM1 */
DEFINE_UPROBE_COUNT_SINGLE(strlen);
DEFINE_UPROBE_COUNT_SINGLE(strchr);

/* ========================================================================= */
char __license[] __attribute__((section("license"), used)) = "Dual BSD/GPL";
