/* Copyright (C) 2022-25 Advanced Micro Devices, Inc. All rights reserved.
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
#ifndef _LIBMEM_H_
#define _LIBMEM_H_

#include "almem_defs.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum{
    MEMCPY,
    MEMPCPY,
    MEMMOVE,
    MEMSET,
    MEMCMP, //end of Tunable supported funcs
    MEMCHR,
    STRCPY,
    STRNCPY,
    STRCMP,
    STRNCMP,
    STRCAT,
    STRNCAT,
    STRSTR,
    STRLEN,
    STRNLEN,
    STRCHR,
    STRSPN,
    STRRCHR,
    FUNC_COUNT,
    TUN_FUNC_COUNT = MEMCMP + 1
}func_index;

#ifdef ALMEM_TUNABLES
typedef enum{
/*User AVX2 operation based*/
    AVX2_UNALIGNED,
    AVX2_ALIGNED,
    AVX2_ALIGNED_LOAD,
    AVX2_ALIGNED_STORE,
    AVX2_NON_TEMPORAL,
    AVX2_NON_TEMPORAL_LOAD,
    AVX2_NON_TEMPORAL_STORE,
/*User AVX512 operation based*/
    AVX512_UNALIGNED,
    AVX512_ALIGNED,
    AVX512_ALIGNED_LOAD,
    AVX512_ALIGNED_STORE,
    AVX512_NON_TEMPORAL,
    AVX512_NON_TEMPORAL_LOAD,
    AVX512_NON_TEMPORAL_STORE,
/*User ERMS operation based*/
    ERMS_MOVSB,
    ERMS_MOVSW,
    ERMS_MOVSD,
    ERMS_MOVSQ,
/*User Threshold with vec-temporal, erms, vec-non-temporal*/
    THRESHOLD,
    TUN_VARIANT_COUNT,
    UNKNOWN
} tunable_variant_idx;
#endif //end of tunables

typedef enum {
    /*uArch based*/
    ARCH_ZEN1,
    ARCH_ZEN2,
    ARCH_ZEN3,
    ARCH_ZEN4,
    ARCH_ZEN5,
    ARCH_ZEN6,
    /*System feature & Threshold*/
    SYSTEM,
    CPU_VARIANT_COUNT
} cpu_variant_idx;

typedef void (*func_ptr)(void);

#ifdef ALMEM_TUNABLES
#define generate_avx2_variants(func) \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx2_unaligned), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx2_aligned), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx2_aligned_load), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx2_aligned_store), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx2_nt), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx2_nt_load), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx2_nt_store),


#define generate_avx512_variants(func) \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx512_unaligned), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx512_aligned), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx512_aligned_load), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx512_aligned_store), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx512_nt), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx512_nt_load), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), avx512_nt_store),

#define generate_erms_variants(func) \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), erms_b_aligned), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), erms_w_aligned), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), erms_d_aligned), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), erms_q_aligned),

#define generate_threshold_variant(func) \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), threshold),

#define add_tun_func_variants(func) \
    { \
        generate_avx2_variants(func) \
        generate_avx512_variants(func) \
        generate_erms_variants(func) \
        generate_threshold_variant(func) \
    }

// Tunable Dispatching Table
HIDDEN_SYMBOL func_ptr libmem_tun_impls[TUN_FUNC_COUNT][TUN_VARIANT_COUNT] =
{
    add_tun_func_variants(memcpy),
    add_tun_func_variants(mempcpy),
    add_tun_func_variants(memmove),
    add_tun_func_variants(memset),
    add_tun_func_variants(memcmp)
};


#endif //end of tunable variants

#define generate_arch_variants(func) \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen1), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen2), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen3), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen4), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen5), \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen6),

#define generate_system_variant(func) \
    (func_ptr) __ALMEM_CONCAT(__ALMEM_PREFIX(func), system)

#define add_cpu_func_variants(func) \
    { \
        generate_arch_variants(func) \
        generate_system_variant(func) \
    }


// CPU Dispatching Table
HIDDEN_SYMBOL func_ptr libmem_cpu_impls[FUNC_COUNT][CPU_VARIANT_COUNT] =
{
    add_cpu_func_variants(memcpy),
    add_cpu_func_variants(mempcpy),
    add_cpu_func_variants(memmove),
    add_cpu_func_variants(memset),
    add_cpu_func_variants(memcmp),
    add_cpu_func_variants(memchr),
    add_cpu_func_variants(strcpy),
    add_cpu_func_variants(strncpy),
    add_cpu_func_variants(strcmp),
    add_cpu_func_variants(strncmp),
    add_cpu_func_variants(strcat),
    add_cpu_func_variants(strncat),
    add_cpu_func_variants(strstr),
    add_cpu_func_variants(strlen),
    add_cpu_func_variants(strnlen),
    add_cpu_func_variants(strchr),
    add_cpu_func_variants(strspn),
    add_cpu_func_variants(strrchr),
};


#ifdef __cplusplus
}
#endif

#endif
