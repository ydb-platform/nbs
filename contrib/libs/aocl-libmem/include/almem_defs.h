/* Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef _LIBMEM_DEFS_H_
#define _LIBMEM_DEFS_H_

#define PAGE_SZ                 4096
#define CACHELINE_SZ            64
#define STR_TERM_CHAR           '\0'
#define NULL_BYTE               1
#define ALL_BITS_SET            ((uint64_t)-1)
#define LOWER_BIT_SET           ((uint64_t)1)
#define NULL_MASK               0x0
#define CACHE_LINE_OFFSET       (1 << 5)
#define AVX2_VEC_4_OFFSET       (1 << 6)
#define AVX512_VEC_4_OFFSET     (1 << 7)
#define AVX2_VEC_4_SZ           (1 << 7)
#define AVX512_VEC_4_SZ         (1 << 8)
#define AVX512_VEC_OFFSET       (1 << 6)
#define ALL_BITS_SET_64         0xFFFFFFFFFFFFFFFF
#define NULL_TERM_CHAR         '\0'

#define PAGE_SAFE_VECS_BYTES(ptr) (PAGE_SZ - ((uintptr_t)(ptr) & (PAGE_SZ - 1)))
#define PAGE_SAFE_VECS(ptr)      (PAGE_SAFE_VECS_BYTES(ptr) >> 6)

#define PREFETCH_NTA(addr) _mm_prefetch((const char *) (addr), _MM_HINT_NTA)

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

#define __ALMEM_CONCAT2(x,y)    x##_##y
#define __ALMEM_CONCAT(x,y)     __ALMEM_CONCAT2(x,y)

#define __ALMEM_PREFIX(x)       __##x

#ifndef ALWAYS_INLINE
#if defined(__GNUC__) || defined(__clang__)
#define ALWAYS_INLINE __attribute__((always_inline))
#else
// Fallback for other compilers (may not guarantee inlining)*/
#define ALWAYS_INLINE inline
#endif
#endif

#ifndef HIDDEN_SYMBOL
#if defined(__GNUC__) || defined(__clang__)
#define HIDDEN_SYMBOL __attribute__((visibility("hidden")))
#else
// Define as empty for compilers that don't support this attribute
#define HIDDEN_SYMBOL
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __linux__
#define ALM_MEM_BARRIER() __asm__ __volatile__("":::"memory");
#define ALM_STORE_FENCE() __asm__ __volatile__("sfence" ::: "memory");

/*
    Intrinsic '_tzcnt_u16' is not supported on GCC major versions 11 and below.
    Henceforth, handling '_tzcnt_u16' with masked '_tzcnt_u32' intrinsics.
*/

#if (( __GNUC__ <= 11 ))
#define ALM_TZCNT_U16(x)    _tzcnt_u32((x) & 0xffff)
#else
#define ALM_TZCNT_U16(x)    _tzcnt_u16(x)
#endif

#endif

#ifdef __cplusplus
}
#endif

#endif //HEADER
