/* Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
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

/* Optimized AVX-512 implementation of strrchr.
 * Uses a two-phase approach:
 *   Phase 1: Scan forward to find null terminator and string length
 *   Phase 2: Scan backward in 64-byte chunks to find the last occurrence
 */

#include <stddef.h>
#include <stdint.h>
#include <immintrin.h>

#include "logger.h"
#include "almem_defs.h"
#include <zen_cpu_info.h>

#define ZMM_LAST_IDX (ZMM_SZ - 1u)

/* Process one vector: update last_match, return non-zero if null was found.
 * On null: truncates mask_c to bytes at/before the null, updates last_match,
 * and stores 1 in *done.  Caller must return last_match when *done is set.
 */
#define HANDLE_VEC(vec, mask_c, ptr, off, done)                                 \
    do {                                                                         \
        __mmask64 _null = _mm512_cmpeq_epu8_mask((vec), z0);                    \
        if (_null) {                                                             \
            unsigned _np  = (unsigned)_tzcnt_u64((unsigned long long)_null);    \
            __mmask64 _tr = (__mmask64)((_np < ZMM_LAST_IDX)                     \
                                        ? ((2ULL << _np) - 1ULL)               \
                                        : ~0ULL);                               \
            (mask_c) &= _tr;                                                    \
            if (mask_c) {                                                        \
                unsigned _i = (unsigned)(ZMM_LAST_IDX - _lzcnt_u64(             \
                                            (unsigned long long)(mask_c)));     \
                last_match = (char *)(ptr) + (off) + _i;                        \
            }                                                                    \
            (done) = 1;                                                          \
        } else if (mask_c) {                                                     \
            unsigned _i = (unsigned)(ZMM_LAST_IDX - _lzcnt_u64(                 \
                                        (unsigned long long)(mask_c)));         \
            last_match = (char *)(ptr) + (off) + _i;                           \
        }                                                                        \
    } while (0)

static inline char *_strrchr_avx512(const char *str, int c)
{
    const __m512i zch = _mm512_set1_epi8((char)c);
    const __m512i z0 = _mm512_setzero_si512();
    char *last_match = NULL;

    /* First chunk: page-boundary-aware initial load.
     *
     * Split into explicit hot/cold paths so the compiler emits zero mask-AND
     * instructions on the common (hot) path.  Merging into one block with
     * first_mask=~0ULL forces a KANDQ even when it is a no-op.
     *
     * Hot path (>99% of calls): str is not within the last ZMM_SZ bytes of
     * its page, so loadu is safe and all 64 loaded bytes are valid.
     *
     * Cold path: str is within the last ZMM_SZ bytes; masked load fills
     * unused bytes with 0xFF so null/match detection ignores them.
     */
    if (likely((PAGE_SZ - ZMM_SZ) >= ((PAGE_SZ - 1) & (uintptr_t)str)))
    {
        __m512i vec = _mm512_loadu_si512((const void *)str);
        __mmask64 mask_c = _mm512_cmpeq_epu8_mask(vec, zch);
        __mmask64 mask_null = _mm512_cmpeq_epu8_mask(vec, z0);

        if (mask_null)
        {
            unsigned null_pos = (unsigned)_tzcnt_u64((unsigned long long)mask_null);
            __mmask64 trunc = (__mmask64)((null_pos < ZMM_LAST_IDX)
                                            ? ((2ULL << null_pos) - 1ULL)
                                            : ~0ULL);
            mask_c &= trunc;
            if (mask_c)
                last_match = (char *)str
                             + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
            return last_match;
        }
        if (mask_c)
            last_match = (char *)str
                         + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
    }
    else
    {
        unsigned offset = (unsigned)((uintptr_t)str & (ZMM_SZ - 1));
        __mmask64 first_mask = (__mmask64)_bzhi_u64((uint64_t)-1, ZMM_SZ - offset);
        __m512i vec = _mm512_mask_loadu_epi8(_mm512_set1_epi8((char)0xff),
                                                       first_mask, str);
        __mmask64 mask_c = _mm512_cmpeq_epu8_mask(vec, zch) & first_mask;
        __mmask64 mask_null = _mm512_cmpeq_epu8_mask(vec, z0)  & first_mask;

        if (mask_null)
        {
            unsigned null_pos = (unsigned)_tzcnt_u64((unsigned long long)mask_null);
            __mmask64 trunc  = (__mmask64)((null_pos < ZMM_LAST_IDX)
                                            ? ((2ULL << null_pos) - 1ULL)
                                            : ~0ULL);
            mask_c &= trunc;
            if (mask_c)
                last_match = (char *)str
                             + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
            return last_match;
        }
        if (mask_c)
            last_match = (char *)str
                         + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
    }

    /* Advance to the first 64-byte aligned address after str */
    const char *ptr = (const char *)(((uintptr_t)str & ~(uintptr_t)(ZMM_SZ - 1)) + ZMM_SZ);

    /* Warmup: process up to 7 individual aligned vectors (covers strings
     * up to ~512B from str).  Each load is page-safe: if the prior vector
     * had no null, the string extends past it, so the next aligned address
     * is readable.  This avoids page_end computation for short strings. */
    {
        unsigned cnt = 7;
        while (cnt--)
        {
            __m512i vec = _mm512_load_si512((const void *)ptr);
            __mmask64 mask_c = _mm512_cmpeq_epu8_mask(vec, zch);
            __mmask64 mask_null = _mm512_cmpeq_epu8_mask(vec, z0);

            if (mask_null)
            {
                unsigned null_pos = (unsigned)_tzcnt_u64((unsigned long long)mask_null);
                __mmask64 trunc = (__mmask64)((null_pos < ZMM_LAST_IDX)
                                                ? ((2ULL << null_pos) - 1ULL)
                                                : ~0ULL);
                mask_c &= trunc;
                if (mask_c)
                    last_match = (char *)ptr
                                 + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
                return last_match;
            }
            if (mask_c)
                last_match = (char *)ptr
                             + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
            ptr += ZMM_SZ;
        }
    }

    /* Align ptr to a 4*ZMM_SZ boundary so the main loop's 256B blocks are
     * naturally aligned.  Residual 1–3 vectors are page-safe by same argument. */
    {
        unsigned tail = (unsigned)(4 - (((uintptr_t)ptr >> 6) & 3)) & 3;
        while (tail--)
        {
            __m512i vec = _mm512_load_si512((const void *)ptr);
            __mmask64 mask_c = _mm512_cmpeq_epu8_mask(vec, zch);
            __mmask64 mask_null = _mm512_cmpeq_epu8_mask(vec, z0);

            if (mask_null)
            {
                unsigned null_pos = (unsigned)_tzcnt_u64((unsigned long long)mask_null);
                __mmask64 trunc = (__mmask64)((null_pos < ZMM_LAST_IDX)
                                                ? ((2ULL << null_pos) - 1ULL)
                                                : ~0ULL);
                mask_c &= trunc;
                if (mask_c)
                    last_match = (char *)ptr
                                 + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
                return last_match;
            }
            if (mask_c)
                last_match = (char *)ptr
                             + (ZMM_LAST_IDX - (unsigned)_lzcnt_u64((unsigned long long)mask_c));
            ptr += ZMM_SZ;
        }
    }

    /* Page-aware 4-vector main loop.
     * ptr is 4*ZMM_SZ-aligned; page_end is 4096B-aligned (multiple of 256),
     * so the loop always exits with ptr == page_end — no fallback needed.
     * Loads are restricted to ptr+256 ≤ page_end to avoid speculative reads
     * across the page boundary.
     */
    while (1)
    {
        /* End of the current OS page (= start of the next page). */
        const char *page_end =
            (const char *)(((uintptr_t)ptr + PAGE_SZ) & ~(uintptr_t)(PAGE_SZ - 1));

        /* ---- 4-vector fast path ---- */
        while (ptr + 4 * ZMM_SZ <= page_end)
        {
            __m512i v1, v2, v3, v4, zmin;
            __mmask64 m1, m2, m3, m4, any_null, any_match;
            int done;

            /* non-faulting; safe even near a guard page */
            _mm_prefetch(ptr + 8 * ZMM_SZ, _MM_HINT_T0);

            v1 = _mm512_load_si512((const void *)(ptr));
            v2 = _mm512_load_si512((const void *)(ptr + ZMM_SZ));
            v3 = _mm512_load_si512((const void *)(ptr + 2 * ZMM_SZ));
            v4 = _mm512_load_si512((const void *)(ptr + 3 * ZMM_SZ));

            m1 = _mm512_cmpeq_epu8_mask(v1, zch);
            m2 = _mm512_cmpeq_epu8_mask(v2, zch);
            m3 = _mm512_cmpeq_epu8_mask(v3, zch);
            m4 = _mm512_cmpeq_epu8_mask(v4, zch);

            /* min across 4 vectors: zero byte survives iff any vector has null */
            zmin      = _mm512_min_epu8(_mm512_min_epu8(v1, v2),
                                        _mm512_min_epu8(v3, v4));
            any_null  = _mm512_cmpeq_epu8_mask(zmin, z0);
            any_match = m1 | m2 | m3 | m4;

            if (likely((any_match | any_null) == 0))
            {
                ptr += 4 * ZMM_SZ;
                continue;
            }

            done = 0;
            HANDLE_VEC(v1, m1, ptr, 0, done); if (done) return last_match;
            HANDLE_VEC(v2, m2, ptr, ZMM_SZ, done); if (done) return last_match;
            HANDLE_VEC(v3, m3, ptr, 2 * ZMM_SZ, done); if (done) return last_match;
            HANDLE_VEC(v4, m4, ptr, 3 * ZMM_SZ, done); if (done) return last_match;

            ptr += 4 * ZMM_SZ;
        }
        /* ptr == page_end here; outer loop advances to the next page. */
    }
}
#undef HANDLE_VEC
