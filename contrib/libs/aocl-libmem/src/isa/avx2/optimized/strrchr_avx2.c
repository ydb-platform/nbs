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

/* Optimized AVX2 implementation of strrchr.
 * Uses a single forward scan and defers final last-match resolution until the
 * terminating null byte is observed.
 */

#include <stddef.h>
#include <stdint.h>
#include <immintrin.h>

#include "logger.h"
#include "almem_defs.h"
#include <zen_cpu_info.h>

#define YMM_LAST_IDX (YMM_SZ - 1u)

static inline unsigned _last_bit_idx_avx2(uint32_t mask)
{
    return YMM_LAST_IDX - (unsigned)_lzcnt_u32(mask);
}

static inline char *_update_last_avx2(char *last, const char *ptr, uint32_t mask)
{
    return mask ? (char *)ptr + _last_bit_idx_avx2(mask) : last;
}

static inline char * __attribute__((flatten)) _strrchr_avx2(const char *str, int c)
{
    const __m256i y0 = _mm256_setzero_si256();
    const __m256i ych = _mm256_set1_epi8((char)c);
    /* Align down and load from the containing 32-byte block.
     * Safety: first iteration masks out bytes before str via 'valid', so they
     * cannot participate in null/match detection.  This keeps the hot path to
     * one aligned load + mask ops, avoiding an extra page-crossing branch.
     */
    const char *ptr = (const char *)((uintptr_t)str & ~(uintptr_t)(YMM_SZ - 1));
    unsigned start_byte = (unsigned)((uintptr_t)str & (YMM_SZ - 1));
    uint32_t valid = ~0u << start_byte;
    char *last = NULL;

    /* First vector may include bytes before str; gate them out with 'valid'. */
    {
        __m256i y1 = _mm256_load_si256((const __m256i *)ptr);
        uint32_t mask_c = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, ych)) & valid;
        uint32_t mask_end = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0)) & valid;

        if (mask_end)
        {
            unsigned end_idx = (unsigned)_tzcnt_u32(mask_end);
            uint32_t before = (end_idx < YMM_LAST_IDX) ? ((2u << end_idx) - 1u) : ~0u;
            mask_c &= before;
            if (mask_c)
                last = _update_last_avx2(last, ptr, mask_c);
            return last;
        }

        if (mask_c)
            last = _update_last_avx2(last, ptr, mask_c);

        ptr += YMM_SZ;
    }

    /* Warmup: keep short-string overhead low before entering the 4x path. */
    {
        unsigned cnt = 1;
        while (cnt--)
        {
            __m256i y1 = _mm256_load_si256((const __m256i *)ptr);
            uint32_t mask_c = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, ych));
            uint32_t mask_end = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));

            if (mask_end)
            {
                unsigned end_idx = (unsigned)_tzcnt_u32(mask_end);
                uint32_t before = (end_idx < YMM_LAST_IDX) ? ((2u << end_idx) - 1u) : ~0u;
                mask_c &= before;
                if (mask_c)
                    last = _update_last_avx2(last, ptr, mask_c);
                return last;
            }

            if (mask_c)
                last = _update_last_avx2(last, ptr, mask_c);

            ptr += YMM_SZ;
        }
    }

    while (1)
    {
        const char *page_end =
            (const char *)(((uintptr_t)ptr + PAGE_SZ) & ~(uintptr_t)(PAGE_SZ - 1));

        while (ptr + 4 * YMM_SZ <= page_end)
        {
            __m256i y1, y2, y3, y4;
            __m256i z1, z2, z3, z4, z5, z6, zmin;
            uint32_t m1, m2, m3, m4;
            uint32_t candidate;
            uint32_t mask_end;
            unsigned end_idx;
            uint32_t before;

            y1 = _mm256_load_si256((const __m256i *)ptr);
            y2 = _mm256_load_si256((const __m256i *)(ptr + YMM_SZ));
            y3 = _mm256_load_si256((const __m256i *)(ptr + 2 * YMM_SZ));
            y4 = _mm256_load_si256((const __m256i *)(ptr + 3 * YMM_SZ));

            /* strchr-style candidate detector:
             * min(xor(v, ch), v) == 0 iff byte is either ch or '\0'.
             * Build exact masks only when a candidate exists in this 4x block.
             */
            z1 = _mm256_min_epu8(_mm256_xor_si256(y1, ych), y1);
            z2 = _mm256_min_epu8(_mm256_xor_si256(y2, ych), y2);
            z3 = _mm256_min_epu8(_mm256_xor_si256(y3, ych), y3);
            z4 = _mm256_min_epu8(_mm256_xor_si256(y4, ych), y4);
            z5 = _mm256_min_epu8(z1, z2);
            z6 = _mm256_min_epu8(z3, z4);
            zmin = _mm256_min_epu8(z5, z6);
            candidate = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(zmin, y0));

            if (likely(candidate == 0))
            {
                ptr += 4 * YMM_SZ;
                continue;
            }

            m1 = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, ych));
            m2 = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y2, ych));
            m3 = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y3, ych));
            m4 = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y4, ych));

            mask_end = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
            if (mask_end)
            {
                end_idx = (unsigned)_tzcnt_u32(mask_end);
                before = (end_idx < YMM_LAST_IDX) ? ((2u << end_idx) - 1u) : ~0u;
                m1 &= before;
                if (m1)
                    last = _update_last_avx2(last, ptr, m1);
                return last;
            }
            if (m1)
                last = _update_last_avx2(last, ptr, m1);

            mask_end = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y2, y0));
            if (mask_end)
            {
                end_idx = (unsigned)_tzcnt_u32(mask_end);
                before = (end_idx < YMM_LAST_IDX) ? ((2u << end_idx) - 1u) : ~0u;
                m2 &= before;
                if (m2)
                    last = _update_last_avx2(last, ptr + YMM_SZ, m2);
                return last;
            }
            if (m2)
                last = _update_last_avx2(last, ptr + YMM_SZ, m2);

            mask_end = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y3, y0));
            if (mask_end)
            {
                end_idx = (unsigned)_tzcnt_u32(mask_end);
                before = (end_idx < YMM_LAST_IDX) ? ((2u << end_idx) - 1u) : ~0u;
                m3 &= before;
                if (m3)
                    last = _update_last_avx2(last, ptr + 2 * YMM_SZ, m3);
                return last;
            }
            if (m3)
                last = _update_last_avx2(last, ptr + 2 * YMM_SZ, m3);

            mask_end = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y4, y0));
            if (mask_end)
            {
                end_idx = (unsigned)_tzcnt_u32(mask_end);
                before = (end_idx < YMM_LAST_IDX) ? ((2u << end_idx) - 1u) : ~0u;
                m4 &= before;
                if (m4)
                    last = _update_last_avx2(last, ptr + 3 * YMM_SZ, m4);
                return last;
            }
            if (m4)
                last = _update_last_avx2(last, ptr + 3 * YMM_SZ, m4);

            ptr += 4 * YMM_SZ;
        }

        while (ptr < page_end)
        {
            __m256i y1 = _mm256_load_si256((const __m256i *)ptr);
            uint32_t mask_c = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, ych));
            uint32_t mask_end = (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));

            if (mask_end)
            {
                unsigned end_idx = (unsigned)_tzcnt_u32(mask_end);
                uint32_t before = (end_idx < YMM_LAST_IDX) ? ((2u << end_idx) - 1u) : ~0u;
                mask_c &= before;
                if (mask_c)
                    last = _update_last_avx2(last, ptr, mask_c);
                return last;
            }

            if (mask_c)
                last = _update_last_avx2(last, ptr, mask_c);

            ptr += YMM_SZ;
        }
    }
}
