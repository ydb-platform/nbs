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

#include "almem_defs.h"
#include "logger.h"
#include <immintrin.h>
#include <nmmintrin.h>
#include <stddef.h>
#include <zen_cpu_info.h>

/* Strspn AVX2 implementation */
static inline size_t __attribute__((flatten)) _strspn_avx2(const char *s, const char *accept)
{
    if (unlikely(!accept[0] || !s[0]))
    {
        return 0;
    }

    /* Single-char accept: align down + masked first load, then YMM loop */
    if (unlikely(!accept[1]))
    {
        __m256i vc = _mm256_set1_epi8(accept[0]);
        __m256i zero = _mm256_setzero_si256();

        /* Align down — always safe, same page as s */
        const char *ptr = (const char *) ((uintptr_t) s & ~((size_t)YMM_SZ - 1));
        unsigned skip = (unsigned) ((uintptr_t) s & (YMM_SZ - 1));

        /* First aligned load, mask out bytes before s */
        __m256i block = _mm256_load_si256((const __m256i *) ptr);
        __m256i neq = _mm256_andnot_si256(_mm256_cmpeq_epi8(block, vc), _mm256_set1_epi8((char) 0xFF));
        __m256i nul = _mm256_cmpeq_epi8(block, zero);
        uint32_t stop = (uint32_t) _mm256_movemask_epi8(_mm256_or_si256(neq, nul)) >> skip;
        if (stop)
            return (size_t) _tzcnt_u32(stop);
        ptr += YMM_SZ;

        /* Main loop — aligned, no page checks */
        while (1)
        {
            block = _mm256_load_si256((const __m256i *) ptr);
            neq = _mm256_andnot_si256(_mm256_cmpeq_epi8(block, vc), _mm256_set1_epi8((char) 0xFF));
            nul = _mm256_cmpeq_epi8(block, zero);
            stop = (uint32_t) _mm256_movemask_epi8(_mm256_or_si256(neq, nul));
            if (unlikely(stop))
                return (size_t) (ptr - s) + (size_t) _tzcnt_u32(stop);
            ptr += YMM_SZ;
        }
    }

    /* Page boundary check for accept */
    __m128i accept_vec;
    size_t accept_off = (uintptr_t) accept & (PAGE_SZ - 1);
    if (unlikely(accept_off > (PAGE_SZ - XMM_SZ)))
    {
        char buf[XMM_SZ] __attribute__((aligned(XMM_SZ))) = {0};
        for (int i = 0; i < XMM_SZ && accept[i]; i++)
        {
            buf[i] = accept[i];
        }
        accept_vec = _mm_load_si128((const __m128i *) buf);
    } else
    {
        accept_vec = _mm_loadu_si128((const __m128i *) accept);
    }

    /* Check if accept has null in first XMM_SZ bytes OR accept[XMM_SZ] is null */
    __m128i zero_xmm = _mm_setzero_si128();
    int null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(accept_vec, zero_xmm));

    /* If no null in first XMM_SZ bytes, check if accept[XMM_SZ] exists */
    if (!null_mask && accept[XMM_SZ])
    {
        /* accept > 16 chars - use 256-byte LUT */
        uint8_t lut[256] __attribute__((aligned(YMM_SZ))) = {0};
        for (const unsigned char *a = (const unsigned char *) accept; *a; ++a)
        {
            lut[*a] = *a;
        }

        const unsigned char *ptr = (const unsigned char *) s;

        /* 4x unrolled scalar loop with LUT */
        while (1)
        {
            unsigned char c0 = ptr[0];
            if (!lut[c0])
            {
                return (size_t) (ptr - (const unsigned char *) s);
            }

            unsigned char c1 = ptr[1];
            if (!lut[c1])
            {
                return (size_t) (ptr + 1 - (const unsigned char *) s);
            }

            unsigned char c2 = ptr[2];
            if (!lut[c2])
            {
                return (size_t) (ptr + 2 - (const unsigned char *) s);
            }

            unsigned char c3 = ptr[3];
            if (!lut[c3])
            {
                return (size_t) (ptr + 3 - (const unsigned char *) s);
            }

            ptr += 4;
        }
    }

    /* accept <= 16 chars - use SSE4.2 pcmpistri */
    {
        const char *ptr = s;
        int idx;

        /* First chunk - check page boundary */
        size_t page_off = (uintptr_t) s & (PAGE_SZ - 1);
        if (likely(page_off <= (PAGE_SZ - XMM_SZ)))
        {
            /*
             * _mm_cmpistri with (_SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY):
             * - Compares each byte in the source operand against ALL bytes in the accept operand
             * - Returns the index of the first byte NOT found in accept (or the first null byte)
             * - Handles null terminators implicitly in both operands
             */
            idx = _mm_cmpistri(accept_vec, _mm_loadu_si128((const __m128i *) s),
                               _SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY);
            if (idx < XMM_SZ)
            {
                return (size_t) idx;
            }
            ptr = s + XMM_SZ;
        } else
        {
            /* Near page boundary - byte by byte until aligned */
            while ((uintptr_t) ptr & 15)
            {
                char c = *ptr;
                if (!c)
                {
                    return (size_t) (ptr - s);
                }
                const char *a = accept;
                while (*a && *a != c)
                {
                    a++;
                }
                if (!*a)
                {
                    return (size_t) (ptr - s);
                }
                ptr++;
            }
        }

        /* Align to 16 bytes for fast loads */
        ptr = (const char *) (((uintptr_t) ptr) & ~15ULL);

        /* Main loop - 2x16B per iteration */
        while (1)
        {
            idx = _mm_cmpistri(accept_vec, _mm_load_si128((const __m128i *) ptr),
                               _SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY);
            if (idx < XMM_SZ)
            {
                return (size_t) (ptr - s) + (size_t) idx;
            }

            idx = _mm_cmpistri(accept_vec, _mm_load_si128((const __m128i *) (ptr + XMM_SZ)),
                               _SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY);
            if (idx < XMM_SZ)
            {
                return (size_t) (ptr - s) + XMM_SZ + (size_t) idx;
            }

            ptr += YMM_SZ;
        }
    }
}
