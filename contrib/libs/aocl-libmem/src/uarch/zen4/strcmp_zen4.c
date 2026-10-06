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

#include <stddef.h>
#include "logger.h"
#include <stdint.h>
#include <immintrin.h>
#include "almem_defs.h"
#include "zen_cpu_info.h"
#include "../../base_impls/load_compare_impls.h"

/* Zen4-optimized strcmp/strncmp using AVX-512. */
#ifdef STRNCMP
static inline int __attribute__((flatten)) _strncmp_zen4(const char *str1, const char *str2, size_t size)
#else
HIDDEN_SYMBOL int __attribute__((flatten)) __strcmp_zen4(const char *str1, const char *str2)
#endif
{
    LOG_INFO("\n");
#ifdef STRNCMP
    if (unlikely(size == 0))
        return 0;
#endif

    size_t offset1, offset2, offset;
    uint64_t cmp_idx, ret;
    __m512i z1, z2, z_mask;

    size_t dist1 = PAGE_SZ - ((uintptr_t)str1 & (PAGE_SZ - 1));
    size_t dist2 = PAGE_SZ - ((uintptr_t)str2 & (PAGE_SZ - 1));
    size_t safe = (dist1 < dist2) ? dist1 : dist2;

    if (likely(safe >= ZMM_SZ))
    {
        z1 = _mm512_loadu_si512(str1);
        z2 = _mm512_loadu_si512(str2);
        ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
        if (ret)
        {
            cmp_idx = _tzcnt_u64(ret);
#ifdef STRNCMP
            if (cmp_idx >= size)
                return 0;
#endif
            return (unsigned char)str1[cmp_idx] - (unsigned char)str2[cmp_idx];
        }
#ifdef STRNCMP
        if (size <= ZMM_SZ)
            return 0;
#endif

        offset1 = (uintptr_t)str1 & (ZMM_SZ - 1);
        offset2 = (uintptr_t)str2 & (ZMM_SZ - 1);
        offset = (offset1 >= offset2) ? offset1 : offset2;
    }
    else
    {
        z_mask = _mm512_set1_epi8((char)0xff);
        __mmask64 mask = _bzhi_u64(UINT64_MAX, safe);

        z1 = _mm512_mask_loadu_epi8(z_mask, mask, str1);
        z2 = _mm512_mask_loadu_epi8(z_mask, mask, str2);
        ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
        if (ret)
        {
            cmp_idx = _tzcnt_u64(ret);
#ifdef STRNCMP
            if (cmp_idx >= size)
                return 0;
#endif
            return (unsigned char)str1[cmp_idx] - (unsigned char)str2[cmp_idx];
        }
#ifdef STRNCMP
        if (size <= safe)
            return 0;
#endif

        offset1 = (uintptr_t)str1 & (ZMM_SZ - 1);
        offset2 = (uintptr_t)str2 & (ZMM_SZ - 1);
        offset = (offset1 >= offset2) ? offset1 : offset2;
    }

    offset = ZMM_SZ - offset;

    /*
     * Aligned case: 3x entry ramp then unified 4x reduction.
     */
    if (unlikely(offset1 == offset2))
    {
#ifdef STRNCMP
        if (offset + 3 * ZMM_SZ <= size)
#endif
        {
            z1 = _mm512_load_si512(str1 + offset);
            z2 = _mm512_load_si512(str2 + offset);
            ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
            if (ret)
                goto return_val;
            offset += ZMM_SZ;

            z1 = _mm512_load_si512(str1 + offset);
            z2 = _mm512_load_si512(str2 + offset);
            ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
            if (ret)
                goto return_val;
            offset += ZMM_SZ;

            z1 = _mm512_load_si512(str1 + offset);
            z2 = _mm512_load_si512(str2 + offset);
            ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
            if (ret)
                goto return_val;
            offset += ZMM_SZ;
        }

#ifdef STRNCMP
        while (offset < size)
#else
        while (1)
#endif
        {
            uint16_t v1 = PAGE_SAFE_VECS(str1 + offset);
            uint16_t v2 = PAGE_SAFE_VECS(str2 + offset);
            uint16_t vecs = (v1 < v2) ? v1 : v2;

            {
                size_t n4 = vecs >> 2;
                if (n4)
                    do
                    {
                        VEC_4X_LOAD_COMPARE_REDUCE(AVX512, ALIGNED)
                    } while (!ret && --n4);
                if (ret)
                    goto return_val;
                vecs &= 3;
            }

            while (vecs--)
            {
                z1 = _mm512_load_si512(str1 + offset);
                z2 = _mm512_load_si512(str2 + offset);
                ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
                if (ret)
                    goto return_val;
                offset += ZMM_SZ;
            }
        }
#ifdef STRNCMP
        return 0;
#endif
    }
    /* Unaligned path */
    else
    {
#ifdef STRNCMP
        while (offset < size)
#else
        while (1)
#endif
        {
            uint16_t s1 = PAGE_SAFE_VECS_BYTES(str1 + offset);
            uint16_t s2 = PAGE_SAFE_VECS_BYTES(str2 + offset);
            uint16_t safe_bytes = (s1 < s2) ? s1 : s2;
            uint16_t vecs = safe_bytes >> 6;

            if (unlikely(vecs == 0))
            {
                z_mask = _mm512_set1_epi8((char)0xff);
                __mmask64 mask = _bzhi_u64(UINT64_MAX, safe_bytes);
                z1 = _mm512_mask_loadu_epi8(z_mask, mask, str1 + offset);
                z2 = _mm512_mask_loadu_epi8(z_mask, mask, str2 + offset);
                ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
                if (ret)
                    goto return_val;
                offset += safe_bytes;
                continue;
            }

            {
                size_t n4 = vecs >> 2;
                if (n4)
                    do
                    {
                        VEC_4X_LOAD_COMPARE_REDUCE(AVX512, UNALIGNED)
                    } while (!ret && --n4);
                if (ret)
                    goto return_val;
                vecs &= 3;
            }

            while (vecs--)
            {
                z1 = _mm512_loadu_si512(str1 + offset);
                z2 = _mm512_loadu_si512(str2 + offset);
                ret = _mm512_testn_epi8_mask(z1, z1) | _mm512_cmpneq_epu8_mask(z1, z2);
                if (ret)
                    goto return_val;
                offset += ZMM_SZ;
            }
        }
#ifdef STRNCMP
        return 0;
#endif
    }

return_val:
    cmp_idx = _tzcnt_u64(ret) + offset;
#ifdef STRNCMP
    if (cmp_idx >= size)
        return 0;
#endif
    return (unsigned char)str1[cmp_idx] - (unsigned char)str2[cmp_idx];
}

#ifndef STRNCMP
#ifndef ALMEM_DYN_DISPATCH
int strcmp(const char *, const char *) __attribute__((weak, alias("__strcmp_zen4")));
#endif
#endif
