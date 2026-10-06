/* Copyright (C) 2022-26 Advanced Micro Devices, Inc. All rights reserved.
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

#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>

#include "almem_defs.h"
#include "logger.h"
#include "threshold.h"
#include "zen_cpu_info.h"

extern cpu_info zen_info;

#define MEMCMP_PREFETCH_DISTANCE (16 * ZMM_SZ) /* 1024 B ahead */
#define MEMCMP_PREFETCH_MIN_SIZE (1 << 20)     /* 1 MB  */
#define MEMCMP_PREFETCH_MAX_SIZE (12 << 20)    /* 12 MB */

static inline int __attribute__((flatten)) _memcmp_avx512(const void *mem1, const void *mem2, size_t size)
{
    if (unlikely(size == 0 || mem1 == mem2))
        return 0;

    if (size <= YMM_SZ)
    {
        /* 0-32B: single masked YMM compare. */
        __mmask32 k = _bzhi_u32((uint32_t) -1, (uint32_t) size);
        __m256i y0 = _mm256_maskz_loadu_epi8(k, mem1);
        __m256i y1 = _mm256_maskz_loadu_epi8(k, mem2);
        __mmask32 m = _mm256_cmpneq_epu8_mask(y0, y1);
        if (m == 0)
            return 0;
        uint32_t pos = _tzcnt_u32(m);
        return ((*(uint8_t *) (mem1 + pos)) - (*(uint8_t *) (mem2 + pos)));
    }

    __m512i z0, z1;
    size_t offset, index = 0;
    __mmask64 mask;

    if (likely(size <= ZMM_SZ))
    {
        /* 33-64B: two overlapping YMM compares*/
        __m256i y0 = _mm256_loadu_si256((const __m256i *) mem1);
        __m256i y1 = _mm256_loadu_si256((const __m256i *) mem2);
        __mmask32 m = _mm256_cmpneq_epu8_mask(y0, y1);
        if (m != 0)
        {
            uint32_t pos = _tzcnt_u32(m);
            return ((*(uint8_t *) (mem1 + pos)) - (*(uint8_t *) (mem2 + pos)));
        }
        y0 = _mm256_loadu_si256((const __m256i *) (mem1 + size - YMM_SZ));
        y1 = _mm256_loadu_si256((const __m256i *) (mem2 + size - YMM_SZ));
        m = _mm256_cmpneq_epu8_mask(y0, y1);
        if (m != 0)
        {
            uint32_t pos = (size - YMM_SZ) + _tzcnt_u32(m);
            return ((*(uint8_t *) (mem1 + pos)) - (*(uint8_t *) (mem2 + pos)));
        }
        return 0;
    }

    /* >64B: compare the first ZMM, early-exit on mismatch. */
    z0 = _mm512_loadu_si512(mem1);
    z1 = _mm512_loadu_si512(mem2);
    mask = _mm512_cmpneq_epu8_mask(z0, z1);
    if (unlikely(mask != 0))
    {
        index = _tzcnt_u64(mask);
        return ((*(uint8_t *) (mem1 + index)) - (*(uint8_t *) (mem2 + index)));
    }

    if (size <= 2 * ZMM_SZ)
    {
        /* 65-128B: overlapping final ZMM. */
        index = size - ZMM_SZ;
        z0 = _mm512_loadu_si512(mem1 + index);
        z1 = _mm512_loadu_si512(mem2 + index);
        mask = _mm512_cmpneq_epu8_mask(z0, z1);
        if (likely(mask == 0))
            return 0;
        index += _tzcnt_u64(mask);
        return ((*(uint8_t *) (mem1 + index)) - (*(uint8_t *) (mem2 + index)));
    }

    if (size <= 4 * ZMM_SZ)
    {
        /* 129-256B: three remaining ZMM blocks */
        __mmask64 m1 = _mm512_cmpneq_epu8_mask(_mm512_loadu_si512(mem1 + ZMM_SZ), _mm512_loadu_si512(mem2 + ZMM_SZ));
        __mmask64 m2 = _mm512_cmpneq_epu8_mask(_mm512_loadu_si512(mem1 + size - 2 * ZMM_SZ),
                                               _mm512_loadu_si512(mem2 + size - 2 * ZMM_SZ));
        __mmask64 m3 =
            _mm512_cmpneq_epu8_mask(_mm512_loadu_si512(mem1 + size - ZMM_SZ), _mm512_loadu_si512(mem2 + size - ZMM_SZ));
        if (likely((m1 | m2 | m3) == 0))
            return 0;
        if (m1 != 0)
            index = ZMM_SZ + _tzcnt_u64(m1);
        else if (m2 != 0)
            index = (size - 2 * ZMM_SZ) + _tzcnt_u64(m2);
        else
            index = (size - ZMM_SZ) + _tzcnt_u64(m3);
        return ((*(uint8_t *) (mem1 + index)) - (*(uint8_t *) (mem2 + index)));
    }

    /* >256B: per-ZMM loop with immediate early-exit on each 64B block. */
    offset = ZMM_SZ;
    if ((size >= MEMCMP_PREFETCH_MIN_SIZE) && (size <= MEMCMP_PREFETCH_MAX_SIZE))
    {
        while ((size - ZMM_SZ) >= offset)
        {
            PREFETCH_NTA(mem1 + offset + MEMCMP_PREFETCH_DISTANCE);
            PREFETCH_NTA(mem2 + offset + MEMCMP_PREFETCH_DISTANCE);
            z0 = _mm512_loadu_si512(mem1 + offset);
            z1 = _mm512_loadu_si512(mem2 + offset);
            mask = _mm512_cmpneq_epu8_mask(z0, z1);
            if (unlikely(mask != 0))
            {
                index = _tzcnt_u64(mask) + offset;
                return ((*(uint8_t *) (mem1 + index)) - (*(uint8_t *) (mem2 + index)));
            }
            offset += ZMM_SZ;
        }
    }
    else
    {
        while ((size - ZMM_SZ) >= offset)
        {
            z0 = _mm512_loadu_si512(mem1 + offset);
            z1 = _mm512_loadu_si512(mem2 + offset);
            mask = _mm512_cmpneq_epu8_mask(z0, z1);
            if (unlikely(mask != 0))
            {
                index = _tzcnt_u64(mask) + offset;
                return ((*(uint8_t *) (mem1 + index)) - (*(uint8_t *) (mem2 + index)));
            }
            offset += ZMM_SZ;
        }
    }

    /* tail: overlapping final ZMM. */
    if (offset < size)
    {
        index = size - ZMM_SZ;
        z0 = _mm512_loadu_si512(mem1 + index);
        z1 = _mm512_loadu_si512(mem2 + index);
        mask = _mm512_cmpneq_epu8_mask(z0, z1);
        if (unlikely(mask != 0))
        {
            index += _tzcnt_u64(mask);
            return ((*(uint8_t *) (mem1 + index)) - (*(uint8_t *) (mem2 + index)));
        }
    }

    return 0;
}
