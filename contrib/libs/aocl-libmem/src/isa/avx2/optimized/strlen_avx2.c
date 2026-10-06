/* Copyright (C) 2023-25 Advanced Micro Devices, Inc. All rights reserved.
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
#include <zen_cpu_info.h>

/* This function is an optimized version of strlen/strnlen using AVX2 instructions. */
#ifdef STRNLEN_AVX2
static inline size_t __attribute__((flatten)) _strnlen_avx2(const char *str, size_t maxlen)
#else
static inline size_t __attribute__((flatten)) _strlen_avx2(const char *str)
#endif
{
#ifdef STRNLEN_AVX2
    if (maxlen == 0)
        return 0;
#endif
    size_t offset;
    __m256i y0, y1, y2, y3, y4, y5, y6;
    int32_t  ret, ret1, ret2;

    y0 = _mm256_setzero_si256();

    if (unlikely(((PAGE_SZ - YMM_SZ) < ((PAGE_SZ - 1) & (uintptr_t)str))))
    {
        offset = (uintptr_t)str & (YMM_SZ - 1);
        y1 = _mm256_loadu_si256((void *)str - offset);
        ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
        ret = ret >> offset;
#ifdef STRNLEN_AVX2
        if (ret)
        {
            size_t pos = _tzcnt_u32(ret);
            return (pos < maxlen) ? pos : maxlen;
        }
        size_t safe_bytes = YMM_SZ - offset;
        if (safe_bytes >= maxlen)
            return maxlen;
        if (maxlen <= YMM_SZ)
        {
            size_t rem = maxlen - safe_bytes;
            y1 = _mm256_loadu_si256((void *) (str + safe_bytes));
            ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
            uint32_t mask = (rem >= 32) ? UINT32_MAX : ((1U << rem) - 1);
            ret = ret & mask;
            if (ret)
                return _tzcnt_u32(ret) + safe_bytes;
            return maxlen;
        }
#else
        if (ret)
            return _tzcnt_u32(ret);
#endif
    }
    else
    {
#ifdef STRNLEN_AVX2
        if (maxlen < YMM_SZ)
        {
            y1 = _mm256_loadu_si256((void *)str);
            ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
            uint32_t mask = (maxlen >= 32) ? UINT32_MAX : ((1U << maxlen) - 1);
            ret = ret & mask;
            if (ret)
                return _tzcnt_u32(ret);
            return maxlen;
        }
#endif
        y1 = _mm256_loadu_si256((void *)str);
        ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
        if (ret)
            return _tzcnt_u32(ret);
    }

#ifdef STRNLEN_AVX2
    if (YMM_SZ >= maxlen)
        return maxlen;
#endif

    // cache-line alignment
    if (!((uintptr_t)str & CACHE_LINE_OFFSET))
    {
        offset = ((uintptr_t)str & -(CACHE_LINE_OFFSET)) + YMM_SZ;
#ifdef STRNLEN_AVX2
        if (offset - (uintptr_t)str + YMM_SZ > maxlen)
        {
            if (offset - (uintptr_t)str >= maxlen)
                return maxlen;
            // Partial load
            size_t rem = maxlen - (offset - (uintptr_t)str);
            y1 = _mm256_loadu_si256((void *)offset);
            ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
            uint32_t mask = (rem >= 32) ? UINT32_MAX : ((1U << rem) - 1);
            ret = ret & mask;
            if (ret)
                return _tzcnt_u32(ret) + offset - (uintptr_t)str;
            return maxlen;
        }
#endif
        y1 = _mm256_load_si256((void *)offset);
        ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
        if (ret)
#ifdef STRNLEN_AVX2
        {
            size_t pos = _tzcnt_u32(ret) + offset - (uintptr_t)str;
            return (pos < maxlen) ? pos : maxlen;
        }
#else
            return _tzcnt_u32(ret) + offset - (uintptr_t)str;
#endif
    }

    // 4-vec alignment
    if (!((uintptr_t)str & AVX2_VEC_4_OFFSET))
    {
        offset = ((uintptr_t)str & -(AVX2_VEC_4_OFFSET)) + (2 * YMM_SZ);
#ifdef STRNLEN_AVX2
        if (offset - (uintptr_t)str + 2 * YMM_SZ > maxlen)
        {
            // Convert to byte offset and jump to main loop
            offset = offset - (uintptr_t)str;
            goto main_loop_with_offset;
        }
#endif
        y1 = _mm256_load_si256((void*)offset);
        y2 = _mm256_load_si256((void*)offset + YMM_SZ);
        y3 = _mm256_cmpeq_epi8(y1, y0);
        y4 = _mm256_cmpeq_epi8(y2, y0);
        ret = _mm256_movemask_epi8(_mm256_or_si256(y3, y4));
        if (ret)
        {
            ret1 = _mm256_movemask_epi8(y3);
            if (ret1)
#ifdef STRNLEN_AVX2
            {
                size_t pos = _tzcnt_u32(ret1) + offset - (uintptr_t)str;
                return (pos < maxlen) ? pos : maxlen;
            }
            size_t pos = _tzcnt_u32(ret) + offset + YMM_SZ - (uintptr_t)str;
            return (pos < maxlen) ? pos : maxlen;
#else
                return _tzcnt_u32(ret1) + offset - (uintptr_t)str;
            return _tzcnt_u32(ret) + offset + YMM_SZ - (uintptr_t)str;
#endif
        }
    }

    offset = (uintptr_t)str & -(AVX2_VEC_4_SZ);
#ifdef STRNLEN_AVX2
    offset = offset - (uintptr_t)str + AVX2_VEC_4_SZ;  // Convert to byte offset from str
    
main_loop_with_offset:
    while (offset + AVX2_VEC_4_SZ <= maxlen)
    {
        y1 = _mm256_load_si256((void*)(str + offset));
        y2 = _mm256_load_si256((void*)(str + offset + YMM_SZ));
        y3 = _mm256_load_si256((void*)(str + offset + 2 * YMM_SZ));
        y4 = _mm256_load_si256((void*)(str + offset + 3 * YMM_SZ));
#else
    do
    {
        offset += AVX2_VEC_4_SZ;
        y1 = _mm256_load_si256((void*)offset);
        y2 = _mm256_load_si256((void*)offset + YMM_SZ);
        y3 = _mm256_load_si256((void*)offset + 2 * YMM_SZ);
        y4 = _mm256_load_si256((void*)offset + 3 * YMM_SZ);
#endif

        y5 = _mm256_min_epu8(y1, y2);
        y6 = _mm256_min_epu8(y3, y4);

        ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_min_epu8(y5, y6), y0));
        
#ifdef STRNLEN_AVX2
        if (ret)
            break;
        
        offset += AVX2_VEC_4_SZ;
    }
    
    // Check if we found null in the 4-vector block
    if (ret)
    {
#else
    } while(!ret);
#endif

        if ((ret1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y5, y0))))
        {
            if ((ret2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0))))
            {
#ifdef STRNLEN_AVX2
                size_t pos = _tzcnt_u32(ret2) + offset;
                return (pos < maxlen) ? pos : maxlen;
#else
                return (_tzcnt_u32(ret2) + offset - (uintptr_t)str);
#endif
            }
#ifdef STRNLEN_AVX2
            size_t pos = _tzcnt_u32(ret1) + YMM_SZ + offset;
            return (pos < maxlen) ? pos : maxlen;
#else
            return (_tzcnt_u32(ret1) + YMM_SZ + offset  - (uintptr_t)str);
#endif
        }
        else
        {
            if ((ret2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y3, y0))))
            {
#ifdef STRNLEN_AVX2
                size_t pos = _tzcnt_u32(ret2) + 2 * YMM_SZ + offset;
                return (pos < maxlen) ? pos : maxlen;
#else
                return (_tzcnt_u32(ret2) + 2 * YMM_SZ + offset - (uintptr_t)str);
#endif
            }
#ifdef STRNLEN_AVX2
            size_t pos = _tzcnt_u32(ret) + 3 * YMM_SZ + offset;
            return (pos < maxlen) ? pos : maxlen;
#else
            return (_tzcnt_u32(ret) + 3 * YMM_SZ + offset - (uintptr_t)str);
#endif
        }
#ifdef STRNLEN_AVX2
    }

    // Handle remaining vectors
    while (offset + YMM_SZ <= maxlen)
    {
        y1 = _mm256_loadu_si256((void*)(str + offset));
        ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
        if (ret)
        {
            size_t pos = _tzcnt_u32(ret) + offset;
            return (pos < maxlen) ? pos : maxlen;
        }
        offset += YMM_SZ;
    }

    // Handle final partial vector
    if (offset < maxlen)
    {
        size_t rem = maxlen - offset;
        y1 = _mm256_loadu_si256((void*)(str + offset));
        ret = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
        uint32_t mask = (rem >= 32) ? UINT32_MAX : ((1U << rem) - 1);
        ret = ret & mask;
        if (ret)
        {
            size_t pos = _tzcnt_u32(ret) + offset;
            return (pos < maxlen) ? pos : maxlen;
        }
    }

    return maxlen;
#endif
}