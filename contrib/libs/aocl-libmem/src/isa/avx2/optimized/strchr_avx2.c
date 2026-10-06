/* Copyright (C) 2024 Advanced Micro Devices, Inc. All rights reserved.
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
#include <immintrin.h>
#include "almem_defs.h"
#include "zen_cpu_info.h"

static inline char * __attribute__((flatten)) _strchr_avx2(const char *str, int c)
{
    size_t  offset = 0;
    __m256i y0, y1, y2, y3, y4, ych, yxor;
    __m256i y5, y6, y7, y8;
    __mmask32 match_mask, match_mask1, match_mask2;
    uint32_t  match_idx;
    char ch = (char)c;
    y0 = _mm256_setzero_si256 ();
    ych =  _mm256_set1_epi8(ch);

    if (unlikely(((PAGE_SZ - YMM_SZ) < ((PAGE_SZ - 1) & (uintptr_t)str))))
    {
        offset = (uintptr_t)str & (YMM_SZ - 1);
    }
    y1 = _mm256_loadu_si256((void *)str - offset);
    yxor = _mm256_xor_si256(y1, ych);
    match_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_min_epu8(yxor, y1),y0));
    match_mask = match_mask >> offset;
    if (match_mask)
    {
        match_idx = _tzcnt_u32(match_mask);
        char *result = (char *)str + match_idx;
        if (!(*result ^ ch))
            return result;
        return NULL;
    }
    // cache-line alignment
    if (!((uintptr_t)str & CACHE_LINE_OFFSET))
    {
        offset = ((uintptr_t)str & -(CACHE_LINE_OFFSET)) + YMM_SZ;
        y1 = _mm256_load_si256((void *)offset);

        yxor = _mm256_xor_si256(y1, ych);
        match_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_min_epu8(yxor, y1),y0));

        if(match_mask)
        {
            match_idx = _tzcnt_u32(match_mask);
            char* result = (char*)(offset + match_idx);
            if (!(*result ^ ch))
                return result;
            return NULL;
        }
    }
    if (!((uintptr_t)str & AVX2_VEC_4_OFFSET))
    {
        offset = ((uintptr_t)str & -(AVX2_VEC_4_OFFSET)) + (2 * YMM_SZ);
        y1 = _mm256_load_si256((void*)offset);
        y2 = _mm256_load_si256((void*)offset + YMM_SZ);

        y3 = _mm256_xor_si256(y1, ych);
        y4 = _mm256_xor_si256(y2, ych);

        y3 = _mm256_min_epu8(y3, y1);
        y4 = _mm256_min_epu8(y4, y2);

        y5 = _mm256_min_epu8(y3, y4);

        match_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y5, y0));
        if (match_mask)
        {
            match_mask1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y3,y0));
            if (match_mask1)
            {    match_idx = _tzcnt_u32(match_mask1);
                char* result = (char*)(offset + match_idx);
                if (!(*result ^ ch))
                    return result;
                return NULL;
            }
            match_idx = _tzcnt_u32(match_mask);
            char* result = (char*)(offset + match_idx + YMM_SZ);
            if (!(*result ^ ch))
                return result;
            return NULL;

        }
    }    offset = (uintptr_t)str & -(AVX2_VEC_4_SZ);
    do
    {
        offset += AVX2_VEC_4_SZ;
        y5 = _mm256_load_si256((void*)offset);
        y6 = _mm256_load_si256((void*)offset + 1 * YMM_SZ);
        y7 = _mm256_load_si256((void*)offset + 2 * YMM_SZ);
        y8 = _mm256_load_si256((void*)offset + 3 * YMM_SZ);

        y1 = _mm256_xor_si256(y5, ych);
        y2 = _mm256_xor_si256(y6, ych);
        y3 = _mm256_xor_si256(y7, ych);
        y4 = _mm256_xor_si256(y8, ych);

        y1 = _mm256_min_epu8(y1, y5);
        y2 = _mm256_min_epu8(y2, y6);
        y3 = _mm256_min_epu8(y3, y7);
        y4 = _mm256_min_epu8(y4, y8);

        y5 = _mm256_min_epu8(y1, y2);
        y6 = _mm256_min_epu8(y3, y4);

        y6 = _mm256_min_epu8(y5, y6);

        match_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y6, y0));
    }while(match_mask == 0);

    match_mask1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y5, y0));
    if (match_mask1)
    {
        match_mask2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0));
        if (match_mask2)
        {
            offset += _tzcnt_u32(match_mask2);
        }
        else
        {
            offset += _tzcnt_u32(match_mask1) + YMM_SZ;
        }
        if (*((char*)offset) == (char)c)
            return (char*) offset;
    }
    else
    {
        match_mask2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y3, y0));
        if (match_mask2)
        {
            offset += _tzcnt_u32(match_mask2) + 2 * YMM_SZ;
        }
        else
        {
            offset += _tzcnt_u32(match_mask) + 3 * YMM_SZ;
        }
        if (*((char*)offset) == (char)c)
            return (char*) offset;
    }
    return NULL;
}
