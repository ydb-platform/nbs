/* Copyright (C) 2024-25 Advanced Micro Devices, Inc. All rights reserved.
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
#include <stdint.h>
#include "zen_cpu_info.h"
#include "almem_defs.h"

HIDDEN_SYMBOL int __attribute__((flatten)) __memcmp_zen4(const void *mem1, const void *mem2, size_t size)
{
    LOG_INFO("\n");
    __m512i z0, z1, z2, z3, z4, z5, z6, z7;
    __m256i y0, y1, y2, y3, y4, y5, y6, y7;
    size_t offset = 0, index = 0;
    int64_t mask;
    int ret, ret1;

    if (likely(size <= ZMM_SZ))
    {
        __mmask64 mask;
        mask = _bzhi_u64((uint64_t)-1, (uint8_t)size);
        z0 = _mm512_setzero_epi32();
        z1 =  _mm512_mask_loadu_epi8(z0 ,mask, mem1);
        z2 =  _mm512_mask_loadu_epi8(z0 ,mask, mem2);

        mask = _mm512_cmpneq_epu8_mask(z1, z2);
        if (mask != 0)
        {
            index = _tzcnt_u64(mask);
            return ((*(uint8_t*)(mem1 + index)) - (*(uint8_t*)(mem2 + index)));
        }
        return 0;
    }
    else if (likely(size <= 2 * ZMM_SZ))
    {
        z0 = _mm512_loadu_si512(mem2 + 0 * ZMM_SZ);
        z2 = _mm512_loadu_si512(mem1 + 0 * ZMM_SZ);

        mask = _mm512_cmpneq_epu8_mask(z0, z2);
        if (mask == 0)
        {
            index = size - ZMM_SZ;
            z1 = _mm512_loadu_si512(mem2 + index);
            z3 = _mm512_loadu_si512(mem1 + index);
            mask = _mm512_cmpneq_epu8_mask(z1, z3);
            if (mask == 0)
                return 0;
        }
        index += _tzcnt_u64(mask);
        return ((*(uint8_t*)(mem1 + index)) - (*(uint8_t*)(mem2 + index)));
    }

    else if (likely(size <= 4 * ZMM_SZ))
    {
        z0 = _mm512_loadu_si512(mem2 + 0 * ZMM_SZ);
        z1 = _mm512_loadu_si512(mem2 + 1 * ZMM_SZ);
        z4 = _mm512_loadu_si512(mem1 + 0 * ZMM_SZ);
        z5 = _mm512_loadu_si512(mem1 + 1 * ZMM_SZ);
        mask = _mm512_cmpneq_epu8_mask(z0, z4);
        if (mask != 0)
        {
            index = _tzcnt_u64(mask);
            return ((*(uint8_t*)(mem1 + index)) - (*(uint8_t*)(mem2 + index)));
        }
        mask = _mm512_cmpneq_epu8_mask(z1, z5);
        if (mask != 0)
        {
            index = _tzcnt_u64(mask) + ZMM_SZ;
            return ((*(uint8_t*)(mem1 + index)) - (*(uint8_t*)(mem2 + index)));
        }
        z2 = _mm512_loadu_si512(mem2 + size - 2 * ZMM_SZ);
        z3 = _mm512_loadu_si512(mem2 + size - 1 * ZMM_SZ);
        z6 = _mm512_loadu_si512(mem1 + size - 2 * ZMM_SZ);
        z7 = _mm512_loadu_si512(mem1 + size - 1 * ZMM_SZ);
        mask = _mm512_cmpneq_epu8_mask(z2, z6);
        if (mask != 0)
        {
            index = _tzcnt_u64(mask) + size - 2 * ZMM_SZ;
            return ((*(uint8_t*)(mem1 + index)) - (*(uint8_t*)(mem2 + index)));
        }
        mask = _mm512_cmpneq_epu8_mask(z3, z7);
        if (mask != 0)
        {
            index = _tzcnt_u64(mask) + size - ZMM_SZ;
            return ((*(uint8_t*)(mem1 + index)) - (*(uint8_t*)(mem2 + index)));
        }
        return 0;
    }

    while ((size - (4 * YMM_SZ)) > offset)
    {

        y0 = _mm256_loadu_si256(mem2 + offset + 0 * YMM_SZ);
        y1 = _mm256_loadu_si256(mem2 + offset + 1 * YMM_SZ);
        y4 = _mm256_loadu_si256(mem1 + offset + 0 * YMM_SZ);
        y5 = _mm256_loadu_si256(mem1 + offset + 1 * YMM_SZ);
        y0 = _mm256_cmpeq_epi8(y0, y4);
        y1 = _mm256_cmpeq_epi8(y1, y5);
        ret = _mm256_movemask_epi8(_mm256_and_si256(y0, y1));
        if (ret != (int32_t)-1)
        {
            ret1 =  _mm256_movemask_epi8(y0);
            if (ret1 != (int32_t)-1)
                offset += _tzcnt_u32(ret1 + 1);
            else
                offset += _tzcnt_u32(ret + 1) + YMM_SZ;
            return ((*(uint8_t*)(mem1 + offset)) - (*(uint8_t*)(mem2 + offset)));
        }
        y2 = _mm256_loadu_si256(mem2 + offset + 2 * YMM_SZ);
        y3 = _mm256_loadu_si256(mem2 + offset + 3 * YMM_SZ);
        y6 = _mm256_loadu_si256(mem1 + offset + 2 * YMM_SZ);
        y7 = _mm256_loadu_si256(mem1 + offset + 3 * YMM_SZ);
        y2 = _mm256_cmpeq_epi8(y2, y6);
        y3 = _mm256_cmpeq_epi8(y3, y7);
        ret = _mm256_movemask_epi8(_mm256_and_si256(y2, y3));
        if (ret != (int32_t)-1)
        {
            ret1 =  _mm256_movemask_epi8(y2);
            if (ret1 != (int32_t)-1)
            {
                offset += _tzcnt_u32(ret1 + 1) + 2 * YMM_SZ;
            }
            else
                offset += _tzcnt_u32(ret + 1) + 3 * YMM_SZ;

            return ((*(uint8_t*)(mem1 + offset)) - (*(uint8_t*)(mem2 + offset)));
        }
        offset += 4 * YMM_SZ;
    }
    if (offset == size)
        return 0;

    // remaining data to be compared
    uint16_t rem_data = size - offset;

    // data vector blocks to be compared
    uint8_t rem_vecs = ((rem_data) >> 5) + !!(rem_data & (0x1F));

    switch (rem_vecs)
    {
        case 4:
            y0 = _mm256_loadu_si256(mem2 + size - 4 * YMM_SZ);
            y4 = _mm256_loadu_si256(mem1 + size - 4 * YMM_SZ);
            y0 = _mm256_cmpeq_epi8(y0, y4);
            ret = _mm256_movemask_epi8(y0) + 1;
            if (ret != 0)
            {
                ret = _tzcnt_u32(ret) + size - 4 * YMM_SZ;
                return ((*(uint8_t*)(mem1 + ret)) - (*(uint8_t*)(mem2 + ret)));
            }
            __attribute__ ((fallthrough));
        case 3:
            y1 = _mm256_loadu_si256(mem2 + size - 3 * YMM_SZ);
            y5 = _mm256_loadu_si256(mem1 + size - 3 * YMM_SZ);
            y1 = _mm256_cmpeq_epi8(y1, y5);
            ret = _mm256_movemask_epi8(y1) + 1;
            if (ret != 0)
            {
                ret = _tzcnt_u32(ret) + size - 3 * YMM_SZ;
                return ((*(uint8_t*)(mem1 + ret)) - (*(uint8_t*)(mem2 + ret)));
            }
            __attribute__ ((fallthrough));
        case 2:
            y2 = _mm256_loadu_si256(mem2 + size - 2 * YMM_SZ);
            y6 = _mm256_loadu_si256(mem1 + size - 2 * YMM_SZ);
            y2 = _mm256_cmpeq_epi8(y2, y6);
            ret = _mm256_movemask_epi8(y2) + 1;
            if (ret != 0)
            {
                ret = _tzcnt_u32(ret) + size - 2 * YMM_SZ;
                return ((*(uint8_t*)(mem1 + ret)) - (*(uint8_t*)(mem2 + ret)));
            }
            __attribute__ ((fallthrough));
        case 1:
            y3 = _mm256_loadu_si256(mem2 + size - 1 * YMM_SZ);
            y7 = _mm256_loadu_si256(mem1 + size - 1 * YMM_SZ);
            y3 = _mm256_cmpeq_epi8(y3, y7);
            ret = _mm256_movemask_epi8(y3) + 1;
            if (ret != 0)
            {
                ret = _tzcnt_u32(ret) + size - 1 * YMM_SZ;
                return ((*(uint8_t*)(mem1 + ret)) - (*(uint8_t*)(mem2 + ret)));
            }
    }
    return 0;
}

#ifndef ALMEM_DYN_DISPATCH
int memcmp(const void *, const void *, size_t) __attribute__((weak,
                        alias("__memcmp_zen4")));
#endif

