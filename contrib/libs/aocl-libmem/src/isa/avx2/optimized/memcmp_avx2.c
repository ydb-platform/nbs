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

#include "almem_defs.h"
#include "logger.h"
#include "zen_cpu_info.h"
#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>

static inline int memcmp_lt_ymm(const void *mem1, const void *mem2, uint8_t size)
{
    __m128i x0, x1, x2, x3;
    int ret, ret1;
    uint64_t res;
    uint32_t index;

    if (likely(size >= QWORD_SZ))
    {
        if (size >= XMM_SZ)
        {
            x0 = _mm_loadu_si128(mem1);
            x2 = _mm_loadu_si128(mem1 + size - XMM_SZ);
            x1 = _mm_loadu_si128(mem2);
            x3 = _mm_loadu_si128(mem2 + size - XMM_SZ);
            x0 = _mm_cmpeq_epi8(x0, x1);
            x2 = _mm_cmpeq_epi8(x2, x3);
            ret = _mm_movemask_epi8(_mm_and_si128(x0, x2));
            if (ret != (uint16_t) -1)
            {
                ret1 = _mm_movemask_epi8(x0);
                if (ret1 != (uint16_t) -1)
                    index = _tzcnt_u32(ret1 + 1);
                else
                    index = _tzcnt_u32(ret + 1) + size - XMM_SZ;
                return (*(uint8_t *) (mem1 + index) - *(uint8_t *) (mem2 + index));
            }
            return 0;
        }
        res = (*(uint64_t *) mem1) ^ (*(uint64_t *) mem2);
        if (res != 0)
        {
            index = _tzcnt_u64(res) >> 3;
            return *(uint8_t *) (mem1 + index) - *(uint8_t *) (mem2 + index);
        }
        index = size - QWORD_SZ;
        res = (*(uint64_t *) (mem1 + index)) ^ (*(uint64_t *) (mem2 + index));
        if (res != 0)
        {
            ret = _tzcnt_u64(res) >> 3;
            return *(uint8_t *) (mem1 + index + ret) - *(uint8_t *) (mem2 + index + ret);
        }
        return 0;
    }

    if (size >= DWORD_SZ)
    {
        ret = (*(uint32_t *) mem1) ^ (*(uint32_t *) mem2);
        if (ret)
        {
            index = _tzcnt_u32(ret) >> 3;
            return *(uint8_t *) (mem1 + index) - *(uint8_t *) (mem2 + index);
        }
        ret = (*(uint32_t *) (mem1 + size - DWORD_SZ)) ^ (*(uint32_t *) (mem2 + size - DWORD_SZ));
        if (ret)
        {
            index = _tzcnt_u32(ret) >> 3;
            return *(uint8_t *) (mem1 + size - DWORD_SZ + index) - *(uint8_t *) (mem2 + size - DWORD_SZ + index);
        }
        return 0;
    }

    if (size >= WORD_SZ)
    {
        ret = (*(uint16_t *) mem1) ^ (*(uint16_t *) mem2);
        if (ret)
        {
            index = _tzcnt_u32(ret) >> 3;
            return *(uint8_t *) (mem1 + index) - *(uint8_t *) (mem2 + index);
        }
        if (size == 3)
            return *(uint8_t *) (mem1 + 2) - *(uint8_t *) (mem2 + 2);
        return 0;
    }

    if (size == 1)
        return *((uint8_t *) mem1) - *((uint8_t *) mem2);
    return 0;
}

static inline int __attribute__((flatten)) _memcmp_avx2(const void *mem1, const void *mem2, size_t size)
{
    __m256i y0, y1, y2, y3, y4, y5, y6, y7;
    size_t offset = 0;
    int ret, ret1;

    if (likely(size < YMM_SZ))
        return memcmp_lt_ymm(mem1, mem2, (uint8_t) size);

    if (likely(size <= 2 * YMM_SZ))
    {
        y0 = _mm256_loadu_si256(mem1);
        y2 = _mm256_loadu_si256(mem1 + size - YMM_SZ);
        y1 = _mm256_loadu_si256(mem2);
        y3 = _mm256_loadu_si256(mem2 + size - YMM_SZ);
        y0 = _mm256_cmpeq_epi8(y0, y1);
        y2 = _mm256_cmpeq_epi8(y2, y3);
        ret = _mm256_movemask_epi8(_mm256_and_si256(y0, y2));
        if (ret != (int32_t) -1)
        {
            ret1 = _mm256_movemask_epi8(y0);
            if (ret1 != (int32_t) -1)
                offset = _tzcnt_u32(ret1 + 1);
            else
                offset = _tzcnt_u32(ret + 1) + size - YMM_SZ;

            return ((*(uint8_t *) (mem1 + offset)) - (*(uint8_t *) (mem2 + offset)));
        }
        return 0;
    }

    y0 = _mm256_loadu_si256(mem2 + 0 * YMM_SZ);
    y1 = _mm256_loadu_si256(mem2 + 1 * YMM_SZ);
    y4 = _mm256_loadu_si256(mem1 + 0 * YMM_SZ);
    y5 = _mm256_loadu_si256(mem1 + 1 * YMM_SZ);
    y0 = _mm256_cmpeq_epi8(y0, y4);
    y1 = _mm256_cmpeq_epi8(y1, y5);
    ret = _mm256_movemask_epi8(_mm256_and_si256(y0, y1));
    if (ret != (int32_t) -1)
    {
        ret1 = _mm256_movemask_epi8(y0);
        if (ret1 != (int32_t) -1)
            offset = _tzcnt_u32(ret1 + 1);
        else
            offset = _tzcnt_u32(ret + 1) + YMM_SZ;
        return ((*(uint8_t *) (mem1 + offset)) - (*(uint8_t *) (mem2 + offset)));
    }

    if (size <= 4 * YMM_SZ)
    {
        y2 = _mm256_loadu_si256(mem2 + size - 2 * YMM_SZ);
        y3 = _mm256_loadu_si256(mem2 + size - 1 * YMM_SZ);
        y6 = _mm256_loadu_si256(mem1 + size - 2 * YMM_SZ);
        y7 = _mm256_loadu_si256(mem1 + size - 1 * YMM_SZ);
        y2 = _mm256_cmpeq_epi8(y2, y6);
        y3 = _mm256_cmpeq_epi8(y3, y7);
        ret = _mm256_movemask_epi8(_mm256_and_si256(y2, y3));
        if (ret != (int32_t) -1)
        {
            ret1 = _mm256_movemask_epi8(y2);
            if (ret1 != (int32_t) -1)
            {
                offset = _tzcnt_u32(ret1 + 1) + size - 2 * YMM_SZ;
            } else
                offset = _tzcnt_u32(ret + 1) + size - YMM_SZ;

            return ((*(uint8_t *) (mem1 + offset)) - (*(uint8_t *) (mem2 + offset)));
        }
        return 0;
    }

    y2 = _mm256_loadu_si256(mem2 + 2 * YMM_SZ);
    y3 = _mm256_loadu_si256(mem2 + 3 * YMM_SZ);
    y6 = _mm256_loadu_si256(mem1 + 2 * YMM_SZ);
    y7 = _mm256_loadu_si256(mem1 + 3 * YMM_SZ);
    y2 = _mm256_cmpeq_epi8(y2, y6);
    y3 = _mm256_cmpeq_epi8(y3, y7);
    ret = _mm256_movemask_epi8(_mm256_and_si256(y2, y3));
    if (ret != (int32_t) -1)
    {
        ret1 = _mm256_movemask_epi8(y2);
        if (ret1 != (int32_t) -1)
        {
            offset = _tzcnt_u32(ret1 + 1) + 2 * YMM_SZ;
        } else
            offset = _tzcnt_u32(ret + 1) + 3 * YMM_SZ;

        return ((*(uint8_t *) (mem1 + offset)) - (*(uint8_t *) (mem2 + offset)));
    }

    if (size > 8 * YMM_SZ)
    {
        offset = 4 * YMM_SZ;
        while ((size & (-4 * YMM_SZ)) > offset)
        {
            y0 = _mm256_loadu_si256(mem2 + offset + 0 * YMM_SZ);
            y1 = _mm256_loadu_si256(mem2 + offset + 1 * YMM_SZ);
            y4 = _mm256_loadu_si256(mem1 + offset + 0 * YMM_SZ);
            y5 = _mm256_loadu_si256(mem1 + offset + 1 * YMM_SZ);
            y0 = _mm256_cmpeq_epi8(y0, y4);
            y1 = _mm256_cmpeq_epi8(y1, y5);
            ret = _mm256_movemask_epi8(_mm256_and_si256(y0, y1));
            if (ret != (int32_t) -1)
            {
                ret1 = _mm256_movemask_epi8(y0);
                if (ret1 != (int32_t) -1)
                    offset += _tzcnt_u32(ret1 + 1);
                else
                    offset += _tzcnt_u32(ret + 1) + YMM_SZ;
                return ((*(uint8_t *) (mem1 + offset)) - (*(uint8_t *) (mem2 + offset)));
            }
            y2 = _mm256_loadu_si256(mem2 + offset + 2 * YMM_SZ);
            y3 = _mm256_loadu_si256(mem2 + offset + 3 * YMM_SZ);
            y6 = _mm256_loadu_si256(mem1 + offset + 2 * YMM_SZ);
            y7 = _mm256_loadu_si256(mem1 + offset + 3 * YMM_SZ);
            y2 = _mm256_cmpeq_epi8(y2, y6);
            y3 = _mm256_cmpeq_epi8(y3, y7);
            ret = _mm256_movemask_epi8(_mm256_and_si256(y2, y3));
            if (ret != (int32_t) -1)
            {
                ret1 = _mm256_movemask_epi8(y2);
                if (ret1 != (int32_t) -1)
                {
                    offset += _tzcnt_u32(ret1 + 1) + 2 * YMM_SZ;
                } else
                    offset += _tzcnt_u32(ret + 1) + 3 * YMM_SZ;

                return ((*(uint8_t *) (mem1 + offset)) - (*(uint8_t *) (mem2 + offset)));
            }
            offset += 4 * YMM_SZ;
        }
        if (offset == size)
            return 0;
    }
    // Compare the tail 4 VECs sized memory
    y0 = _mm256_loadu_si256(mem2 + size - 4 * YMM_SZ);
    y1 = _mm256_loadu_si256(mem2 + size - 3 * YMM_SZ);
    y4 = _mm256_loadu_si256(mem1 + size - 4 * YMM_SZ);
    y5 = _mm256_loadu_si256(mem1 + size - 3 * YMM_SZ);
    y0 = _mm256_cmpeq_epi8(y0, y4);
    ret = _mm256_movemask_epi8(y0) + 1;
    if (ret != 0)
    {
        ret = _tzcnt_u32(ret) + size - 4 * YMM_SZ;
        return ((*(uint8_t *) (mem1 + ret)) - (*(uint8_t *) (mem2 + ret)));
    }
    y1 = _mm256_cmpeq_epi8(y1, y5);
    ret = _mm256_movemask_epi8(y1) + 1;
    if (ret != 0)
    {
        ret = _tzcnt_u32(ret) + size - 3 * YMM_SZ;
        return ((*(uint8_t *) (mem1 + ret)) - (*(uint8_t *) (mem2 + ret)));
    }
    y2 = _mm256_loadu_si256(mem2 + size - 2 * YMM_SZ);
    y3 = _mm256_loadu_si256(mem2 + size - 1 * YMM_SZ);
    y6 = _mm256_loadu_si256(mem1 + size - 2 * YMM_SZ);
    y7 = _mm256_loadu_si256(mem1 + size - 1 * YMM_SZ);
    y2 = _mm256_cmpeq_epi8(y2, y6);
    ret = _mm256_movemask_epi8(y2) + 1;
    if (ret != 0)
    {
        ret = _tzcnt_u32(ret) + size - 2 * YMM_SZ;
        return ((*(uint8_t *) (mem1 + ret)) - (*(uint8_t *) (mem2 + ret)));
    }
    y3 = _mm256_cmpeq_epi8(y3, y7);
    ret = _mm256_movemask_epi8(y3) + 1;
    if (ret != 0)
    {
        ret = _tzcnt_u32(ret) + size - 1 * YMM_SZ;
        return ((*(uint8_t *) (mem1 + ret)) - (*(uint8_t *) (mem2 + ret)));
    }
    return 0;
}
