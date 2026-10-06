/* Copyright (C) 2024-26 Advanced Micro Devices, Inc. All rights reserved.
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
#include "almem_defs.h"
#include "logger.h"
#include "zen_cpu_info.h"

extern cpu_info zen_info;

static inline void *__attribute__((flatten)) _memchr_avx2(const void *mem, int val, size_t size)
{
    __m256i y0, y1, y2, y3, y4, y5, y6, y7, y8;
    size_t offset;
    int ret, match1, match2, match3, match4;
    uint64_t match_tail;
    void *ret_ptr = (void *) mem;

    if (unlikely(size == 0))
        return NULL;

    y0 = _mm256_set1_epi8((char) val);

#ifdef ALMEM_STRICT_BOUNDS
    if (likely(size <= YMM_SZ))
    {
        __m128i x0, x1, x2;
        int m1, m2;

        x0 = _mm_set1_epi8((char) val);

        if (size >= XMM_SZ)
        {
            x1 = _mm_loadu_si128((const __m128i *) mem);
            m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(x0, x1));
            if (m1)
                return ret_ptr + _tzcnt_u32(m1);

            x2 = _mm_loadu_si128((const __m128i *) (mem + size - XMM_SZ));
            m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(x0, x2));
            if (m2)
                return ret_ptr + (size - XMM_SZ) + _tzcnt_u32(m2);
            return NULL;
        }
        if (size >= QWORD_SZ)
        {
            x1 = _mm_cvtsi64_si128(*((uint64_t *) mem));
            m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(x0, x1)) & 0xFF;
            if (m1)
                return ret_ptr + _tzcnt_u32(m1);

            x2 = _mm_cvtsi64_si128(*((uint64_t *) (mem + size - QWORD_SZ)));
            m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(x0, x2)) & 0xFF;
            if (m2)
                return ret_ptr + (size - QWORD_SZ) + _tzcnt_u32(m2);
            return NULL;
        }
        if (size >= DWORD_SZ)
        {
            x1 = _mm_cvtsi32_si128(*((uint32_t *) mem));
            m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(x0, x1)) & 0xF;
            if (m1)
                return ret_ptr + _tzcnt_u32(m1);

            x2 = _mm_cvtsi32_si128(*((uint32_t *) (mem + size - DWORD_SZ)));
            m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(x0, x2)) & 0xF;
            if (m2)
                return ret_ptr + (size - DWORD_SZ) + _tzcnt_u32(m2);
            return NULL;
        }

        uint8_t cmp = (*(const uint8_t *) mem) ^ (uint8_t) val;
        if (cmp == 0)
            return ret_ptr;
        if (size > 1)
        {
            cmp = (*(const uint8_t *) (mem + 1)) ^ (uint8_t) val;
            if (cmp == 0)
                return ret_ptr + 1;
            if (size > 2)
            {
                cmp = (*(const uint8_t *) (mem + 2)) ^ (uint8_t) val;
                if (cmp == 0)
                    return ret_ptr + 2;
            }
        }
        return NULL;
    }
    y1 = _mm256_loadu_si256(mem);
    y1 = _mm256_cmpeq_epi8(y0, y1);
    ret = _mm256_movemask_epi8(y1);
    if (ret)
        return ret_ptr + _tzcnt_u32(ret);
#else
    uint32_t page_offset = (uintptr_t) mem & (PAGE_SZ - 1);

    if (likely(page_offset <= (PAGE_SZ - YMM_SZ)))
    {
        y1 = _mm256_loadu_si256(mem);
        y1 = _mm256_cmpeq_epi8(y0, y1);
        ret = _mm256_movemask_epi8(y1);

        if (likely(size <= YMM_SZ))
        {
            ret = _bzhi_u32(ret, (uint32_t) size);
            if (ret)
                return ret_ptr + _tzcnt_u32(ret);
            return NULL;
        }

        if (ret)
            return ret_ptr + _tzcnt_u32(ret);
    }
    else
    {
        uintptr_t aligned_addr = (uintptr_t) mem & ~(uintptr_t) (YMM_SZ - 1);
        uint32_t leading = (uintptr_t) mem & (YMM_SZ - 1);
        uint32_t trailing = YMM_SZ - leading;

        y1 = _mm256_load_si256((const __m256i *) aligned_addr);
        y1 = _mm256_cmpeq_epi8(y0, y1);
        uint32_t mask = (uint32_t) _mm256_movemask_epi8(y1) >> leading;

        if (size <= trailing)
        {
            mask = _bzhi_u32(mask, (uint32_t) size);
            if (mask)
                return ret_ptr + _tzcnt_u32(mask);
            return NULL;
        }

        if (mask)
            return ret_ptr + _tzcnt_u32(mask);

        y1 = _mm256_load_si256((const __m256i *) (aligned_addr + YMM_SZ));
        y1 = _mm256_cmpeq_epi8(y0, y1);
        ret = _mm256_movemask_epi8(y1);

        if (size <= trailing + YMM_SZ)
        {
            ret = _bzhi_u32(ret, (uint32_t) (size - trailing));
            if (ret)
                return ret_ptr + trailing + _tzcnt_u32(ret);
            return NULL;
        }

        if (ret)
            return ret_ptr + trailing + _tzcnt_u32(ret);
    }
#endif

    if (likely(size <= 2 * YMM_SZ))
    {
        y1 = _mm256_loadu_si256(mem + size - YMM_SZ);
        y1 = _mm256_cmpeq_epi8(y0, y1);
        ret = _mm256_movemask_epi8(y1);
        if (ret)
            return ret_ptr + size - YMM_SZ + _tzcnt_u32(ret);
        return NULL;
    }
    if (size <= 4 * YMM_SZ)
    {
        y1 = _mm256_loadu_si256(mem);
        y2 = _mm256_loadu_si256(mem + YMM_SZ);
        y5 = _mm256_loadu_si256(mem + (size - 2 * YMM_SZ));
        y6 = _mm256_loadu_si256(mem + (size - YMM_SZ));

        y3 = _mm256_cmpeq_epi8(y0, y1);
        y4 = _mm256_cmpeq_epi8(y0, y2);
        y7 = _mm256_cmpeq_epi8(y0, y5);
        y8 = _mm256_cmpeq_epi8(y0, y6);

        y1 = _mm256_or_si256(y3, y4);
        y5 = _mm256_or_si256(y7, y8);
        y2 = _mm256_or_si256(y1, y5);

        ret = _mm256_movemask_epi8(y2);
        if (ret)
        {
            match1 = _mm256_movemask_epi8(y3);
            if (match1)
                return ret_ptr + _tzcnt_u32(match1);

            match2 = _mm256_movemask_epi8(y4);
            if (match2)
                return ret_ptr + YMM_SZ + _tzcnt_u32(match2);

            match3 = _mm256_movemask_epi8(y7);
            match4 = _mm256_movemask_epi8(y8);
            match_tail = ((uint64_t) (uint32_t) match4 << 32) | (uint64_t) (uint32_t) match3;
            return ret_ptr + (size - 2 * YMM_SZ) + _tzcnt_u64(match_tail);
        }
        return NULL;
    }

    y5 = _mm256_cmpeq_epi8(y0, _mm256_loadu_si256(mem));
    y6 = _mm256_cmpeq_epi8(y0, _mm256_loadu_si256(mem + YMM_SZ));
    y7 = _mm256_cmpeq_epi8(y0, _mm256_loadu_si256(mem + 2 * YMM_SZ));
    y8 = _mm256_cmpeq_epi8(y0, _mm256_loadu_si256(mem + 3 * YMM_SZ));

    y1 = _mm256_or_si256(y5, y6);
    y2 = _mm256_or_si256(y7, y8);
    y3 = _mm256_or_si256(y1, y2);

    ret = _mm256_movemask_epi8(y3);
    if (ret)
    {
        match1 = _mm256_movemask_epi8(y5);
        if (match1)
            return ret_ptr + _tzcnt_u32(match1);

        match2 = _mm256_movemask_epi8(y6);
        if (match2)
            return ret_ptr + YMM_SZ + _tzcnt_u32(match2);

        match3 = _mm256_movemask_epi8(y7);
        match4 = _mm256_movemask_epi8(y8);
        match_tail = ((uint64_t) (uint32_t) match4 << 32) | (uint64_t) (uint32_t) match3;
        return ret_ptr + 2 * YMM_SZ + _tzcnt_u64(match_tail);
    }

    if (size > 8 * YMM_SZ)
    {
        size -= 4 * YMM_SZ;
        offset = 4 * YMM_SZ - ((uintptr_t) mem & (YMM_SZ - 1));

        while (size >= offset)
        {
            y5 = _mm256_cmpeq_epi8(y0, _mm256_load_si256(mem + offset));
            y6 = _mm256_cmpeq_epi8(y0, _mm256_load_si256(mem + offset + YMM_SZ));
            y7 = _mm256_cmpeq_epi8(y0, _mm256_load_si256(mem + offset + 2 * YMM_SZ));
            y8 = _mm256_cmpeq_epi8(y0, _mm256_load_si256(mem + offset + 3 * YMM_SZ));

            y1 = _mm256_or_si256(y5, y6);
            y2 = _mm256_or_si256(y7, y8);
            y3 = _mm256_or_si256(y1, y2);

            ret = _mm256_movemask_epi8(y3);
            if (unlikely(ret))
            {
                match1 = _mm256_movemask_epi8(y5);
                if (match1)
                    return ret_ptr + offset + _tzcnt_u32(match1);

                match2 = _mm256_movemask_epi8(y6);
                if (match2)
                    return ret_ptr + offset + YMM_SZ + _tzcnt_u32(match2);

                match3 = _mm256_movemask_epi8(y7);
                match4 = _mm256_movemask_epi8(y8);
                match_tail = ((uint64_t) (uint32_t) match4 << 32) | (uint64_t) (uint32_t) match3;
                return ret_ptr + offset + 2 * YMM_SZ + _tzcnt_u64(match_tail);
            }
            offset += 4 * YMM_SZ;
        }

        size += 4 * YMM_SZ;
        if (size == offset)
            return NULL;
    }

    y1 = _mm256_loadu_si256(mem + size - 4 * YMM_SZ);
    y2 = _mm256_loadu_si256(mem + size - 3 * YMM_SZ);
    y3 = _mm256_loadu_si256(mem + size - 2 * YMM_SZ);
    y4 = _mm256_loadu_si256(mem + size - 1 * YMM_SZ);

    y5 = _mm256_cmpeq_epi8(y0, y1);
    y6 = _mm256_cmpeq_epi8(y0, y2);
    y7 = _mm256_cmpeq_epi8(y0, y3);
    y8 = _mm256_cmpeq_epi8(y0, y4);

    y1 = _mm256_or_si256(y5, y6);
    y2 = _mm256_or_si256(y7, y8);
    y3 = _mm256_or_si256(y1, y2);

    ret = _mm256_movemask_epi8(y3);
    if (ret)
    {
        match1 = _mm256_movemask_epi8(y5);
        if (match1)
            return ret_ptr + (size - 4 * YMM_SZ) + _tzcnt_u32(match1);

        match2 = _mm256_movemask_epi8(y6);
        if (match2)
            return ret_ptr + (size - 3 * YMM_SZ) + _tzcnt_u32(match2);

        match3 = _mm256_movemask_epi8(y7);
        match4 = _mm256_movemask_epi8(y8);
        match_tail = ((uint64_t) (uint32_t) match4 << 32) | (uint64_t) (uint32_t) match3;
        return ret_ptr + (size - 2 * YMM_SZ) + _tzcnt_u64(match_tail);
    }
    return NULL;
}
