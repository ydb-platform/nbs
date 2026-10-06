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

#include "almem_defs.h"
#include "logger.h"
#include "zen_cpu_info.h"
#include <immintrin.h>

/* This function is an optimized version of memchr using AVX-512 instructions.
It scans  the initial `size` bytes of the `mem` area for the first instance of `val`. */
static inline void *__attribute__((flatten)) _memchr_avx512(const void *mem, int val, size_t size)
{
    if (unlikely(size == 0))
        return NULL;

    __m512i z0, z1, z2, z3, z4;
    uint64_t match, match1, match2, match3;
    size_t index;

    // Broadcast the value to find to all elements of z0
    z0 = _mm512_set1_epi8((char) val);

    // Use the 'rax' register to hold the return pointer
    register void *ret asm("rax");
    ret = (void *) mem;

    // Handle the case where size is less than or equal to 64B
    if (likely(size <= ZMM_SZ))
    {
#ifndef ALMEM_STRICT_BOUNDS
        // Check if we may cross page boundary with one vector load
        uintptr_t addr = (uintptr_t) mem;
        uintptr_t page_offset = addr & (PAGE_SZ - 1);

        if (likely(page_offset <= (PAGE_SZ - ZMM_SZ)))
        {
            z1 = _mm512_loadu_si512(mem);
            match = _mm512_cmpeq_epu8_mask(z0, z1);
            index = _tzcnt_u64(match);
            if (index >= size)
                return NULL;
            return ret + index;
        } else
#endif
        {
            // masked load in case of page-crossing
            __mmask64 mask = _bzhi_u64((uint64_t) -1, (uint8_t) size);
            z2 = _mm512_maskz_loadu_epi8(mask, mem);
            match = _mm512_cmpeq_epu8_mask(z0, z2);
            match &= mask;

            if (!match)
                return NULL;

            index = _tzcnt_u64(match);
            return ret + index;
        }
    }

    // Handle the case where size lies between 65B-128B
    else if (size <= 2 * ZMM_SZ)
    {
        index = 0;
        // Load the first 64B and check for the match
        z1 = _mm512_loadu_si512(mem);
        match = _mm512_cmpeq_epu8_mask(z0, z1);

        // If no match is found, adjust the index and load the last 64B of the memory block
        if (!match)
        {
            index = size - ZMM_SZ;
            z1 = _mm512_loadu_si512(ret + index);
            match = _mm512_cmpeq_epu8_mask(z0, z1);

            // If no match is found, return NULL
            if (!match)
                return NULL;
        }
        // Find the index of the first match and return the pointer to the matching byte
        index += _tzcnt_u64(match);
        return ret + index;
    }

    // Handle the case where size lies between 129B-256B
    else if (size <= 4 * ZMM_SZ)
    {
        // Load the memory block in to four parts
        z1 = _mm512_loadu_si512(mem);
        z2 = _mm512_loadu_si512(mem + ZMM_SZ);
        z3 = _mm512_loadu_si512(mem + (size - 2 * ZMM_SZ));
        z4 = _mm512_loadu_si512(mem + (size - ZMM_SZ));

        // Perform comparisons across all four parts
        match = _mm512_cmpeq_epu8_mask(z0, z1);
        match1 = match | _mm512_cmpeq_epu8_mask(z0, z2);
        match2 = match1 | _mm512_cmpeq_epu8_mask(z0, z3);
        match3 = match2 | _mm512_cmpeq_epu8_mask(z0, z4);

        // If a match is found in any part, determine which part,
        // and return the pointer to the matching byte
        if (match3)
        {
            if (match)
            {
                index = _tzcnt_u64(match);
                return ret + index;
            }
            if (match1)
            {
                index = _tzcnt_u64(match1) + ZMM_SZ;
                return ret + index;
            }
            if (match2)
            {
                index = _tzcnt_u64(match2) + (size - 2 * ZMM_SZ);
                return ret + index;
            }
            index = _tzcnt_u64(match3) + (size - ZMM_SZ);
            return ret + index;
        }
        // If no match is found, return NULL
        return NULL;
    }

    // Load first 256B of memory into four ZMM registers
    z1 = _mm512_loadu_si512(mem);
    z2 = _mm512_loadu_si512(mem + ZMM_SZ);
    z3 = _mm512_loadu_si512(mem + 2 * ZMM_SZ);
    z4 = _mm512_loadu_si512(mem + 3 * ZMM_SZ);

    match = _mm512_cmpeq_epu8_mask(z0, z1);
    match1 = match | _mm512_cmpeq_epu8_mask(z0, z2);
    match2 = match1 | _mm512_cmpeq_epu8_mask(z0, z3);
    match3 = match2 | _mm512_cmpeq_epu8_mask(z0, z4);

    if (match3)
    {
        if (match)
        {
            index = _tzcnt_u64(match);
            return ret + index;
        }
        if (match1)
        {
            index = _tzcnt_u64(match1) + ZMM_SZ;
            return ret + index;
        }
        if (match2)
        {
            index = _tzcnt_u64(match2) + 2 * ZMM_SZ;
            return ret + index;
        }
        index = _tzcnt_u64(match3) + 3 * ZMM_SZ;
        return ret + index;
    }

    // For larger sizes(>256B), loop through the memory block in chunks of four ZMM registers
    if (size > 8 * ZMM_SZ)
    {
        size -= 4 * ZMM_SZ;
        size_t offset = 4 * ZMM_SZ - ((uintptr_t) mem & (ZMM_SZ - 1));

        // Loop through the memory block in chunks of four ZMM registers
        while (size >= offset)
        {
            z1 = _mm512_load_si512(mem + offset);
            z2 = _mm512_load_si512(mem + offset + ZMM_SZ);
            z3 = _mm512_load_si512(mem + offset + 2 * ZMM_SZ);
            z4 = _mm512_load_si512(mem + offset + 3 * ZMM_SZ);

            match = _mm512_cmpeq_epu8_mask(z0, z1);
            match1 = match | _mm512_cmpeq_epu8_mask(z0, z2);
            match2 = match1 | _mm512_cmpeq_epu8_mask(z0, z3);
            match3 = match2 | _mm512_cmpeq_epu8_mask(z0, z4);

            if (match3)
            {
                if (match)
                {
                    index = _tzcnt_u64(match) + offset;
                    return ret + index;
                }
                if (match1)
                {
                    index = _tzcnt_u64(match1) + offset + ZMM_SZ;
                    return ret + index;
                }
                if (match2)
                {
                    index = _tzcnt_u64(match2) + offset + 2 * ZMM_SZ;
                    return ret + index;
                }
                index = _tzcnt_u64(match3) + offset + 3 * ZMM_SZ;
                return ret + index;
            }
            offset += 4 * ZMM_SZ;
        }

        size += 4 * ZMM_SZ;
        if (size == offset)
            return NULL;
    }

    // Handle any remaining bytes that were not compared in the above loop
    z1 = _mm512_loadu_si512(mem + size - 4 * ZMM_SZ);
    z2 = _mm512_loadu_si512(mem + size - 3 * ZMM_SZ);
    z3 = _mm512_loadu_si512(mem + size - 2 * ZMM_SZ);
    z4 = _mm512_loadu_si512(mem + size - 1 * ZMM_SZ);

    match = _mm512_cmpeq_epu8_mask(z0, z1);
    match1 = match | _mm512_cmpeq_epu8_mask(z0, z2);
    match2 = match1 | _mm512_cmpeq_epu8_mask(z0, z3);
    match3 = match2 | _mm512_cmpeq_epu8_mask(z0, z4);

    if (match3)
    {
        if (match)
        {
            index = _tzcnt_u64(match) + size - 4 * ZMM_SZ;
            return ret + index;
        }
        if (match1)
        {
            index = _tzcnt_u64(match1) + size - 3 * ZMM_SZ;
            return ret + index;
        }
        if (match2)
        {
            index = _tzcnt_u64(match2) + size - 2 * ZMM_SZ;
            return ret + index;
        }
        index = _tzcnt_u64(match3) + size - ZMM_SZ;
        return ret + index;
    }
    return NULL;
}
