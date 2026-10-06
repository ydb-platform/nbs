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
#include <stdint.h>
#include <immintrin.h>
#include "almem_defs.h"
#include "zen_cpu_info.h"

#define GET_TAIL_MASK(match) (_bzhi_u64(UINT64_MAX, _tzcnt_u64(match) + 1))

HIDDEN_SYMBOL char * __attribute__((flatten)) __strcpy_zen4(char *dst, const char *src)
{
     size_t offset, index;
    __m512i z0, z1, z2, z3, z4, z5, z6;
    uint64_t match, match1, match2;
    __mmask64 mask;

    // Store the original destination pointer to return at the end
    register void *ret asm("rax");
    ret = dst;

    // Initialize a zeroed AVX-512 register for comparisons
    z0 = _mm512_setzero_epi32();

    // Handle cases where `src` is close to the end of a memory page
    if (unlikely((PAGE_SZ - ZMM_SZ) < ((PAGE_SZ - 1) & (uintptr_t)src)))
    {
        offset = (uintptr_t)src & (ZMM_SZ - 1);
        // Load bytes from `src` into z1 with a offsetted mask to avoid crossing the page boundary
        z6 = _mm512_set1_epi8(0xff);
        mask = (UINT64_MAX >> offset);
        z1 =  _mm512_mask_loadu_epi8(z6 ,mask, src);

        // Compare `z1` with zero register to find the null terminator
        match = _mm512_cmpeq_epu8_mask(z1, z0);

        // If a null terminator is found, calculate the index and store the result
        if (match)
        {
            mask = GET_TAIL_MASK(match);
            _mm512_mask_storeu_epi8(dst, mask, z1);
            return ret;
        }
    }

    // Load the first 64 bytes from `src` and check for null terminator
    z1 = _mm512_loadu_si512(src);
    match =_mm512_cmpeq_epu8_mask(z0, z1);
    if (match)
    {
        mask = GET_TAIL_MASK(match);
        _mm512_mask_storeu_epi8(dst, mask, z1);
        return ret;
    }
    // Store the first 64 bytes to `dst`
    _mm512_storeu_si512(dst, z1);

    // Adjust the offset for the next load operation
    offset = (uintptr_t)src & (ZMM_SZ - 1);
    offset = ZMM_SZ - offset;

    // Initialize a counter to process the next 192 bytes (3 * 64B) from the source
    uint8_t cnt_vec = 2;
    do
    {
        _mm_prefetch(src + offset + ZMM_SZ, _MM_HINT_NTA);
        z2 = _mm512_load_si512(src + offset);
        match = _mm512_cmpeq_epu8_mask(z2, z0);

        // If a null terminator is found within the loaded bytes, copy the tail vector.
        // Load and copy the remaining bytes up to and including the null terminator
        if (match)
        {
            index = offset + _tzcnt_u64(match) - ZMM_SZ + 1;
            z5 = _mm512_loadu_si512(src + index);
            _mm512_storeu_si512(dst + index, z5);
            return ret;
        }

        _mm512_storeu_si512(dst + offset, z2);
        offset += ZMM_SZ;
    } while (cnt_vec--);


    // Determine if the next four 64-byte blocks (256 bytes total) are within the same memory
    // page , this is to avoid crossing a page boundary, which could lead to a page fault if
    // the memory is not mapped. Here it calculates and processes the maximum number of 64-byte
    // vectors that can be handled without the risk of crossing into an adjacent memory page.
    cnt_vec = 4 - ((((uintptr_t)src + offset) & (4 * ZMM_SZ - 1)) >> 6);
    while (cnt_vec--)
    {
        z2 = _mm512_load_si512(src + offset);
        match = _mm512_cmpeq_epu8_mask(z2, z0);

        if (match)
        {
            mask = GET_TAIL_MASK(match);
            _mm512_mask_storeu_epi8(dst + offset, mask, z2);
            return ret;
        }

        _mm512_storeu_si512(dst + offset, z2);
        offset += ZMM_SZ;
    }

    // Main loop to process 4 vectors at a time for larger sizes(> 256B)
    while (1)
    {
        _mm_prefetch(src + offset + (4 * ZMM_SZ), _MM_HINT_NTA);
        // Load 4 vectors from `src`
        z1 = _mm512_load_si512(src + offset);
        z2 = _mm512_load_si512(src + offset + ZMM_SZ);
        z3 = _mm512_load_si512(src + offset + 2 * ZMM_SZ);
        z4 = _mm512_load_si512(src + offset + 3 * ZMM_SZ);

        // Find the minimum of the loaded vectors to check for null terminator
        z5 = _mm512_min_epu8(z1,z2);
        z6 = _mm512_min_epu8(z3,z4);

        // Compare the minimums to find the null terminator
        match = _mm512_cmpeq_epu8_mask(_mm512_min_epu8(z5, z6), z0);
        if (match)
          break; // If found, exit the loop

        // Store the 4 vectors to `dst`
        _mm512_storeu_si512(dst + offset, z1);
        _mm512_storeu_si512(dst + offset + ZMM_SZ, z2);
        _mm512_storeu_si512(dst + offset + 2 * ZMM_SZ, z3);
        _mm512_storeu_si512(dst + offset + 3 * ZMM_SZ, z4);
        offset += 4 * ZMM_SZ;
    }

    // Check for null terminator in the first two vectors (z1, z2)
    if ((match1 = _mm512_cmpeq_epu8_mask(z5, z0)))
    {
        if ((match2 = _mm512_cmpeq_epu8_mask(z1, z0)))
        {
            match = match2;
            goto copy_tail_vec;
        }
        _mm512_storeu_si512(dst + offset, z1);
        offset += ZMM_SZ;
        match = match1;
    }
    // Check for null terminator in the last two vectors (z3, z4)
    else
    {
        _mm512_storeu_si512(dst + offset, z1);
        _mm512_storeu_si512(dst + offset + ZMM_SZ, z2);
        if ((match2 =_mm512_cmpeq_epu8_mask(z3, z0)))
        {
            match = match2;
            offset += 2 * ZMM_SZ;
            goto copy_tail_vec;
        }
        _mm512_storeu_si512(dst + offset + 2 * ZMM_SZ, z3);
        offset += 3 * ZMM_SZ;
    }

    // Label for copying the tail of the string where the null terminator was found
    copy_tail_vec:
    {
        index = offset + _tzcnt_u64(match) - ZMM_SZ + 1;
        z5 = _mm512_loadu_si512(src + index);
        _mm512_storeu_si512(dst + index, z5);
        return ret;
    }
}

#ifndef ALMEM_DYN_DISPATCH
char *strcpy(char *, const char *) __attribute__((weak,
                        alias("__strcpy_zen4")));
#endif
