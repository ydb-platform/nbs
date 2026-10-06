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
#include <zen_cpu_info.h>

HIDDEN_SYMBOL size_t __attribute__((flatten)) __strlen_zen4(const char *str)
{
    size_t offset;
    __m512i z0, z1, z2, z3, z4, z5, z6;
    uint64_t ret, ret1, ret2;

    // Initialize a zeroed AVX-512 register for comparisons
    z0 = _mm512_setzero_epi32();

    // Calculate the offset to align the source pointer to 64 bytes
    offset = (uintptr_t)str & (ZMM_SZ - 1);

    // Handle cases where start of string is close to the end of a memory page
    if (unlikely(((PAGE_SZ - ZMM_SZ) < ((PAGE_SZ -1) & (uintptr_t)str))))
    {
        z6 = _mm512_set1_epi8(0xff);
        //Mask to load only the valid bytes from the string without crossing the page boundary
        __mmask64 mask = _bzhi_u64(UINT64_MAX, ZMM_SZ - offset);
        z1 = _mm512_mask_loadu_epi8(z6, mask, str);
        ret = _mm512_cmpeq_epu8_mask(z1, z0);
        if (ret)
            return _tzcnt_u64(ret);
    }
    // Load first 64 bytes from `str` into z1 and check for null terminator
    else
    {
        z1 = _mm512_loadu_si512(str);
        ret = _mm512_cmpeq_epu8_mask(z1, z0);
        if (ret)
            return _tzcnt_u64(ret);
    }

    // Adjust the offset to align with cache line for the next load operation
    offset = ZMM_SZ - offset;

    // Process the next 7 vectors (7 * 64B) with offset adjustments
    uint8_t cnt_vec = 7;
    while(cnt_vec--)
    {
        z2 = _mm512_load_si512(str + offset);
        ret = _mm512_cmpeq_epu8_mask(z2, z0);

        if (ret)
            return _tzcnt_u64(ret) + offset;

        offset += ZMM_SZ;
    }

    // Ensure the next 4 * 64B vector loads in the main loop
    // do not cross a page boundary to prevent potential page faults.
    cnt_vec = 4 - ((((uintptr_t)str + offset) & (4 * ZMM_SZ - 1)) >> 6);
    while (cnt_vec--)
    {
        z1 = _mm512_load_si512(str + offset);
        ret = _mm512_cmpeq_epu8_mask(z1, z0);
        if (ret)
            return _tzcnt_u64(ret) + offset;
        offset += ZMM_SZ;
    }

    // Main loop to process 4 vectors at a time for larger sizes(> 512B)
    offset -= AVX512_VEC_4_SZ;
    do
    {
        offset += AVX512_VEC_4_SZ;
        // Load 4 vectors from `str`
        z1 = _mm512_load_si512(str + offset);
        z2 = _mm512_load_si512(str + offset + ZMM_SZ);
        z3 = _mm512_load_si512(str + offset + 2 * ZMM_SZ);
        z4 = _mm512_load_si512(str + offset + 3 * ZMM_SZ);

        // Find the minimum of the loaded vectors to check for null terminator
        z5 = _mm512_min_epu8(z1,z2);
        z6 = _mm512_min_epu8(z3,z4);

        // Compare the minimums to find the null terminator
        ret = _mm512_cmpeq_epu8_mask(_mm512_min_epu8(z5, z6), z0);
    } while (!ret);

    // Check for null terminator in the first two vectors (z1, z2)
    if ((ret1 = _mm512_cmpeq_epu8_mask(z5, z0)))
    {
        if ((ret2 = _mm512_cmpeq_epu8_mask(z1, z0)))
        {
            return (_tzcnt_u64(ret2) + offset);
        }
        return (_tzcnt_u64(ret1) + ZMM_SZ + offset);
    }
    // Check for null terminator in the last two vectors (z3, z4)
    else
    {
        if ((ret2 =_mm512_cmpeq_epu8_mask(z3, z0)))
        {
            return (_tzcnt_u64(ret2) + 2 * ZMM_SZ + offset);
        }
        return  (_tzcnt_u64(ret) + 3 * ZMM_SZ + offset);
    }
}
#ifndef ALMEM_DYN_DISPATCH
size_t strlen(const char *) __attribute__((weak,
                        alias("__strlen_zen4")));
#endif
