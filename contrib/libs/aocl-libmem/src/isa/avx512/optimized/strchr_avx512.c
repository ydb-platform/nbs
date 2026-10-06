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
#include "almem_defs.h"
#include <zen_cpu_info.h>

/* This function is an optimized version of strchr using AVX-512 instructions.
It finds and returns a pointer to the first occurrence of the character in the string. */


static inline char* __attribute__((flatten)) _strchr_avx512(const char *str, int c)
{
    size_t  offset,  ret;
    __m512i z0, z1, z2, z3, z4, z7, zch, zxor, zmin;
    __mmask64  match_mask, match_mask1, match_mask2, match_mask3, match_mask4, mask;
    char* result;
    char ch = (char)c;

    // Initialize a zeroed AVX-512 register for comparisons
    z0 = _mm512_setzero_epi32();

    // Broadcast the character to find to all elements of zch
    zch = _mm512_set1_epi8(ch);

    // Calculate the offset to align the source pointer to 64 bytes
    offset = (uintptr_t)str & (ZMM_SZ - 1);

    // Handle cases where start of string is close to the end of a memory page
    if (unlikely((PAGE_SZ - ZMM_SZ) < ((PAGE_SZ -1) & (uintptr_t)str)))
    {
        z7 = _mm512_set1_epi8(0xff);
        //Mask to load only the valid bytes from the string without crossing the page boundary
        mask = _bzhi_u64((uint64_t)-1, ZMM_SZ - offset);
        z1 = _mm512_mask_loadu_epi8(z7, mask, str);
    }
    // Load first 64 bytes from `str` into z1
    else
    {
        z1 = _mm512_loadu_si512(str);
    }

    // Compares bytes in z1 with the target character ch and check for null termination;
    // It returns a pointer to the first occurrence of ch or NULL if ch is not found before NULL.
    ret = _mm512_cmpeq_epu8_mask(_mm512_min_epu8(_mm512_xor_si512(z1, zch),z1), z0);
    if (ret)
    {
        if(*((char*)str + _tzcnt_u64(ret)) == ch)
            return ((char*)str + _tzcnt_u64(ret));
        return NULL;
    }
    // Adjust the offset to align with cache line for the next load operation
    offset = ZMM_SZ - offset;

    // Process the next 7 vectors (7 * 64B) with offset adjustments
    uint8_t cnt_vec = 7;
    while (cnt_vec--)
    {
        _mm_prefetch(str + offset + ZMM_SZ, _MM_HINT_NTA);
        z1 = _mm512_load_si512(str + offset);
        zxor = _mm512_xor_si512(z1, zch);
        zmin = _mm512_min_epu8(zxor, z1);
        ret = _mm512_testn_epi8_mask(zmin,zmin);

        if (ret)
        {
            if(*((char*)str + _tzcnt_u64(ret) + offset) == ch)
                return (char*)str + _tzcnt_u64(ret) + offset;
            return NULL;
        }

        offset += ZMM_SZ;
    }

    // Ensure the next 4 * 64B vector loads in the main loop
    // do not cross a page boundary to prevent potential seg faults.
    cnt_vec = 4 - ((((uintptr_t)str + offset) & (4 * ZMM_SZ - 1)) >> 6);
    while (cnt_vec--) {
        z1 = _mm512_load_si512(str + offset);
        ret = _mm512_cmpeq_epu8_mask(_mm512_min_epu8(_mm512_xor_si512(z1, zch),z1), z0);
        if (ret)
        {
            if(*((char*)str + _tzcnt_u64(ret) + offset) == ch )
                return (char*)str + _tzcnt_u64(ret) + offset;
            return NULL;
        }
        offset += ZMM_SZ;
    }

    // Main loop to process 4 vectors at a time for larger sizes(> 512B)
    while(1)
    {
        // Load 4 vectors from `str`
        z1 = _mm512_load_si512(str + offset);
        z2 = _mm512_load_si512(str + offset + 1 * ZMM_SZ);
        z3 = _mm512_load_si512(str + offset + 2 * ZMM_SZ);
        z4 = _mm512_load_si512(str + offset + 3 * ZMM_SZ);

        // Compare the loaded vectors with the broadcasted char and create masks of non-matches
        match_mask1 = _mm512_cmpneq_epu8_mask(z1, zch);
        match_mask2 = _mm512_cmpneq_epu8_mask(z2, zch);

        // Find the minimum of the first two vectors to check for null terminator or target char
        z2 = _mm512_min_epu8(z1, z2);

        // Continue comparing the remaining vectors with the character and update masks
        match_mask3 = _mm512_mask_cmpneq_epu8_mask(match_mask1, z3, zch);
        match_mask4 = _mm512_mask_cmpneq_epu8_mask(match_mask2, z4, zch);

        // Find the minimum of the last two vectors and apply the mask to check for null
        // terminator or the target character
        z4 = _mm512_min_epu8(z3, z4);
        z4 = _mm512_maskz_min_epu8(match_mask3, z2, z4);

        // Test the combined mask to see if all bits are set;
        // ndicating no matches or null terminators found
        match_mask = _mm512_mask_test_epi8_mask(match_mask4, z4, z4);

        // a match or null terminator has been found; exit the loop
        if (match_mask != ALL_BITS_SET_64 )
            break;
        offset += 4 * ZMM_SZ;
    }

    // Check each vector individually for a match or null terminator and return the result if found
    match_mask1 = _mm512_mask_test_epi8_mask(match_mask1, z1, z1);
    if (match_mask1 != ALL_BITS_SET_64 )
    {
        result = (char*)(str + _tzcnt_u64(~match_mask1) + offset);
        if (!(*result ^ ch))
            return result;
        return NULL;
    }
    match_mask2 = _mm512_mask_test_epi8_mask(match_mask2, z2, z2);
    if (match_mask2 != ALL_BITS_SET_64)
    {
        result = (char*)(str + _tzcnt_u64(~match_mask2) + offset + 1 * ZMM_SZ);
        if (!(*result ^ ch))
            return result;
        return NULL;
    }
    match_mask3 = _mm512_mask_test_epi8_mask(match_mask3, z3, z3);
    if (match_mask3 != ALL_BITS_SET_64)
    {
        result = (char*)(str + _tzcnt_u64(~match_mask3) + offset + 2 * ZMM_SZ);
        if (!(*result ^ ch))
            return result;
        return NULL;
    }
    result = (char*)(str + _tzcnt_u64(~match_mask) + offset + 3 * ZMM_SZ);
    if (!(*result ^ ch))
        return result;
    return NULL;
}
