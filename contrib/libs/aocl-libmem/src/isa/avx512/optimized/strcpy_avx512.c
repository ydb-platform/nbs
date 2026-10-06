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
#include "zen_cpu_info.h"
#include "../../../base_impls/memset_erms_impls.h"
#ifdef STRNCPY_AVX512
/* This is an optimized function to fill a memory region with null bytes ('\0') */
static inline void *_fill_null_avx512(void *mem, size_t size)
{
    __m512i z0;

    if (size < 2 * ZMM_SZ)
    {
        // Use the ERMS feature to fill memory with zeros
        // This is likely more efficient for small sizes
        return __erms_stosb(mem, 0, size);
    }

    z0 = _mm512_set1_epi8(0);
    if (size <= 4 * ZMM_SZ)
    {
        _mm512_storeu_si512(mem , z0);
        _mm512_storeu_si512(mem + ZMM_SZ, z0);
        _mm512_storeu_si512(mem + size - 2 * ZMM_SZ, z0);
        _mm512_storeu_si512(mem + size - ZMM_SZ, z0);
        return mem;
    }
    _mm512_storeu_si512(mem + 0 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + 1 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + 2 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + 3 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + size - 4 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + size - 3 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + size - 2 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + size - 1 * ZMM_SZ, z0);

    if (size <= 8 * ZMM_SZ)
        return mem;

    size_t offset = 4 * ZMM_SZ;
    size -= 4 * ZMM_SZ;
    offset -= ((uint64_t)mem & (ZMM_SZ-1));

    while( offset < size )
    {
        _mm512_store_si512(mem + offset + 0 * ZMM_SZ, z0);
        _mm512_store_si512(mem + offset + 1 * ZMM_SZ, z0);
        _mm512_store_si512(mem + offset + 2 * ZMM_SZ, z0);
        _mm512_store_si512(mem + offset + 3 * ZMM_SZ, z0);
        offset += 4 * ZMM_SZ;
    }
    return mem;
}
#endif

/* This function is an optimized version of strcpy and strncpy using AVX-512 instructions.
It copies a null-terminated string from `src` to `dst`, returning the original value of `dst` */
#ifdef STRNCPY_AVX512
static inline char * __attribute__((flatten)) _strncpy_avx512(char *dst, const char *src, size_t size)
#else
static inline char * __attribute__((flatten)) _strcpy_avx512(char *dst, const char *src)
#endif
{
#ifdef STRNCPY_AVX512
    if (unlikely(size == 0))
        return dst;
    size_t slen,rem,null_idx,null_start;
#endif

    size_t offset, index;
    __m512i z0, z1, z2, z3, z4, z5, z6;
    uint64_t match, match1,match2;
    __mmask64 mask;

    // Store the original destination pointer to return at the end
    register void *ret asm("rax");
    ret = dst;

    // Initialize a zeroed AVX-512 register for comparisons
    z0 = _mm512_setzero_epi32();

    // Calculate the offset to align the source pointer to 64 bytes
    offset = (uintptr_t)src & (ZMM_SZ - 1);

    // Handle cases where `src` is close to the end of a memory page
    if (unlikely((PAGE_SZ - ZMM_SZ) < ((PAGE_SZ - 1) & (uintptr_t)src)))
    {
        // Load bytes from `src` into z1 with a offsetted mask to avoid crossing the page boundary
        z6 = _mm512_set1_epi8(0xff);
        mask = UINT64_MAX >> offset;
        z1 =  _mm512_mask_loadu_epi8(z6 ,mask, src);

        // Compare `z1` with zero register to find the null terminator
        match = _mm512_cmpeq_epu8_mask(z1, z0);

        // If a null terminator is found, calculate the index and store the result
        if (match)
        {
            index =  _tzcnt_u64(match);
#ifdef STRNCPY_AVX512
            slen = (index + 1 < size) ? index + 1 : size;
            mask = _bzhi_u64(UINT64_MAX, slen);
            _mm512_mask_storeu_epi8(dst, mask, z1);
            // Null fill
            if (likely(index + 1 < size))
                _fill_null_avx512(dst + index + 1, size - (index + 1));
#else
            mask = UINT64_MAX >> (63 - (index));
            _mm512_mask_storeu_epi8(dst, mask, z1);
#endif
            return ret;
        }
    }
    // Load the first 64 bytes from `src` and check for null terminator
#ifdef STRNCPY_AVX512
    // For strncpy with small size, use masked load to avoid reading beyond valid memory
    if (size < ZMM_SZ) {
        mask = _bzhi_u64(UINT64_MAX, size);
        z1 = _mm512_maskz_loadu_epi8(mask, src);
    }
    else {
        z1 = _mm512_loadu_si512(src);
    }
#else
    z1 = _mm512_loadu_si512(src);
#endif
    match = _mm512_cmpeq_epu8_mask(z0, z1);
    if (match)
    {
        index =  _tzcnt_u64(match);
#ifdef STRNCPY_AVX512
        slen = (index + 1 < size) ? index + 1 : size;
        mask = _bzhi_u64(UINT64_MAX, slen);
        _mm512_mask_storeu_epi8(dst, mask, z1);
        // NULL fill
        if (likely(index + 1 < size))
            _fill_null_avx512(dst + index + 1, size - (index + 1));
#else
        mask = UINT64_MAX >> (63 - index);
        _mm512_mask_storeu_epi8(dst, mask, z1);
#endif

        return ret;
    }
    // Store the first 64 bytes to `dst`
#ifdef STRNCPY_AVX512
    // Use fast path for full vector, masked operation only if needed
    if (size >= ZMM_SZ) {
        _mm512_storeu_si512(dst, z1);
    } else {
        mask = _bzhi_u64(UINT64_MAX, size);
        _mm512_mask_storeu_epi8(dst, mask, z1);
    }
#else
    _mm512_storeu_si512(dst, z1);
#endif

    // Adjust the offset for the next load operation
    offset = ZMM_SZ - offset;

    // Initialize a counter to process the next 192 bytes (3 * 64B) from the source
    uint8_t cnt_vec = 2;
    do
    {
#ifdef STRNCPY_AVX512
        if (offset >= size) break;
#endif
        z2 = _mm512_load_si512(src + offset);
        match = _mm512_cmpeq_epu8_mask(z2, z0);

        // If a null terminator is found within the loaded bytes, copy the tail vector.
        // Load and copy the remaining bytes up to and including the null terminator
        if (match)
        {
#ifdef STRNCPY_AVX512
            null_idx = _tzcnt_u64(match);
            rem = size - offset;
            slen = (null_idx + 1 < rem) ? null_idx + 1 : rem;
            
            // Use fast path for full vector, masked operation only if needed
            if (slen >= ZMM_SZ) {
                _mm512_storeu_si512(dst + offset, z2);
            } else {
                mask = _bzhi_u64(UINT64_MAX, slen);
                _mm512_mask_storeu_epi8(dst + offset, mask, z2);
            }
            
            // Null fill
            null_start = offset + null_idx + 1;
            if (likely(null_start < size)) {
                _fill_null_avx512(dst + null_start, size - null_start);
            }
#else
            index = offset + _tzcnt_u64(match) - ZMM_SZ + 1;
            z5 = _mm512_loadu_si512(src + index);
            _mm512_storeu_si512(dst + index, z5);
#endif

            return ret;
        }

#ifdef STRNCPY_AVX512
        rem = size - offset;
        
        // Use fast path for full vector, masked operation only if needed
        if (rem >= ZMM_SZ) {
            _mm512_storeu_si512(dst + offset, z2);
        } else {
            mask = _bzhi_u64(UINT64_MAX, rem);
            _mm512_mask_storeu_epi8(dst + offset, mask, z2);
        }
#else
        _mm512_storeu_si512(dst + offset, z2);
#endif
        offset += ZMM_SZ;
    } while (cnt_vec--);


    // Determine if the next four 64-byte blocks (256 bytes total) are within the same memory
    // page , this is to avoid crossing a page boundary, which could lead to a page fault if
    // the memory is not mapped. Here it calculates and processes the maximum number of 64-byte
    // vectors that can be handled without the risk of crossing into an adjacent memory page.
    cnt_vec = 4 - ((((uintptr_t)src + offset) & (4 * ZMM_SZ - 1)) >> 6);
    while (cnt_vec--)
    {
#ifdef STRNCPY_AVX512
        if (offset >= size) break;
#endif
        z2 = _mm512_load_si512(src + offset);
        match = _mm512_cmpeq_epu8_mask(z2, z0);

        if (match)
        {
            index =  _tzcnt_u64(match);
#ifdef STRNCPY_AVX512
            rem = size - offset;
            slen = (index + 1 < rem) ? index + 1 : rem;
            
            // Use fast path for full vector, masked operation only if needed
            if (slen >= ZMM_SZ) {
                _mm512_storeu_si512(dst + offset, z2);
            } else {
                mask = _bzhi_u64(UINT64_MAX, slen);
                _mm512_mask_storeu_epi8(dst + offset, mask, z2);
            }
            
            // Null fill
            null_start = offset + index + 1;
            if (likely(null_start < size)) {
                _fill_null_avx512(dst + null_start, size - null_start);
            }
#else
            mask = UINT64_MAX >> (63 - index);
            _mm512_mask_storeu_epi8(dst + offset, mask, z2);
#endif

            return ret;
        }

#ifdef STRNCPY_AVX512
        rem = size - offset;
        
        // Use fast path for full vector, masked operation only if needed
        if (rem >= ZMM_SZ) {
            _mm512_storeu_si512(dst + offset, z2);
        } else {
            mask = _bzhi_u64(UINT64_MAX, rem);
            _mm512_mask_storeu_epi8(dst + offset, mask, z2);
        }
#else
        _mm512_storeu_si512(dst + offset, z2);
#endif
        offset += ZMM_SZ;
    }

    // Main loop to process 4 vectors at a time for larger sizes(> 256B)
#ifdef STRNCPY_AVX512
    while (offset + 4 * ZMM_SZ <= size)
#else
    while (1)
#endif
    {
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

#ifdef STRNCPY_AVX512
    // Handle remaining bytes
    if (offset < size) goto handle_remaining;
    return ret;
#endif

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
#ifdef STRNCPY_AVX512
        null_idx = _tzcnt_u64(match) + 1;
        rem = size - offset;
        slen = (null_idx < rem) ? null_idx : rem;
        
        if (slen >= ZMM_SZ) {
            z5 = _mm512_loadu_si512(src + offset);
            _mm512_storeu_si512(dst + offset, z5);
        } else {
            mask = _bzhi_u64(UINT64_MAX, slen);
            z5 = _mm512_maskz_loadu_epi8(mask, src + offset);
            _mm512_mask_storeu_epi8(dst + offset, mask, z5);
        }
        
        // Null fill
        null_start = offset + null_idx;
        if (likely(null_start < size)) {
            _fill_null_avx512(dst + null_start, size - null_start);
        }
#else
        index = offset + _tzcnt_u64(match) - ZMM_SZ + 1;
        z5 = _mm512_loadu_si512(src + index);
        _mm512_storeu_si512(dst + index, z5);
#endif
        return ret;
    }

#ifdef STRNCPY_AVX512
handle_remaining:
    // Remaining bytes handling
    while (offset < size) {
        rem = size - offset;
        __mmask64 block = (rem >= ZMM_SZ) ? UINT64_MAX : _bzhi_u64(UINT64_MAX, rem);
        z1 = _mm512_maskz_loadu_epi8(block, src + offset);

        uint64_t null_mask = _mm512_cmpeq_epu8_mask(z1, z0) & block;
        if (null_mask) {
            // Found null
            null_idx = _tzcnt_u64(null_mask) + 1;
            slen = (null_idx < rem) ? null_idx : rem;
            
            if (slen >= ZMM_SZ) {
                _mm512_storeu_si512(dst + offset, z1);
            } else {
                mask = _bzhi_u64(UINT64_MAX, slen);
                _mm512_mask_storeu_epi8(dst + offset, mask, z1);
            }
            
            // Null fill
            null_start = offset + null_idx;
            if (null_start < size)
                _fill_null_avx512(dst + null_start, size - null_start);
            return ret;
        }

        // No null found 
        if (rem >= ZMM_SZ) {
            _mm512_storeu_si512(dst + offset, z1);
        } else {
            _mm512_mask_storeu_epi8(dst + offset, block, z1);
        }
        offset += ZMM_SZ;
    }
    return ret;
#endif
}