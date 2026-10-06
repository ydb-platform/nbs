/* Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.
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
 #include <stddef.h>
 #include <stdint.h>
 #include <zen_cpu_info.h>
#include "almem_defs.h"
#include "logger.h"

static inline void strcpy_ble_ymm(void *dst, const void *src, uint32_t size)
{
    __m128i x0, x1;

    if (size == 1)
    {
        *((uint8_t *) dst) = *((uint8_t *) src);
        return;
    }
    if (size <= 2 * WORD_SZ)
    {
        *((uint16_t *) dst) = *((uint16_t *) src);
        *((uint16_t *) (dst + size - WORD_SZ)) = *((uint16_t *) (src + size - WORD_SZ));
        return;
    }
    if (size <= 2 * DWORD_SZ)
    {
        *((uint32_t *) dst) = *((uint32_t *) src);
        *((uint32_t *) (dst + size - DWORD_SZ)) = *((uint32_t *) (src + size - DWORD_SZ));
        return;
    }
    if (size <= 2 * QWORD_SZ)
    {
        *((uint64_t *) dst) = *((uint64_t *) src);
        *((uint64_t *) (dst + size - QWORD_SZ)) = *((uint64_t *) (src + size - QWORD_SZ));
        return;
    }
    x0 = _mm_loadu_si128((void *) src);
    _mm_storeu_si128((void *) dst, x0);
    x1 = _mm_loadu_si128((void *) (src + size - XMM_SZ));
    _mm_storeu_si128((void *) (dst + size - XMM_SZ), x1);
    return;
}

#ifdef STRNCPY_AVX2
/* This is a function to fill a memory region with null bytes ('\0'). */
static inline void *_fill_null_avx2(void *mem, size_t size)
{
    __m256i y0;
    __m128i x0;

    if (size <= 2 * YMM_SZ)
    {
        if (size >= YMM_SZ)
        {
            y0 = _mm256_setzero_si256();
            _mm256_storeu_si256(mem, y0);
            _mm256_storeu_si256(mem + size - YMM_SZ, y0);
            return mem;
        }
        if (size >= XMM_SZ)
        {
            x0 = _mm_setzero_si128();
            _mm_storeu_si128(mem, x0);
            _mm_storeu_si128(mem + size - XMM_SZ, x0);
            return mem;
        }
        if (size >= QWORD_SZ)
        {
            *((uint64_t *) mem) = 0;
            *((uint64_t *) (mem + size - QWORD_SZ)) = 0;
            return mem;
        }
        if (size >= DWORD_SZ)
        {
            *((uint32_t *) mem) = 0;
            *((uint32_t *) (mem + size - DWORD_SZ)) = 0;
            return mem;
        }
        if (size >= WORD_SZ)
        {
            *((uint16_t *) mem) = 0;
            *((uint16_t *) (mem + size - WORD_SZ)) = 0;
            return mem;
        }
        if (size > 0)
            *((uint8_t *) mem) = 0;
        return mem;
    }

    if (size <= 4 * YMM_SZ)
    {
        y0 = _mm256_setzero_si256();
        _mm256_storeu_si256(mem, y0);
        _mm256_storeu_si256(mem + YMM_SZ, y0);
        _mm256_storeu_si256(mem + size - 2 * YMM_SZ, y0);
        _mm256_storeu_si256(mem + size - YMM_SZ, y0);
        return mem;
    }

    y0 = _mm256_setzero_si256();
    _mm256_storeu_si256(mem + 0 * YMM_SZ, y0);
    _mm256_storeu_si256(mem + 1 * YMM_SZ, y0);
    _mm256_storeu_si256(mem + 2 * YMM_SZ, y0);
    _mm256_storeu_si256(mem + 3 * YMM_SZ, y0);
    _mm256_storeu_si256(mem + size - 4 * YMM_SZ, y0);
    _mm256_storeu_si256(mem + size - 3 * YMM_SZ, y0);
    _mm256_storeu_si256(mem + size - 2 * YMM_SZ, y0);
    _mm256_storeu_si256(mem + size - 1 * YMM_SZ, y0);

    if (size <= 8 * YMM_SZ)
        return mem;

    size_t offset = 4 * YMM_SZ;
    size -= 4 * YMM_SZ;
    offset -= ((uint64_t) mem & (YMM_SZ - 1));

    while (offset < size)
    {
        _mm256_store_si256(mem + offset + 0 * YMM_SZ, y0);
        _mm256_store_si256(mem + offset + 1 * YMM_SZ, y0);
        _mm256_store_si256(mem + offset + 2 * YMM_SZ, y0);
        _mm256_store_si256(mem + offset + 3 * YMM_SZ, y0);
        offset += 4 * YMM_SZ;
    }
    return mem;
}

/*
 * Optimized strncpy for small sizes (1B-31B)
 */
static inline char *__attribute__((always_inline)) _strncpy_small_avx2(char *dst, const char *src, size_t size)
{
    __m128i x0 = _mm_setzero_si128();
    __m128i x1, x2;
    uint32_t null_mask;
    size_t null_pos;

    if (size <= DWORD_SZ)
    {
        if (size == 1)
        {
            dst[0] = src[0];
            return dst;
        }
#ifndef ALMEM_STRICT_BOUNDS
        uint32_t chunk = *((uint32_t *) src);
        x1 = _mm_cvtsi32_si128(chunk);
        null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0xF;

        if (null_mask)
        {
            null_pos = _tzcnt_u32(null_mask);
            if (null_pos < size)
            {
                *((uint16_t *) dst) = *((uint16_t *) src);
                if (size > WORD_SZ)
                    *((uint16_t *) (dst + size - WORD_SZ)) = *((uint16_t *) (src + size - WORD_SZ));
                if (null_pos + 1 < size)
                    _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
                return dst;
            }
        }
        *((uint16_t *) dst) = *((uint16_t *) src);
        if (size > WORD_SZ)
            *((uint16_t *) (dst + size - WORD_SZ)) = *((uint16_t *) (src + size - WORD_SZ));
        return dst;
#else
        uint16_t w0 = *((uint16_t *) src);
        x1 = _mm_cvtsi32_si128(w0);
        null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0x3;

        if (null_mask)
        {
            null_pos = _tzcnt_u32(null_mask);
            *((uint16_t *) dst) = w0;
            if (null_pos + 1 < size)
                _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
            return dst;
        }

        *((uint16_t *) dst) = w0;
        if (size > WORD_SZ)
        {
            uint16_t w1 = *((uint16_t *) (src + size - WORD_SZ));
            x1 = _mm_cvtsi32_si128(w1);
            uint32_t null_mask2 = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0x3;

            *((uint16_t *) (dst + size - WORD_SZ)) = w1;
            if (null_mask2)
            {
                null_pos = (size - WORD_SZ) + _tzcnt_u32(null_mask2);
                if (null_pos + 1 < size)
                    _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
            }
        }
        return dst;
#endif
    }

    if (size <= QWORD_SZ)
    {
#ifndef ALMEM_STRICT_BOUNDS
        uint64_t chunk = *((uint64_t *) src);
        x1 = _mm_cvtsi64_si128(chunk);
        null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0xFF;

        if (null_mask)
        {
            null_pos = _tzcnt_u32(null_mask);
            if (null_pos < size)
            {
                *((uint32_t *) dst) = *((uint32_t *) src);
                *((uint32_t *) (dst + size - DWORD_SZ)) = *((uint32_t *) (src + size - DWORD_SZ));
                if (null_pos + 1 < size)
                    _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
                return dst;
            }
        }
        *((uint32_t *) dst) = *((uint32_t *) src);
        *((uint32_t *) (dst + size - DWORD_SZ)) = *((uint32_t *) (src + size - DWORD_SZ));
        return dst;
#else
        uint32_t d0 = *((uint32_t *) src);
        x1 = _mm_cvtsi32_si128(d0);
        null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0xF;

        if (null_mask)
        {
            null_pos = _tzcnt_u32(null_mask);
            *((uint32_t *) dst) = d0;
            if (null_pos + 1 < size)
                _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
            return dst;
        }

        uint32_t d1 = *((uint32_t *) (src + size - DWORD_SZ));
        x1 = _mm_cvtsi32_si128(d1);
        uint32_t null_mask2 = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0xF;

        *((uint32_t *) dst) = d0;
        *((uint32_t *) (dst + size - DWORD_SZ)) = d1;
        if (null_mask2)
        {
            null_pos = (size - DWORD_SZ) + _tzcnt_u32(null_mask2);
            if (null_pos + 1 < size)
                _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
        }
        return dst;
#endif
    }

    if (size <= XMM_SZ)
    {
#ifndef ALMEM_STRICT_BOUNDS
        x1 = _mm_loadu_si128((void *) src);
        null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0));
        null_mask &= (1U << size) - 1;

        if (null_mask)
        {
            null_pos = _tzcnt_u32(null_mask);
            *((uint64_t *) dst) = *((uint64_t *) src);
            *((uint64_t *) (dst + size - QWORD_SZ)) = *((uint64_t *) (src + size - QWORD_SZ));
            if (null_pos + 1 < size)
                _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
            return dst;
        }
        *((uint64_t *) dst) = *((uint64_t *) src);
        *((uint64_t *) (dst + size - QWORD_SZ)) = *((uint64_t *) (src + size - QWORD_SZ));
        return dst;
#else
        uint64_t q0 = *((uint64_t *) src);
        x1 = _mm_cvtsi64_si128(q0);
        null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0xFF;

        if (null_mask)
        {
            null_pos = _tzcnt_u32(null_mask);
            *((uint64_t *) dst) = q0;
            if (null_pos + 1 < size)
                _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
            return dst;
        }

        uint64_t q1 = *((uint64_t *) (src + size - QWORD_SZ));
        x1 = _mm_cvtsi64_si128(q1);
        uint32_t null_mask2 = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0)) & 0xFF;

        *((uint64_t *) dst) = q0;
        *((uint64_t *) (dst + size - QWORD_SZ)) = q1;
        if (null_mask2)
        {
            null_pos = (size - QWORD_SZ) + _tzcnt_u32(null_mask2);
            if (null_pos + 1 < size)
                _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
        }
        return dst;
#endif
    }

    x1 = _mm_loadu_si128((void *) src);
    null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(x1, x0));

    if (null_mask)
    {
        // Null in first 16 bytes
        null_pos = _tzcnt_u32(null_mask);
        _mm_storeu_si128((void *) dst, x1);
        // Zero fill after null
        if (null_pos + 1 < size)
            _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
        return dst;
    }

    // Check second part
    x2 = _mm_loadu_si128((void *) (src + size - XMM_SZ));
    uint32_t null_mask2 = _mm_movemask_epi8(_mm_cmpeq_epi8(x2, x0));

    // Store first 16 bytes
    _mm_storeu_si128((void *) dst, x1);
    // Store last 16 bytes (overlapping)
    _mm_storeu_si128((void *) (dst + size - XMM_SZ), x2);

    if (null_mask2)
    {
        // Null found in overlapping region
        null_pos = (size - XMM_SZ) + _tzcnt_u32(null_mask2);
        if (null_pos + 1 < size)
            _fill_null_avx2(dst + null_pos + 1, size - null_pos - 1);
    }
    return dst;
}
#endif

/* This function is an optimized version of strcpy and strncpy using AVX2 instructions.
It copies a null-terminated string from `src` to `dst`, returning the original value of `dst` */
#ifdef STRNCPY_AVX2
static inline char *__attribute__((flatten)) _strncpy_avx2(char *dst, const char *src, size_t size)
#else
static inline char *__attribute__((flatten)) _strcpy_avx2(char *dst, const char *src)
#endif
{
#ifdef STRNCPY_AVX2
    if (unlikely(size == 0))
        return dst;
    size_t slen, rem, null_start, null_idx;

#endif

    size_t offset, index;
    uint32_t ret = 0, ret1 = 0, ret2 = 0;
    __m256i y0, y1, y2, y3, y4, y5, y6, y_cmp;

    // Store the original destination pointer to return at the end
    register void *ret_dst asm("rax");
    ret_dst = dst;

    // Initialize a zeroed AVX2 register for comparisons
    y0 = _mm256_setzero_si256();

    // Calculate the offset to align the source pointer to 32 bytes
    offset = (uintptr_t) src & (YMM_SZ - 1);

#if defined(STRNCPY_AVX2) && defined(ALMEM_STRICT_BOUNDS)
    if (size < YMM_SZ)
    {
        _strncpy_small_avx2(dst, src, size);
        return ret_dst;
    }
#endif

    // Handle cases where `src` is close to the end of a memory page
    if (unlikely((PAGE_SZ - YMM_SZ) < ((PAGE_SZ - 1) & (uintptr_t) src)))
    {
        y1 = _mm256_load_si256((void *) src - offset);
        y_cmp = _mm256_cmpeq_epi8(y0, y1);

        ret = (uint32_t) _mm256_movemask_epi8(y_cmp) >> offset;

        // If a null terminator is found, calculate the index and store the result
        if (ret)
        {
            index = _tzcnt_u32(ret) + 1;
#ifdef STRNCPY_AVX2
            slen = (index < size) ? index : size;

            // Use fast path for full vector, masked operation only if needed
            if (slen >= YMM_SZ)
            {
                _mm256_storeu_si256((void *) dst, y1);
            }
            else
            {
                strcpy_ble_ymm(dst, src, slen);
            }

            // Null fill
            if (likely(index < size))
                _fill_null_avx2(dst + index, size - index);
#else
            strcpy_ble_ymm(dst, src, index);
#endif
            return ret_dst;
        }
#ifdef STRNCPY_AVX2
        if (size < YMM_SZ && size <= (YMM_SZ - offset))
        {
            strcpy_ble_ymm(dst, src, size);
            return ret_dst;
        }
#endif
    }
    // Load the first 32 bytes from `src` and check for null terminator
#ifdef STRNCPY_AVX2
#ifndef ALMEM_STRICT_BOUNDS
    if (size < YMM_SZ)
    {
        _strncpy_small_avx2(dst, src, size);
        return ret_dst;
    }
#endif
    y1 = _mm256_loadu_si256((void *) src);
#else
    y1 = _mm256_loadu_si256((void *) src);
#endif

    y_cmp = _mm256_cmpeq_epi8(y0, y1);

    ret = _mm256_movemask_epi8(y_cmp);
    if (ret)
    {
        index = _tzcnt_u32(ret) + 1;
#ifdef STRNCPY_AVX2
        slen = (index < size) ? index : size;

        // Use fast path for full vector, masked operation only if needed
        if (slen >= YMM_SZ)
        {
            _mm256_storeu_si256((void *) dst, y1);
        }
        else
        {
            strcpy_ble_ymm(dst, src, slen);
        }

        // Null fill
        if (likely(index < size))
            _fill_null_avx2(dst + index, size - index);
#else
        strcpy_ble_ymm(dst, src, index);
#endif
        return ret_dst;
    }
    // Store the first 32 bytes to `dst`
#ifdef STRNCPY_AVX2
    if (size >= YMM_SZ)
    {
        _mm256_storeu_si256((void *) dst, y1);
    }
    else
    {
        strcpy_ble_ymm(dst, src, size);
        return ret_dst;
    }
#else
    _mm256_storeu_si256((void *) dst, y1);
#endif

    // Adjust the offset for the next load operation
    offset = YMM_SZ - offset;

    // Initialize a counter to process the next 192 bytes (6 * 32B) from the source
    uint8_t cnt_vec = 5;
    do
    {
#ifdef STRNCPY_AVX2
        if (offset >= size)
            break;
#endif
        y1 = _mm256_load_si256((void *) src + offset);
        y_cmp = _mm256_cmpeq_epi8(y0, y1);
        ret = _mm256_movemask_epi8(y_cmp);

        // If a null terminator is found within the loaded bytes, copy the tail vector.
        // Load and copy the remaining bytes up to and including the null terminator
        if (ret)
        {
#ifdef STRNCPY_AVX2
            null_idx = _tzcnt_u32(ret);
            rem = size - offset;
            slen = (null_idx + 1 < rem) ? null_idx + 1 : rem;

            // Use fast path for full vector, masked operation only if needed
            if (slen >= YMM_SZ)
            {
                _mm256_storeu_si256((void *) dst + offset, y1);
            }
            else
            {
                strcpy_ble_ymm(dst + offset, src + offset, slen);
            }

            // Null fill
            null_start = offset + null_idx + 1;
            if (likely(null_start < size))
            {
                _fill_null_avx2(dst + null_start, size - null_start);
            }
#else
            index = offset + _tzcnt_u32(ret) - YMM_SZ + 1;
            y2 = _mm256_loadu_si256((void *) src + index);
            _mm256_storeu_si256((void *) dst + index, y2);
#endif
            return ret_dst;
        }

#ifdef STRNCPY_AVX2
        rem = size - offset;
        if (rem >= YMM_SZ)
        {
            _mm256_storeu_si256((void *) dst + offset, y1);
        }
        else
        {
            strcpy_ble_ymm(dst + offset, src + offset, rem);
            return ret_dst;
        }
#else
        _mm256_storeu_si256((void *) dst + offset, y1);
#endif
        offset += YMM_SZ;
    } while (cnt_vec--);

    // Determine if the next four 32-byte blocks (128 bytes total) are within the same memory
    // page , this is to avoid crossing a page boundary, which could lead to a page fault if
    // the memory is not mapped. Here it calculates and processes the maximum number of 32-byte
    // vectors that can be handled without the risk of crossing into an adjacent memory page.
    cnt_vec = 4 - ((((uintptr_t) src + offset) & (4 * YMM_SZ - 1)) >> 5);
    while (cnt_vec--)
    {
#ifdef STRNCPY_AVX2
        if (offset >= size)
            break;
#endif
        y2 = _mm256_load_si256((void *) src + offset);
        y_cmp = _mm256_cmpeq_epi8(y0, y2);
        ret = _mm256_movemask_epi8(y_cmp);

        if (ret)
        {
#ifdef STRNCPY_AVX2
            null_idx = _tzcnt_u32(ret);
            rem = size - offset;
            slen = (null_idx + 1 < rem) ? null_idx + 1 : rem;

            // Use fast path for full vector, masked operation only if needed
            if (slen >= YMM_SZ)
            {
                _mm256_storeu_si256((void *) dst + offset, y2);
            }
            else
            {
                strcpy_ble_ymm(dst + offset, src + offset, slen);
            }

            // Null fill
            null_start = offset + null_idx + 1;
            if (likely(null_start < size))
            {
                _fill_null_avx2(dst + null_start, size - null_start);
            }
#else
            index = offset + _tzcnt_u32(ret) - YMM_SZ + 1;
            y3 = _mm256_loadu_si256((void *) src + index);
            _mm256_storeu_si256((void *) dst + index, y3);
#endif
            return ret_dst;
        }

#ifdef STRNCPY_AVX2
        rem = size - offset;
        if (rem >= YMM_SZ)
        {
            _mm256_storeu_si256((void *) dst + offset, y2);
        }
        else
        {
            strcpy_ble_ymm(dst + offset, src + offset, rem);
            return ret_dst;
        }
#else
        _mm256_storeu_si256((void *) dst + offset, y2);
#endif
        offset += YMM_SZ;
    }

    // Main loop to process 4 vectors at a time for larger sizes(> 256B)
#ifdef STRNCPY_AVX2
    while (offset + 4 * YMM_SZ <= size)
#else
    while (1)
#endif
    {
        y1 = _mm256_load_si256((void *) src + offset);
        y2 = _mm256_load_si256((void *) src + offset + YMM_SZ);
        y3 = _mm256_load_si256((void *) src + offset + 2 * YMM_SZ);
        y4 = _mm256_load_si256((void *) src + offset + 3 * YMM_SZ);

        // Find the minimum of the loaded vectors to check for null terminator
        y5 = _mm256_min_epu8(y1, y2);
        y6 = _mm256_min_epu8(y3, y4);

        // Compare the minimums to find the null terminator
        y_cmp = _mm256_cmpeq_epi8(_mm256_min_epu8(y5, y6), y0);
        ret = _mm256_movemask_epi8(y_cmp);

        if (ret != 0)
            break; // If found, exit the loop

        // Store the 4 vectors to `dst`
        _mm256_storeu_si256((void *) dst + offset, y1);
        _mm256_storeu_si256((void *) dst + offset + YMM_SZ, y2);
        _mm256_storeu_si256((void *) dst + offset + 2 * YMM_SZ, y3);
        _mm256_storeu_si256((void *) dst + offset + 3 * YMM_SZ, y4);

        offset += 4 * YMM_SZ;
    }

#ifdef STRNCPY_AVX2
    // Handle remaining bytes if any for STRNCPY
    if (offset < size)
        goto handle_remaining;
    // If we exit the loop in STRNCPY mode, we're done
    return ret_dst;
#endif

    // Check for null terminator in the first two vectors (y1, y2)
    if ((ret1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y5, y0))))
    {
        if ((ret2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y1, y0))))
        {
            ret = ret2;
            goto copy_tail_vec;
        }
        _mm256_storeu_si256((void *) dst + offset, y1);
        offset += YMM_SZ;
        ret = ret1;
    }
    // Check for null terminator in the last two vectors (y3, y4)
    else
    {
        _mm256_storeu_si256((void *) dst + offset, y1);
        _mm256_storeu_si256((void *) dst + offset + YMM_SZ, y2);
        if ((ret1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(y3, y0))))
        {
            ret = ret1;
            offset += 2 * YMM_SZ;
            goto copy_tail_vec;
        }
        _mm256_storeu_si256((void *) dst + offset + 2 * YMM_SZ, y3);
        offset += 3 * YMM_SZ;
    }

// Label for copying the tail of the string where the null terminator was found
copy_tail_vec:
{
#ifdef STRNCPY_AVX2
    null_idx = _tzcnt_u32(ret);
    rem = size - offset;
    slen = (null_idx + 1 < rem) ? null_idx + 1 : rem;

    // Use fast path for full vector, masked operation only if needed
    if (slen >= YMM_SZ)
    {
        _mm256_storeu_si256((void *) dst + offset, _mm256_loadu_si256((void *) src + offset));
    }
    else
    {
        strcpy_ble_ymm(dst + offset, src + offset, slen);
    }

    // Null fill
    size_t pad_start = offset + null_idx + 1;
    if (likely(pad_start < size))
    {
        _fill_null_avx2(dst + pad_start, size - pad_start);
    }
#else
    index = offset + _tzcnt_u32(ret) - YMM_SZ + 1;
    y1 = _mm256_loadu_si256((void *) src + index);
    _mm256_storeu_si256((void *) dst + index, y1);
#endif
    return ret_dst;
}

#ifdef STRNCPY_AVX2
handle_remaining:
    // Remaining bytes handling
    while (offset < size)
    {
        rem = size - offset;

        // Use fast path for full vectors when possible
        if (rem >= YMM_SZ)
        {
            y1 = _mm256_loadu_si256((void *) src + offset);
            y_cmp = _mm256_cmpeq_epi8(y0, y1);
            uint32_t null_mask = _mm256_movemask_epi8(y_cmp);

            if (null_mask)
            {
                // Found null
                null_idx = _tzcnt_u32(null_mask);
                _mm256_storeu_si256((void *) dst + offset, y1);

                // Null fill
                size_t pad_start = offset + null_idx + 1;
                if (pad_start < size)
                    _fill_null_avx2(dst + pad_start, size - pad_start);
                return ret_dst;
            }

            _mm256_storeu_si256((void *) dst + offset, y1);
            offset += YMM_SZ;
        }
        else
        {
            _strncpy_small_avx2(dst + offset, src + offset, rem);
            return ret_dst;
        }
    }
    return ret_dst;
#endif
}
