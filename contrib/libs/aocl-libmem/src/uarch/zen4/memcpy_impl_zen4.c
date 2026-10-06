/* Copyright (C) 2025-26 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef MEMCPY_IMPL_ZEN4_H
#define MEMCPY_IMPL_ZEN4_H

#include "threshold.h"
#include "../../base_impls/load_store_impls.h"
#include "zen_cpu_info.h"
#include "almem_defs.h"

extern cpu_info zen_info;

#ifdef MEMMOVE_ZEN4
static inline void *_memmove_zen4_impl(void *dst, const void *src, size_t size)
#else
static inline void *_memcpy_zen4_impl(void *__restrict dst, const void *__restrict src, size_t size)
#endif
{
    register void *ret asm("rax");
    ret = dst;

#ifdef MEMMOVE_ZEN4
    /* __memmove_zen4 handles size <= 2 * ZMM_SZ (128B) in its own overlap-safe
     * head+tail tier and only delegates here for size > 128B */
    if (size <= 8 * ZMM_SZ) // 512B
    {
        if (size <= 4 * ZMM_SZ) // 256B
        {
            __load_store_le_4zmm_vec(dst, src, size);
            return ret;
        }
        __load_store_le_8zmm_vec(dst, src, size);
        return ret;
    }

    // Handle overlapping memory blocks
    if (unlikely(!(((dst + size) < src) || ((src + size) < dst))))
    {
        __m512i z8;
        if (src < dst) // Backward Copy
        {
            size_t off = ((size_t) dst + size) & (ZMM_SZ - 1);
            size_t size_temp = size;

            // Load the last VEC to handle alignment not multiple of the vec
            z8 = _mm512_loadu_si512(src + size - ZMM_SZ);

            size_t rem_data;
            if (((size_t) dst & (ZMM_SZ - 1)) == ((size_t) src & (ZMM_SZ - 1)))
            {
                rem_data = __aligned_load_and_store_4zmm_vec_loop_bkwd_pftch(dst, src, size - off, 4 * ZMM_SZ);
            } else
            {
                rem_data = __unaligned_load_aligned_store_4zmm_vec_loop_bkwd_pftch(dst, src, size - off, 4 * ZMM_SZ);
            }

            // Handle remaining data
            if (rem_data > 2 * ZMM_SZ)
                __load_store_le_4zmm_vec(dst, src, rem_data);
            else if (rem_data > ZMM_SZ)
                __load_store_le_2zmm_vec(dst, src, rem_data);
            else if (rem_data > 0)
                __load_store_ble_zmm_vec(dst, src, rem_data);

            // Store the last VEC
            _mm512_storeu_si512(dst + size_temp - ZMM_SZ, z8);
        } else // Forward Copy
        {
            size_t offset = ZMM_SZ - ((size_t) dst & (ZMM_SZ - 1));

            // Load the first VEC
            z8 = _mm512_loadu_si512(src);

            if (((size_t) dst & (ZMM_SZ - 1)) == ((size_t) src & (ZMM_SZ - 1)))
                offset = __aligned_load_and_store_4zmm_vec_loop_pftch(dst, src, size - 4 * ZMM_SZ, offset);
            else
                offset = __unaligned_load_aligned_store_4zmm_vec_loop_pftch(dst, src, size - 4 * ZMM_SZ, offset);

            // Handle remaining data at the end
            size_t rem_data = size - offset;
            if (rem_data > 2 * ZMM_SZ)
                __load_store_le_4zmm_vec(dst + offset, src + offset, rem_data);
            else if (rem_data > ZMM_SZ)
                __load_store_le_2zmm_vec(dst + offset, src + offset, rem_data);
            else if (rem_data > 0)
                __load_store_ble_zmm_vec(dst + offset, src + offset, rem_data);

            // Store the first VEC
            _mm512_storeu_si512(dst, z8);
        }
        return ret;
    }
#else
    if (size <= 2 * ZMM_SZ) // 128B
    {
        if (size < ZMM_SZ)
        {
            (void) __load_store_ble_zmm_vec(dst, src, (uint8_t) size);
            return ret;
        }
        // TODO: fix gaps between 64B and 128B
        __load_store_le_2zmm_vec(dst, src, (uint8_t) size);
        return ret;
    } 
    else if (size <= 8 * ZMM_SZ) // 512B
    {
        if (size <= 4 * ZMM_SZ) // 256B
        {
            __load_store_le_4zmm_vec(dst, src, size);
            return ret;
        }
        __load_store_le_8zmm_vec(dst, src, size);
        return ret;
    }
#endif

    size_t offset = 0;
    uint32_t dst_align = ((uintptr_t) dst & (ZMM_SZ - 1));

    __load_store_le_8zmm_vec(dst, src, size);
    offset = 4 * ZMM_SZ - dst_align;

    // Aligned Load and Store addresses
    if (((uintptr_t) src & (ZMM_SZ - 1)) == dst_align)
    {
        // 4-ZMM registers
        if (size < zen_info.zen_cache_info.l2_per_core) // L2 Cache Size
        {
            __aligned_load_and_store_8ymm_vec_loop(dst, src, size - 4 * ZMM_SZ, offset);
        }
        // 4-YMM registers with prefetch
        else if (size < __nt_start_threshold)
        {
            __aligned_load_and_store_4ymm_vec_loop_pftch(dst, src, size - 4 * ZMM_SZ, offset);
        }
        // Non-temporal 4-ZMM registers with prefetch
        else
        {
            __aligned_load_nt_store_4zmm_vec_loop_pftch(dst, src, size - 4 * ZMM_SZ, offset);
        }
    }
    // Unaligned Load/Store addresses: force-align store address to ZMM size
    else
    {
        if (size < __nt_start_threshold)
        {
            __unaligned_load_aligned_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, offset);
        } else
        {
            __unaligned_load_nt_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, offset);
        }
    }
    return ret;
}

#ifdef MEMMOVE_ZEN4
#undef MEMMOVE_ZEN4
#endif
#endif // MEMCPY_IMPL_ZEN4_H