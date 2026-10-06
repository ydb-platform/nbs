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
#include "logger.h"
#include "threshold.h"
#include "../../../base_impls/load_store_impls.h"
#include "zen_cpu_info.h"
#include "almem_defs.h"

extern cpu_info zen_info;

static inline void *_memcpy_avx2(void * __restrict dst, const void * __restrict src, size_t size)
{
    register void *ret asm("rax");
    ret = dst;

    if (likely(size <= 4 * YMM_SZ)) //128B
    {
        if (size < YMM_SZ)
        {
            if (size >= XMM_SZ) //16-31B
            {
                __m128i x0 = _mm_loadu_si128((const __m128i *)src);
                __m128i x1 = _mm_loadu_si128((const __m128i *)(src + size - XMM_SZ));
                _mm_storeu_si128((__m128i *)dst, x0);
                _mm_storeu_si128((__m128i *)(dst + size - XMM_SZ), x1);
                return ret;
            }
            if (size >= QWORD_SZ) //8-15B
            {
                *((uint64_t *)dst) = *((const uint64_t *)src);
                *((uint64_t *)(dst + size - QWORD_SZ)) = *((const uint64_t *)(src + size - QWORD_SZ));
                return ret;
            }
            if (size >= DWORD_SZ) //4-7B
            {
                *((uint32_t *)dst) = *((const uint32_t *)src);
                *((uint32_t *)(dst + size - DWORD_SZ)) = *((const uint32_t *)(src + size - DWORD_SZ));
                return ret;
            }
            if (size >= WORD_SZ) //2-3B
            {
                *((uint16_t *)dst) = *((const uint16_t *)src);
                *((uint16_t *)(dst + size - WORD_SZ)) = *((const uint16_t *)(src + size - WORD_SZ));
                return ret;
            }
            if (size)
                *((uint8_t *)dst) = *((const uint8_t *)src);
            return ret;
        }

        __m256i y0 = _mm256_loadu_si256((const __m256i *)src);
        __m256i y1 = _mm256_loadu_si256((const __m256i *)(src + size - YMM_SZ));

        if (likely(size <= 2 * YMM_SZ)) //32-64B
        {
            _mm256_storeu_si256((__m256i *)dst, y0);
            _mm256_storeu_si256((__m256i *)(dst + size - YMM_SZ), y1);
            return ret;
        }

        // 65-128B
        __m256i y2 = _mm256_loadu_si256((const __m256i *)(src + YMM_SZ));
        __m256i y3 = _mm256_loadu_si256((const __m256i *)(src + size - 2 * YMM_SZ));
        ALM_MEM_BARRIER();
        _mm256_storeu_si256((__m256i *)dst, y0);
        _mm256_storeu_si256((__m256i *)(dst + YMM_SZ), y2);
        _mm256_storeu_si256((__m256i *)(dst + size - 2 * YMM_SZ), y3);
        _mm256_storeu_si256((__m256i *)(dst + size - YMM_SZ), y1);
        return ret;
    }
    else if (likely(size <= 8 * YMM_SZ)) //256B
    {
        __load_store_le_8ymm_vec(dst, src, size);
        return ret;
    }

    // Compute alignment of destination address
    uint8_t dst_align = ((size_t)dst & (YMM_SZ - 1));
    // Adjust alignment to 4 VEC alignment
    dst_align = 4 * YMM_SZ - dst_align;

    // Load-Store first 4 VECs
    __load_store_4ymm_vec(dst, src, 0);
    // Check if the size is less than or equal to the L1D cache size per core
    // Note: The L1D cache size can vary depending on the underlying architecture
    if (size <= zen_info.zen_cache_info.l1d_per_core)
    {
        size_t offset = __unaligned_load_aligned_store_4ymm_vec_loop_pftch(dst, src, size - 4 * YMM_SZ, dst_align);

        uint16_t rem_data = size - offset;
        uint8_t rem_vecs = ((rem_data & 0x3E0) >> 5) + !!(rem_data & 0x1F);

        switch (rem_vecs)
        {
        case 1:
            __load_store_ymm_vec(dst + size - YMM_SZ, src + size - YMM_SZ, 0);
            break;
        case 2:
            __load_store_le_2ymm_vec(dst + size - 2 * YMM_SZ, src + size - 2 * YMM_SZ, 2 * YMM_SZ);
            break;
        case 3:
        case 4:
            __load_store_le_4ymm_vec(dst + size - 4 * YMM_SZ, src + size - 4 * YMM_SZ, 4 * YMM_SZ);
        }
        return ret;
    }

    // Matching alignments of load & store addresses;
    if (unlikely((((size_t)src & (YMM_SZ - 1)) == ((size_t)dst & (YMM_SZ - 1)))))
    {
        if (size < __nt_start_threshold)
        {
           __aligned_load_and_store_4ymm_vec_loop(dst, src, size - 4 * YMM_SZ, dst_align);
        }
        else
        {
           __aligned_load_nt_store_4ymm_vec_loop(dst, src, size - 4 * YMM_SZ, dst_align);
        }
    }
    // Mismatching alignments of load & store addresses;
    else
    {
        if (size < __nt_start_threshold)
        {
           __unaligned_load_aligned_store_4ymm_vec_loop_pftch(dst, src, size - 4 * YMM_SZ, dst_align);
        }
        else
        {
           __unaligned_load_nt_store_4ymm_vec_loop(dst, src, size - 4 * YMM_SZ, dst_align);
        }
    }

    // Load-Store Last 4 VECs
    __load_store_le_4ymm_vec(dst + size - 4 * YMM_SZ, src + size - 4 * YMM_SZ, 4 * YMM_SZ);

    return ret;
}
