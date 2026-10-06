/* Copyright (C) 2022-25 Advanced Micro Devices, Inc. All rights reserved.
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

static inline void *_memmove_avx2(void *dst, const void *src, size_t size)
{
    __m256i y4, y5, y6, y7;
    register void *ret asm("rax");
    ret = dst;

    if (likely(size <= 2 * YMM_SZ))
        return __load_store_le_2ymm_vec_overlap(dst, src, size);

    if (size <= 4 * YMM_SZ)
    {
        __load_store_le_4ymm_vec(dst, src, size);
        return ret;
    }
    if (size <= 8 * YMM_SZ)
    {
        __load_store_le_8ymm_vec(dst, src, size);
        return ret;
    }
    if (((dst + size) < src) || ((src + size) < dst))
    {
        size_t offset;
        uint32_t dst_align = ((size_t)dst & (YMM_SZ - 1));

        __load_store_le_8ymm_vec(dst, src, size);

        offset = 4 * YMM_SZ - dst_align;

        //Aligned Load and Store addresses
        if (((uintptr_t)src & (YMM_SZ - 1)) == dst_align)
        {
            if (size < __nt_start_threshold)
               __aligned_load_and_store_4ymm_vec_loop(dst, src, size - 4 * YMM_SZ, offset);
            else
               __aligned_load_nt_store_4ymm_vec_loop_pftch(dst, src, size - 4 * YMM_SZ, offset);
        }
        else
        {
            if (size < __nt_start_threshold)
               __unaligned_load_and_store_4ymm_vec_loop(dst, src, size - 4 * YMM_SZ, offset);
            else
               __unaligned_load_nt_store_4ymm_vec_loop(dst, src, size - 4 * YMM_SZ, offset);
        }
        return ret;
    }

    // Handle overlapping memory blocks
    if ((dst < (src + size)) && (src < dst)) //Backward Copy
    {
        y7 = _mm256_loadu_si256(src + 3 * YMM_SZ);
        y6 = _mm256_loadu_si256(src + 2 * YMM_SZ);
        y5 = _mm256_loadu_si256(src + 1 * YMM_SZ);
        y4 = _mm256_loadu_si256(src + 0 * YMM_SZ);
        __unaligned_load_and_store_4ymm_vec_loop_bkwd(dst, src, size, 4 * YMM_SZ);
        //copy first 4VECs to avoid override
        _mm256_storeu_si256(dst +  3 * YMM_SZ, y7);
        _mm256_storeu_si256(dst +  2 * YMM_SZ, y6);
        _mm256_storeu_si256(dst +  1 * YMM_SZ, y5);
        _mm256_storeu_si256(dst +  0 * YMM_SZ, y4);
    }
    else //Forward Copy
    {
        y4 = _mm256_loadu_si256(src + size - 4 * YMM_SZ);
        y5 = _mm256_loadu_si256(src + size - 3 * YMM_SZ);
        y6 = _mm256_loadu_si256(src + size - 2 * YMM_SZ);
        y7 = _mm256_loadu_si256(src + size - 1 * YMM_SZ);
        __unaligned_load_and_store_4ymm_vec_loop(dst, src, size - 4 * YMM_SZ, 0);
        //copy last 4VECs to avoid override
        _mm256_storeu_si256(dst + size - 4 * YMM_SZ, y4);
        _mm256_storeu_si256(dst + size - 3 * YMM_SZ, y5);
        _mm256_storeu_si256(dst + size - 2 * YMM_SZ, y6);
        _mm256_storeu_si256(dst + size - 1 * YMM_SZ, y7);
    }
    return ret;
}
