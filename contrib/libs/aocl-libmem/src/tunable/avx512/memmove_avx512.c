/* Copyright (C) 2022-24 Advanced Micro Devices, Inc. All rights reserved.
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
#include "zen_cpu_info.h"
#include "../../base_impls/load_store_impls.h"

static inline void * __memmove_ble_8zmm(void *dst, const void *src, size_t size)
{
    if (size <= 2 * ZMM_SZ)
        return __load_store_ble_2zmm_vec_overlap(dst, src, size);

    if (size <= 4 * ZMM_SZ)
    {
        __load_store_le_4zmm_vec(dst, src, size);
        return dst;
    }
    __load_store_le_8zmm_vec(dst, src, size);
    return dst;
}


void *__memmove_avx512_unaligned(void *dst, const void *src, size_t size)
{
    __m512i y4, y5, y6, y7;
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memmove_ble_8zmm(dst, src, size);

    //Overlap check to set the copy direction
    if ((dst < (src + size)) && (src < dst)) //Backward Copy
    {
        y7 = _mm512_loadu_si512(src + 3 * ZMM_SZ);
        y6 = _mm512_loadu_si512(src + 2 * ZMM_SZ);
        y5 = _mm512_loadu_si512(src + 1 * ZMM_SZ);
        y4 = _mm512_loadu_si512(src + 0 * ZMM_SZ);
        __unaligned_load_and_store_4zmm_vec_loop_bkwd\
                            (dst, src, size, 4 * ZMM_SZ);
        //copy first 4VECs to avoid override
        _mm512_storeu_si512(dst +  3 * ZMM_SZ, y7);
        _mm512_storeu_si512(dst +  2 * ZMM_SZ, y6);
        _mm512_storeu_si512(dst +  1 * ZMM_SZ, y5);
        _mm512_storeu_si512(dst +  0 * ZMM_SZ, y4);
    }
    else
    {
        y4 = _mm512_loadu_si512(src + size - 4 * ZMM_SZ);
        y5 = _mm512_loadu_si512(src + size - 3 * ZMM_SZ);
        y6 = _mm512_loadu_si512(src + size - 2 * ZMM_SZ);
        y7 = _mm512_loadu_si512(src + size - 1 * ZMM_SZ);
        __unaligned_load_and_store_4zmm_vec_loop\
                        (dst, src, size - 4 * ZMM_SZ, 0);
        //copy last 4VECs to avoid override
        _mm512_storeu_si512(dst + size - 4 * ZMM_SZ, y4);
        _mm512_storeu_si512(dst + size - 3 * ZMM_SZ, y5);
        _mm512_storeu_si512(dst + size - 2 * ZMM_SZ, y6);
        _mm512_storeu_si512(dst + size - 1 * ZMM_SZ, y7);
    }
    return dst;
}

void *__memmove_avx512_aligned(void *dst, const void *src, size_t size)
{
    __m512i y4, y5, y6, y7, y8;
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memmove_ble_8zmm(dst, src, size);

    //Overlap check to set the copy direction
    if ((dst < (src + size)) && (src < dst)) //Backward COpy
    {
        y7 = _mm512_load_si512(src + 3 * ZMM_SZ);
        y6 = _mm512_load_si512(src + 2 * ZMM_SZ);
        y5 = _mm512_load_si512(src + 1 * ZMM_SZ);
        y4 = _mm512_load_si512(src + 0 * ZMM_SZ);
        //load the last VEC to handle size not multiple of the vec.
        y8 = _mm512_loadu_si512(src + size - ZMM_SZ);
        __aligned_load_and_store_4zmm_vec_loop_bkwd\
                        (dst, src, size & ~(ZMM_SZ-1), 4 * ZMM_SZ);
        //store the last VEC to handle size not multiple of the vec.
        _mm512_storeu_si512(dst + size - ZMM_SZ, y8);
        //copy first 4VECs to avoid override
        _mm512_store_si512(dst +  3 * ZMM_SZ, y7);
        _mm512_store_si512(dst +  2 * ZMM_SZ, y6);
        _mm512_store_si512(dst +  1 * ZMM_SZ, y5);
        _mm512_store_si512(dst +  0 * ZMM_SZ, y4);
    }
    else //Forward Copy
    {
        y4 = _mm512_loadu_si512(src + size - 4 * ZMM_SZ);
        y5 = _mm512_loadu_si512(src + size - 3 * ZMM_SZ);
        y6 = _mm512_loadu_si512(src + size - 2 * ZMM_SZ);
        y7 = _mm512_loadu_si512(src + size - 1 * ZMM_SZ);
        __aligned_load_and_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, 0);
        //copy last 4VECs to avoid override
        _mm512_storeu_si512(dst + size - 4 * ZMM_SZ, y4);
        _mm512_storeu_si512(dst + size - 3 * ZMM_SZ, y5);
        _mm512_storeu_si512(dst + size - 2 * ZMM_SZ, y6);
        _mm512_storeu_si512(dst + size - 1 * ZMM_SZ, y7);
    }
    return dst;
}

void *__memmove_avx512_aligned_load(void *dst, const void *src, size_t size)
{
    __m512i y4, y5, y6, y7, y8;
    size_t offset = 0;

    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memmove_ble_8zmm(dst, src, size);

    //Overlap check to set the copy direction
    if ((dst < (src + size)) && (src < dst)) //Backward Copy
    {
         offset = (size_t)(src + size) & (ZMM_SZ - 1);
         y4 = _mm512_loadu_si512(src + 3 * ZMM_SZ);
         y5 = _mm512_loadu_si512(src + 2 * ZMM_SZ);
         y6 = _mm512_loadu_si512(src + 1 * ZMM_SZ);
         y7 = _mm512_loadu_si512(src + 0 * ZMM_SZ);
         //load the last VEC to handle size not multiple of the vec.
         y8 = _mm512_loadu_si512(src + size - ZMM_SZ);
         __aligned_load_unaligned_store_4zmm_vec_loop_bkwd\
                            (dst, src, size - offset, 4 * ZMM_SZ);
         //store the last VEC to handle size not multiple of the vec.
         _mm512_storeu_si512(dst + size - ZMM_SZ, y8);
         _mm512_storeu_si512(dst +  3 * ZMM_SZ, y4);
         _mm512_storeu_si512(dst +  2 * ZMM_SZ, y5);
         _mm512_storeu_si512(dst +  1 * ZMM_SZ, y6);
         _mm512_storeu_si512(dst +  0 * ZMM_SZ, y7);
    }
    else
    {
        //compute the offset to align the src to ZMM_SZ Bytes boundary
        offset = ZMM_SZ - ((size_t)src & (ZMM_SZ - 1));
        y4 = _mm512_loadu_si512(src + size - 4 * ZMM_SZ);
        y5 = _mm512_loadu_si512(src + size - 3 * ZMM_SZ);
        y6 = _mm512_loadu_si512(src + size - 2 * ZMM_SZ);
        y7 = _mm512_loadu_si512(src + size - 1 * ZMM_SZ);
         //load the first VEC to handle unaligned src address.
        y8 = _mm512_loadu_si512(src);
        __aligned_load_unaligned_store_4zmm_vec_loop\
                        (dst, src, size - 4 * ZMM_SZ, offset);
         //store the first VEC to handle unaligned src address.
         _mm512_storeu_si512(dst, y8);
        //copy last 4VECs to avoid override
        _mm512_storeu_si512(dst + size - 4 * ZMM_SZ, y4);
        _mm512_storeu_si512(dst + size - 3 * ZMM_SZ, y5);
        _mm512_storeu_si512(dst + size - 2 * ZMM_SZ, y6);
        _mm512_storeu_si512(dst + size - 1 * ZMM_SZ, y7);
    }
    return dst;
}

void *__memmove_avx512_aligned_store(void *dst, const void *src, size_t size)
{
    __m512i y4, y5, y6, y7, y8;
    size_t offset = 0;

    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memmove_ble_8zmm(dst, src, size);

    //Overlap check to set the copy direction
    if ((dst < (src + size)) && (src < dst)) //Backward Copy
    {
         offset = (size_t)(dst + size) & (ZMM_SZ - 1);
         y4 = _mm512_loadu_si512(src + 3 * ZMM_SZ);
         y5 = _mm512_loadu_si512(src + 2 * ZMM_SZ);
         y6 = _mm512_loadu_si512(src + 1 * ZMM_SZ);
         y7 = _mm512_loadu_si512(src + 0 * ZMM_SZ);
         //load the last VEC to handle size not multiple of the vec.
         y8 = _mm512_loadu_si512(src + size - ZMM_SZ);
         __unaligned_load_aligned_store_4zmm_vec_loop_bkwd\
                            (dst, src, size - offset, 4 * ZMM_SZ);
         //store the last VEC to handle size not multiple of the vec.
         _mm512_storeu_si512(dst + size - ZMM_SZ, y8);
         _mm512_storeu_si512(dst +  3 * ZMM_SZ, y4);
         _mm512_storeu_si512(dst +  2 * ZMM_SZ, y5);
         _mm512_storeu_si512(dst +  1 * ZMM_SZ, y6);
         _mm512_storeu_si512(dst +  0 * ZMM_SZ, y7);
    }
    else
    {
        //compute the offset to align the src to ZMM_SZ Bytes boundary
        offset = ZMM_SZ - ((size_t)dst & (ZMM_SZ - 1));
        y4 = _mm512_loadu_si512(src + size - 4 * ZMM_SZ);
        y5 = _mm512_loadu_si512(src + size - 3 * ZMM_SZ);
        y6 = _mm512_loadu_si512(src + size - 2 * ZMM_SZ);
        y7 = _mm512_loadu_si512(src + size - 1 * ZMM_SZ);
         //load the first VEC to handle unaligned src address.
        y8 = _mm512_loadu_si512(src);
        __unaligned_load_aligned_store_4zmm_vec_loop\
                        (dst, src, size - 4 * ZMM_SZ, offset);
         //store the first VEC to handle unaligned src address.
         _mm512_storeu_si512(dst, y8);
        //copy last 4VECs to avoid override
        _mm512_storeu_si512(dst + size - 4 * ZMM_SZ, y4);
        _mm512_storeu_si512(dst + size - 3 * ZMM_SZ, y5);
        _mm512_storeu_si512(dst + size - 2 * ZMM_SZ, y6);
        _mm512_storeu_si512(dst + size - 1 * ZMM_SZ, y7);
    }
    return dst;
}

void *__memmove_avx512_nt(void *dst, const void *src, size_t size)
{
    __m512i y4, y5, y6, y7;
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memmove_ble_8zmm(dst, src, size);

    /* Non-temporal operations use WC Buffers to store stream writes and
     * data is written back to memory only after the entire buffer line fill.
     * Any interruptions/evictions from SMP/SMT environment may delay writes,
     * thus leads to data corruptions for overlapped memory buffers.
     * Henceforth, avoid non-temporal operations on overlapped memory buffers.
     */
    if (!(((src + size) < dst) || ((dst + size) < src)))
        return __memmove_avx512_aligned(dst, src, size);

    y4 = _mm512_loadu_si512(src + size - 4 * ZMM_SZ);
    y5 = _mm512_loadu_si512(src + size - 3 * ZMM_SZ);
    y6 = _mm512_loadu_si512(src + size - 2 * ZMM_SZ);
    y7 = _mm512_loadu_si512(src + size - 1 * ZMM_SZ);
    __nt_load_and_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, 0);
    //copy last 4VECs to avoid override
    _mm512_storeu_si512(dst + size - 4 * ZMM_SZ, y4);
    _mm512_storeu_si512(dst + size - 3 * ZMM_SZ, y5);
    _mm512_storeu_si512(dst + size - 2 * ZMM_SZ, y6);
    _mm512_storeu_si512(dst + size - 1 * ZMM_SZ, y7);
    return dst;
}

void *__memmove_avx512_nt_load(void *dst, const void *src, size_t size)
{
    __m512i y4, y5, y6, y7, y8;
    size_t offset;
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memmove_ble_8zmm(dst, src, size);

    /* Non-temporal operations use WC Buffers to store stream writes and
     * data is written back to memory only after the entire buffer line fill.
     * Any interruptions/evictions from SMP/SMT environment may delay writes,
     * thus leads to data corruptions for overlapped memory buffers.
     * Henceforth, avoid non-temporal operations on overlapped memory buffers.
     */
    if (!(((src + size) < dst) || ((dst + size) < src)))
        return __memmove_avx512_aligned_load(dst, src, size);

    //compute the offset to align the src to ZMM_SZ Bytes boundary
    offset = ZMM_SZ - ((size_t)src & (ZMM_SZ - 1));
    y4 = _mm512_loadu_si512(src + size - 4 * ZMM_SZ);
    y5 = _mm512_loadu_si512(src + size - 3 * ZMM_SZ);
    y6 = _mm512_loadu_si512(src + size - 2 * ZMM_SZ);
    y7 = _mm512_loadu_si512(src + size - 1 * ZMM_SZ);
    //load the first VEC to handle unaligned src address.
    y8 = _mm512_loadu_si512(src);
    __nt_load_unaligned_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, offset);
    //store the first VEC to handle unaligned src address.
    _mm512_storeu_si512(dst, y8);
    //copy last 4VECs to avoid override
    _mm512_storeu_si512(dst + size - 4 * ZMM_SZ, y4);
    _mm512_storeu_si512(dst + size - 3 * ZMM_SZ, y5);
    _mm512_storeu_si512(dst + size - 2 * ZMM_SZ, y6);
    _mm512_storeu_si512(dst + size - 1 * ZMM_SZ, y7);
    return dst;
}


void *__memmove_avx512_nt_store(void *dst, const void *src, size_t size)
{
    __m512i y4, y5, y6, y7, y8;
    size_t offset;
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memmove_ble_8zmm(dst, src, size);

    /* Non-temporal operations use WC Buffers to store stream writes and
     * data is written back to memory only after the entire buffer line fill.
     * Any interruptions/evictions from SMP/SMT environment may delay writes,
     * thus leads to data corruptions for overlapped memory buffers.
     * Henceforth, avoid non-temporal operations on overlapped memory buffers.
     */
    if (!(((src + size) < dst) || ((dst + size) < src)))
        return __memmove_avx512_aligned_store(dst, src, size);

    //compute the offset to align the src to ZMM_SZ Bytes boundary
    offset = ZMM_SZ - ((size_t)dst & (ZMM_SZ - 1));
    y4 = _mm512_loadu_si512(src + size - 4 * ZMM_SZ);
    y5 = _mm512_loadu_si512(src + size - 3 * ZMM_SZ);
    y6 = _mm512_loadu_si512(src + size - 2 * ZMM_SZ);
    y7 = _mm512_loadu_si512(src + size - 1 * ZMM_SZ);
    //load the first VEC to handle unaligned src address.
    y8 = _mm512_loadu_si512(src);
    __unaligned_load_nt_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, offset);
    //store the first VEC to handle unaligned src address.
    _mm512_storeu_si512(dst, y8);
    //copy last 4VECs to avoid override
    _mm512_storeu_si512(dst + size - 4 * ZMM_SZ, y4);
    _mm512_storeu_si512(dst + size - 3 * ZMM_SZ, y5);
    _mm512_storeu_si512(dst + size - 2 * ZMM_SZ, y6);
    _mm512_storeu_si512(dst + size - 1 * ZMM_SZ, y7);
    return dst;
}
