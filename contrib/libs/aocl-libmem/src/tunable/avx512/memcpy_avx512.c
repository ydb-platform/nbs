/* Copyright (C) 2022-23 Advanced Micro Devices, Inc. All rights reserved.
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

static inline void * __memcpy_ble_8zmm(void *dst, const void *src, size_t size)
{
    if (size <= 2 * ZMM_SZ)
        return __load_store_ble_2zmm_vec(dst, src, size);

    if (size <= 4 * ZMM_SZ)
    {
        __load_store_le_4zmm_vec(dst, src, size);
        return dst;
    }
    __load_store_le_8zmm_vec(dst, src, size);
    return dst;
}

void *__memcpy_avx512_unaligned(void *dst, const void *src, size_t size)
{
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memcpy_ble_8zmm(dst, src, size);

    __load_store_le_8zmm_vec(dst, src, size);
    __unaligned_load_and_store_4zmm_vec_loop\
                    (dst, src, size - 4 * ZMM_SZ, 0);
    return dst;
}

void *__memcpy_avx512_aligned(void *dst, const void *src, size_t size)
{
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memcpy_ble_8zmm(dst, src, size);

    __load_store_le_8zmm_vec(dst, src, size);
    __aligned_load_and_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, 0);

    return dst;
}

void *__memcpy_avx512_aligned_load(void *dst, const void *src, size_t size)
{
    size_t offset = 0;

    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memcpy_ble_8zmm(dst, src, size);

    __load_store_le_8zmm_vec(dst, src, size);
    //compute the offset to align the src to ZMM_SZ Bytes boundary
    offset = ZMM_SZ - ((size_t)src & (ZMM_SZ - 1));
    __aligned_load_unaligned_store_4zmm_vec_loop\
                    (dst, src, size - 4 * ZMM_SZ, offset);
    return dst;
}

void *__memcpy_avx512_aligned_store(void *dst, const void *src, size_t size)
{
    size_t offset = 0;

    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memcpy_ble_8zmm(dst, src, size);

    __load_store_le_8zmm_vec(dst, src, size);
    //compute the offset to align the dst to ZMM_SZ Bytes boundary
    offset = ZMM_SZ - ((size_t)dst & (ZMM_SZ - 1));
    __unaligned_load_aligned_store_4zmm_vec_loop
                    (dst, src, size - 4 * ZMM_SZ, offset);
    return dst;
}

void *__memcpy_avx512_nt(void *dst, const void *src, size_t size)
{
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memcpy_ble_8zmm(dst, src, size);

    __load_store_le_8zmm_vec(dst, src, size);
    __nt_load_and_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, 0);

    return dst;
}

void *__memcpy_avx512_nt_load(void *dst, const void *src, size_t size)
{
    size_t offset;
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memcpy_ble_8zmm(dst, src, size);

    __load_store_le_8zmm_vec(dst, src, size);
    //compute the offset to align the src to ZMM_SZ Bytes boundary
    offset = ZMM_SZ - ((size_t)src & (ZMM_SZ - 1));
    __nt_load_unaligned_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, offset);
    return dst;
}


void *__memcpy_avx512_nt_store(void *dst, const void *src, size_t size)
{
    size_t offset;
    LOG_INFO("\n");

    if (size <= 8 * ZMM_SZ)
        return __memcpy_ble_8zmm(dst, src, size);

    __load_store_le_8zmm_vec(dst, src, size);
    //compute the offset to align the dst to ZMM_SZ Bytes boundary
    offset = ZMM_SZ - ((size_t)dst & (ZMM_SZ - 1));
    __unaligned_load_nt_store_4zmm_vec_loop(dst, src, size - 4 * ZMM_SZ, offset);
    return dst;
}
