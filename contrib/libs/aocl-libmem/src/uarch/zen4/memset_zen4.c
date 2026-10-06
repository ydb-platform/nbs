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

#include "logger.h"
#include "threshold.h"
#include "zen_cpu_info.h"
#include "almem_defs.h"
#include <stddef.h>
#include <immintrin.h>

extern cpu_info zen_info;

HIDDEN_SYMBOL void * __attribute__((flatten)) __memset_zen4(void * mem, int  val, size_t size)
{
    __m512i z0;
    __m256i y0;
    size_t offset;
    register void *ret asm("rax");
    ret = mem;

    LOG_INFO("\n");
    z0 =  _mm512_set1_epi8(val);

    if (likely(size <= ZMM_SZ))
    {
        __mmask64 mask64 = _bzhi_u64((uint64_t)-1, (uint8_t)size);
        _mm512_mask_storeu_epi8(mem, mask64, z0);
        return ret;
    }

    else if (size <= 4 * ZMM_SZ)
    {
        _mm512_storeu_si512(mem , z0);
        if (size > 2 * ZMM_SZ)
        {
            _mm512_storeu_si512(mem + ZMM_SZ, z0);
            _mm512_storeu_si512(mem + size - 2 * ZMM_SZ, z0);
            _mm512_storeu_si512(mem + size - ZMM_SZ, z0);
            return ret;
        }

        _mm512_storeu_si512(mem + size - ZMM_SZ, z0);
        return ret;
    }
    // store first 4xVECs irrespective of alignment
    _mm512_storeu_si512(mem , z0);
    _mm512_storeu_si512(mem + ZMM_SZ, z0);
    _mm512_storeu_si512(mem + 2 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + 3 * ZMM_SZ, z0);

    offset = 4 * ZMM_SZ;
    if (size > 8 * ZMM_SZ)
    {
        // adjust offset to vector alignment
        offset -= ((uintptr_t)mem & (ZMM_SZ - 1));

        if (size < zen_info.zen_cache_info.l2_per_core)//L2 Cache Size
        {
            y0 = _mm256_set1_epi8(val);
            do
            {
                _mm256_store_si256(mem + offset + 0 * YMM_SZ, y0);
                _mm256_store_si256(mem + offset + 1 * YMM_SZ, y0);
                _mm256_store_si256(mem + offset + 2 * YMM_SZ, y0);
                _mm256_store_si256(mem + offset + 3 * YMM_SZ, y0);
                _mm256_store_si256(mem + offset + 4 * YMM_SZ, y0);
                _mm256_store_si256(mem + offset + 5 * YMM_SZ, y0);
                _mm256_store_si256(mem + offset + 6 * YMM_SZ, y0);
                _mm256_store_si256(mem + offset + 7 * YMM_SZ, y0);
                offset += 4 * ZMM_SZ;
            } while (offset < (size - 4 * ZMM_SZ));
        }
        else
        // ZMM temporal stores for sizes above L2 cache per core
        {
            do
            {
                _mm512_store_si512(mem + offset + 0 * ZMM_SZ, z0);
                _mm512_store_si512(mem + offset + 1 * ZMM_SZ, z0);
                _mm512_store_si512(mem + offset + 2 * ZMM_SZ, z0);
                _mm512_store_si512(mem + offset + 3 * ZMM_SZ, z0);
                offset += 4 * ZMM_SZ;
            } while (offset < (size - 4 * ZMM_SZ));
        }
        if (offset == size)
            return ret;
    }

    uint16_t rem_data = size - offset;
    // data vector blocks to be stored
    uint8_t rem_vecs = ((rem_data) >> 6) + !!(rem_data & (0x3F));
    // store blocks based on leftout vectors
    switch (rem_vecs)
    {
        case 4:
            _mm512_storeu_si512(mem + size - 4 * ZMM_SZ, z0);
            __attribute__ ((fallthrough));
        case 3:
            _mm512_storeu_si512(mem + size - 3 * ZMM_SZ, z0);
            __attribute__ ((fallthrough));
        case 2:
            _mm512_storeu_si512(mem + size - 2 * ZMM_SZ, z0);
            __attribute__ ((fallthrough));
        case 1:
            _mm512_storeu_si512(mem + size - ZMM_SZ, z0);
    }
    return ret;
}

#ifndef ALMEM_DYN_DISPATCH
void *memset(void *, int, size_t) __attribute__((weak,
                        alias("__memset_zen4")));
#endif
