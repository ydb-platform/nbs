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
#include <immintrin.h>
#include <stddef.h>

extern cpu_info zen_info;

static inline void *_memset_avx512(void *mem, int val, size_t size)
{
    __m512i z0;
    size_t offset;
    __mmask64 mask;

    z0 = _mm512_set1_epi8(val);
    if (size <= ZMM_SZ)
    {
        if (size)
        {
            mask = ((uint64_t)-1) >> (ZMM_SZ - size);
            _mm512_mask_storeu_epi8(mem, mask, z0);
        }
        return mem;
    }

    _mm512_storeu_si512(mem , z0);
    _mm512_storeu_si512(mem + size - ZMM_SZ, z0);

    if (size <= 2 * ZMM_SZ)
        return mem;

    _mm512_storeu_si512(mem + ZMM_SZ, z0);
    _mm512_storeu_si512(mem + size - 2 * ZMM_SZ, z0);

    if (size <= 4 * ZMM_SZ)
        return mem;

    _mm512_storeu_si512(mem + 2 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + 3 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + size - 4 * ZMM_SZ, z0);
    _mm512_storeu_si512(mem + size - 3 * ZMM_SZ, z0);

    if (size <= 8 * ZMM_SZ)
        return mem;

    size -= 4 * ZMM_SZ;
    offset = 4 * ZMM_SZ - ((uintptr_t)mem & (ZMM_SZ - 1));

    while (offset < size)
    {
        _mm512_store_si512(mem + offset + 0 * ZMM_SZ, z0);
        _mm512_store_si512(mem + offset + 1 * ZMM_SZ, z0);
        _mm512_store_si512(mem + offset + 2 * ZMM_SZ, z0);
        _mm512_store_si512(mem + offset + 3 * ZMM_SZ, z0);
        offset += 4 * ZMM_SZ;
    }

    return mem;
}
