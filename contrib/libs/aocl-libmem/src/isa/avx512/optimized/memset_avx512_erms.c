/* Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
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
#include "../../../base_impls/memset_erms_impls.h"

/* Large tier, kept OUT-OF-LINE (noinline+cold) on purpose: __memset_zen5 is
 * __attribute__((flatten)), so an inlined __erms_stosb would drag its rdi/rcx/rax
 * clobbers (rax is the pinned return reg) into the hot path and cost ~15-25% on
 * <=256B fills.  Isolating it keeps the hot path lean; large sizes pay one call.
 *
 * ERMS rep-stosb covers the whole large range (no non-temporal tier): on Zen5,
 * NT stores measured ~6% slower than rep-stosb at 64MB, and rep-stosb hits the
 * single-core memory-bandwidth ceiling for all sizes above L2. */
static void __attribute__((noinline, cold))
_memset_avx512_erms_rep(void *mem, int val, size_t size)
{
    /* start past the first 4 vecs already written by the caller; the [size-256,size)
     * overlap stores cover the tail, so only the interior needs filling */
    size_t offset = 4 * ZMM_SZ - ((uintptr_t)mem & (ZMM_SZ - 1));
    __erms_stosb((char *)mem + offset, val, size - offset);
}

/* Hybrid memset: masked stores <=64B, overlapping unaligned stores up to 512B,
 * inline temporal aligned loop up to L2, out-of-line rep-stosb above. */
static inline void *_memset_avx512_erms(void *mem, int val, size_t size)
{
    register void *ret asm("rax") = mem;
    __m512i z0 = _mm512_set1_epi8((char)val);

    if (likely(size <= ZMM_SZ)) {
        if (size <= YMM_SZ) {
            __mmask32 mask = _bzhi_u32((uint32_t)-1, (uint32_t)size);
            /* Masked YMM (256-bit): uses less store-port bandwidth than a 512-bit
             * masked store, favouring streaming small fills.  Reuse z0's low half
             * (free cast). */
            _mm256_mask_storeu_epi8(mem, mask, _mm512_castsi512_si256(z0));
            return ret;
        }
        __mmask64 mask = _bzhi_u64((uint64_t)-1, (uint8_t)size);
        _mm512_mask_storeu_epi8(mem, mask, z0);
        return ret;
    }

    /* mid: overlapping unaligned stores up to 512B */
    _mm512_storeu_si512((__m512i *)mem, z0);
    _mm512_storeu_si512((__m512i *)((char *)mem + size - ZMM_SZ), z0);
    if (size <= 2 * ZMM_SZ)
        return ret;

    _mm512_storeu_si512((__m512i *)((char *)mem + ZMM_SZ), z0);
    _mm512_storeu_si512((__m512i *)((char *)mem + size - 2 * ZMM_SZ), z0);
    if (size <= 4 * ZMM_SZ)
        return ret;

    _mm512_storeu_si512((__m512i *)((char *)mem + 2 * ZMM_SZ), z0);
    _mm512_storeu_si512((__m512i *)((char *)mem + 3 * ZMM_SZ), z0);
    _mm512_storeu_si512((__m512i *)((char *)mem + size - 4 * ZMM_SZ), z0);
    _mm512_storeu_si512((__m512i *)((char *)mem + size - 3 * ZMM_SZ), z0);
    if (size <= 8 * ZMM_SZ)
        return ret;

    /* temporal aligned loop (512B .. L2), stays inline */
    if (size < __repstore_start_threshold)
    {
        size_t offset = 4 * ZMM_SZ - ((uintptr_t)mem & (ZMM_SZ - 1));
        /* fill only the interior [256, size-256): the overlap stores above already
         * wrote head and tail, so stop short of the tail to avoid re-writing it */
        while (offset < size - 4 * ZMM_SZ)
        {
            _mm512_store_si512((__m512i *)((char *)mem + offset + 0 * ZMM_SZ), z0);
            _mm512_store_si512((__m512i *)((char *)mem + offset + 1 * ZMM_SZ), z0);
            _mm512_store_si512((__m512i *)((char *)mem + offset + 2 * ZMM_SZ), z0);
            _mm512_store_si512((__m512i *)((char *)mem + offset + 3 * ZMM_SZ), z0);
            offset += 4 * ZMM_SZ;
        }
        return ret;
    }

    /* large (>L2): out-of-line rep-stosb */
    _memset_avx512_erms_rep(mem, val, size);
    return ret;
}
