/* Copyright (C) 2024-26 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef MEMCPY_IMPL_AVX512_H
#define MEMCPY_IMPL_AVX512_H

#include "../../../base_impls/load_store_erms_impls.h"
#include "../../../base_impls/load_store_impls.h"
#include "almem_defs.h"
#include "threshold.h"
#include "zen_cpu_info.h"

extern cpu_info zen_info;

/*
 * AVX-512 memcpy/memmove with tiered head/tail, aligned loop, ERMS, and NT:
 *   <=64B (memcpy):     masked AVX-512 load/store
 *   <=64B (memmove):    scalar/SSE/AVX head-tail (store-forwarding safe)
 *   65..128B:           2-vec head/tail
 *   129..256B:          4-vec head/tail
 *   257..512B:          8-vec head/tail
 *   513..1024B (move):  16-vec head/tail
 *   >512/1024B:         overlap check (memmove only), then (in check order):
 *     < repmov_start:   dst-aligned 4-vec temporal loop (hot path)
 *     >= nt_start:      NT streaming stores   (all variants)
 *     else:             ERMS (rep movsb)      (repmov_start <= size < nt_start,
 *                                              ERMS variants only)
 *
 * memcpy:  __restrict (no overlap handling)
 * memmove: overlap-aware forward/backward via MEMMOVE_AVX512_ERMS guard
 */

#if defined(MEMMOVE_AVX512_ERMS) || defined(MEMMOVE_AVX512)
static inline void *_memcpy_avx512_erms(void *dst, const void *src, size_t size)
#else
static inline void *_memcpy_avx512_erms(void *__restrict dst, const void *__restrict src, size_t size)
#endif
{
    register void *ret __asm__("rax") = dst;

#if defined(MEMMOVE_AVX512_ERMS) || defined(MEMMOVE_AVX512)
    if (likely(size <= XMM_SZ))
    {
        __load_store_ble_scalar_head_tail(dst, src, size);
        return ret;
    }
    if (likely(size <= YMM_SZ))
    {
        __load_store_ble_sse_head_tail(dst, src, size);
        return ret;
    }
    if (likely(size <= ZMM_SZ))
    {
        __load_store_ble_avx_head_tail(dst, src, size);
        return ret;
    }
#else
    if (likely(size <= 1 * ZMM_SZ))
    {
        __load_store_ble_zmm_vec(dst, src, size);
        return ret;
    }
#endif
    if (likely(size <= 2 * ZMM_SZ))
    {
        __load_store_le_2zmm_vec(dst, src, size);
        return ret;
    }
    if (size <= 4 * ZMM_SZ)
    {
        __load_store_le_4zmm_vec(dst, src, size);
        return ret;
    }
    if (size <= 8 * ZMM_SZ)
    {
        __load_store_le_8zmm_vec(dst, src, size);
        return ret;
    }

#if defined(MEMMOVE_AVX512_ERMS) || defined(MEMMOVE_AVX512)
    if (size <= 16 * ZMM_SZ)
    {
        __load_store_le_16zmm_vec(dst, src, size);
        return ret;
    }

    if (unlikely(REGIONS_OVERLAP(dst, src, size)))
    {
        if ((const char *) src < (char *) dst)
        {
            OVERLAP_SAVE_HEAD_4VEC(AVX512, src);
            OVERLAP_SAVE_TAIL_1VEC(AVX512, src, size);

            size_t tail_misalign = ((size_t) dst + size) & (ZMM_SZ - 1);
            if (tail_misalign == 0)
                tail_misalign = ZMM_SZ;
            size_t bk = size - tail_misalign;

            if (unlikely(size >= __nt_start_threshold))
            {
                while (bk > 4 * ZMM_SZ)
                {
                    bk -= 4 * ZMM_SZ;
                    __unaligned_load_nt_store_4zmm_vec((char *) dst + bk, (const char *) src + bk);
                }
                ALM_STORE_FENCE()
            }
            else
            {
                while (bk > 4 * ZMM_SZ)
                {
                    bk -= 4 * ZMM_SZ;
                    __unaligned_load_aligned_store_4zmm_vec((char *) dst + bk, (const char *) src + bk);
                }
            }
            OVERLAP_RESTORE_TAIL1_HEAD4(AVX512, dst, size);
        }
        else
        {
            OVERLAP_SAVE_HEAD_1VEC(AVX512, src);
            OVERLAP_SAVE_TAIL_4VEC(AVX512, src, size);

            const char *sp = (const char *) src;
            char *dp = (char *) dst;
            size_t rem;
            ALIGN_ADVANCE_64(sp, dp, size, rem);

            if (unlikely(size >= __nt_start_threshold))
            {
                while (rem >= 4 * ZMM_SZ)
                {
                    __unaligned_load_nt_store_4zmm_vec(dp, sp);
                    ADVANCE_4VEC(AVX512, sp, dp, rem);
                }
                ALM_STORE_FENCE()
            }
            else
            {
                while (rem >= 4 * ZMM_SZ)
                {
                    __unaligned_load_aligned_store_4zmm_vec(dp, sp);
                    ADVANCE_4VEC(AVX512, sp, dp, rem);
                }
            }
            OVERLAP_RESTORE_TAIL4_HEAD1(AVX512, dst, size);
        }
        return ret;
    }
#endif

    /* dst-aligned temporal loop - hoisted ahead of the NT tier so the common
     * case (513B..repmov_start) takes a single threshold compare on the hot
     * path; the NT compare moves to the cold path below.
     *
     * Correctness: sound only while __repmov_start_threshold <=
     * __nt_start_threshold. On ERMS parts that always holds (26KB <= L3). If
     * ERMS is ever absent, __repmov_start_threshold == REPMOV_DISABLED
     * (UINT64_MAX) makes this guard always-true and would shadow the NT tier,
     * so the NT tier below MUST remain reachable in that configuration. */
    if (likely(size < __repmov_start_threshold))
    {
        const char *sp = (const char *) src;
        char *dp = (char *) dst;

        __unaligned_load_store_1zmm_vec(dp, sp);

        size_t rem;
        ALIGN_ADVANCE_64(sp, dp, size, rem);

        while (rem > 4 * ZMM_SZ)
        {
            __unaligned_load_aligned_store_4zmm_vec(dp, sp);
            ADVANCE_4VEC(AVX512, sp, dp, rem);
        }

        __unaligned_load_store_4zmm_vec((char *) dst + size - 4 * ZMM_SZ, (const char *) src + size - 4 * ZMM_SZ);
        return ret;
    }

    /* NT streaming tier (all variants: NT outperforms temporal regardless of ERMS) */
    if (unlikely(size >= __nt_start_threshold))
    {
        const char *sp = (const char *) src;
        char *dp = (char *) dst;

        __unaligned_load_store_1zmm_vec(dp, sp);

        size_t rem;
        ALIGN_ADVANCE_64(sp, dp, size, rem);

        while (rem > 4 * ZMM_SZ)
        {
            __unaligned_load_nt_store_4zmm_vec(dp, sp);
            ADVANCE_4VEC(AVX512, sp, dp, rem);
        }
        ALM_STORE_FENCE()

        __unaligned_load_store_4zmm_vec((char *) dst + size - 4 * ZMM_SZ, (const char *) src + size - 4 * ZMM_SZ);
        return ret;
    }

    /* ERMS tier (rep movsb) - repmov_start <= size < nt_start */
    __erms_movsb((void *) dst, src, size);
    return ret;
}

/* The rep movsb tier is runtime threshold-gated, so the ERMS and non-ERMS
 * entry points share a single body. Provide the plain _memcpy_avx512 name for
 * the generic (non-uarch) dispatch sources. */
#define _memcpy_avx512 _memcpy_avx512_erms

#ifdef MEMMOVE_AVX512
#undef MEMMOVE_AVX512
#endif

#ifdef MEMMOVE_AVX512_ERMS
#undef MEMMOVE_AVX512_ERMS
#endif

#endif /* MEMCPY_IMPL_AVX512_H */
