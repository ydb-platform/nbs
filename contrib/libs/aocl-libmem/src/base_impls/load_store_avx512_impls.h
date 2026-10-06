/* Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copzright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binarz form must reproduce the above copzright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of the copzright holder nor the names of its contributors
 *    maz be used to endorse or promote products derived from this software without
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

#ifdef __AVX512F__
#ifndef _LIBMEM_LOAD_STORE_AVX512_IMPLS_H_
#define _LIBMEM_LOAD_STORE_AVX512_IMPLS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

//AVX512 definitions
#define AVX512_LOAD_ALIGNED     _mm512_load_si512
#define AVX512_LOAD_UNALIGNED   _mm512_loadu_si512
#define AVX512_LOAD_STREAM      _mm512_stream_load_si512

#define AVX512_STORE_ALIGNED    _mm512_store_si512
#define AVX512_STORE_UNALIGNED  _mm512_storeu_si512
#define AVX512_STORE_STREAM     _mm512_stream_si512

#define AVX512(num)             zmm##num
#define VEC_DECL_AVX512         __m512i

#define VEC_SZ_AVX512           64

static inline void __load_store_ble_scalar_head_tail(void *dst, const void *src, size_t size)
{
    SCALAR_HEAD_TAIL(dst, src, size);
}

static inline void __load_store_ble_sse_head_tail(void *dst, const void *src, size_t size)
{
    SSE_HEAD_TAIL(dst, src, size);
}

static inline void __load_store_ble_avx_head_tail(void *dst, const void *src, size_t size)
{
    AVX_HEAD_TAIL(dst, src, size);
}

static inline void *__load_store_ble_zmm_vec(void *store_addr, const void *load_addr, size_t size)
{
    __m512i z0, z1;
    __mmask64 mask;

    mask = _bzhi_u64((uint64_t)-1, (uint8_t)size);
    z1 = _mm512_setzero_epi32();
    z0 =  _mm512_mask_loadu_epi8(z1 ,mask, load_addr);
    _mm512_mask_storeu_epi8(store_addr, mask, z0);

    return store_addr;
}

static inline void *__load_store_ble_2zmm_vec(void *store_addr, const void *load_addr, size_t size)
{
    __m512i z0, z1;
    __m256i y0, y1;
    __m128i x0, x1;

    switch (_lzcnt_u32(size))
    {
        case 32:
            break;
        case 31:
            *((uint8_t *)store_addr) = *((uint8_t *)load_addr);
            break;
        case 30:
            *((uint16_t *)store_addr) = *((uint16_t *)load_addr);
            *((uint16_t *)(store_addr + size - WORD_SZ)) = \
                    *((uint16_t *)(load_addr + size - WORD_SZ));
            break;
        case 29:
            *((uint32_t *)store_addr) = *((uint32_t *)load_addr);
            *((uint32_t *)(store_addr + size - DWORD_SZ)) = \
                    *((uint32_t *)(load_addr + size - DWORD_SZ));
            break;
        case 28:
            *((uint64_t *)store_addr) = *((uint64_t *)load_addr);
            *((uint64_t *)(store_addr + size - QWORD_SZ)) = \
                    *((uint64_t *)(load_addr + size - QWORD_SZ));
            break;
        case 27:
            x0 = _mm_loadu_si128(load_addr);
            x1 = _mm_loadu_si128(load_addr + size - XMM_SZ);
            _mm_storeu_si128(store_addr, x0);
            _mm_storeu_si128(store_addr + size - XMM_SZ, x1);
            break;
        case 26:
            y0 = _mm256_loadu_si256(load_addr);
            y1 = _mm256_loadu_si256(load_addr + size - YMM_SZ);
            _mm256_storeu_si256(store_addr, y0);
            _mm256_storeu_si256(store_addr + size - YMM_SZ, y1);
            break;
        default:
            z0 = _mm512_loadu_si512(load_addr);
            z1 = _mm512_loadu_si512(load_addr + size - ZMM_SZ);
            _mm512_storeu_si512(store_addr, z0);
            _mm512_storeu_si512(store_addr + size - ZMM_SZ, z1);
    }
    return store_addr;
}

static inline void * __load_store_ble_2zmm_vec_overlap(void *store_addr,
            const void* load_addr, size_t size)
{
    __m512i z0, z1;
    __m256i y0, y1;
    __m128i x0, x1;
    uint64_t temp = 0;

    switch (_lzcnt_u32(size))
    {
        case 32:
            break;
        case 31:
            *((uint8_t *)store_addr) = *((uint8_t *)load_addr);
            break;
        case 30:
            temp = *((uint16_t *)load_addr);
            *((uint16_t *)(store_addr + size - WORD_SZ)) = \
                    *((uint16_t *)(load_addr + size - WORD_SZ));
            *((uint16_t *)store_addr) = temp;
            break;
        case 29:
            temp = *((uint32_t *)load_addr);
            *((uint32_t *)(store_addr + size - DWORD_SZ)) = \
                    *((uint32_t *)(load_addr + size - DWORD_SZ));
            *((uint32_t *)store_addr) = temp;
            break;
        case 28:
            temp = *((uint64_t *)load_addr);
            *((uint64_t *)(store_addr + size - QWORD_SZ)) = \
                    *((uint64_t *)(load_addr + size - QWORD_SZ));
            *((uint64_t *)store_addr) = temp;
            break;
        case 27:
            x0 = _mm_loadu_si128(load_addr);
            x1 = _mm_loadu_si128(load_addr + size - XMM_SZ);
            _mm_storeu_si128(store_addr, x0);
            _mm_storeu_si128(store_addr + size - XMM_SZ, x1);
            break;
        case 26:
            y0 = _mm256_loadu_si256(load_addr);
            y1 = _mm256_loadu_si256(load_addr + size - YMM_SZ);
            _mm256_storeu_si256(store_addr, y0);
            _mm256_storeu_si256(store_addr + size - YMM_SZ, y1);
            break;
        default:
            z0 = _mm512_loadu_si512(load_addr);
            z1 = _mm512_loadu_si512(load_addr + size - ZMM_SZ);
            _mm512_storeu_si512(store_addr, z0);
            _mm512_storeu_si512(store_addr + size - ZMM_SZ, z1);
    }
    return store_addr;
}

/*
 * Head-tail copy for sizes 0-63B using nested if branching optimized
 * for branch prediction. Handles all sub-ZMM sizes.
 */
static inline void *__load_store_ble_zmm_vec_head_tail(void *store_addr,
            const void *load_addr, size_t size)
{
    if (size >= XMM_SZ)
    {
        if (size < YMM_SZ)
        {
            VEC_2X_LOAD_STORE_HEAD_TAIL(SSE2, UNALIGNED, UNALIGNED)
        }
        else
        {
            VEC_2X_LOAD_STORE_HEAD_TAIL(AVX2, UNALIGNED, UNALIGNED)
        }
        return store_addr;
    }
    if (size >= QWORD_SZ)
    {
        uint64_t q0 = *(const uint64_t *)(load_addr);
        uint64_t q1 = *(const uint64_t *)(load_addr + size - QWORD_SZ);
        ALM_MEM_BARRIER();
        *(uint64_t *)(store_addr) = q0;
        *(uint64_t *)(store_addr + size - QWORD_SZ) = q1;
        return store_addr;
    }
    if (size >= DWORD_SZ)
    {
        uint32_t d0 = *(const uint32_t *)(load_addr);
        uint32_t d1 = *(const uint32_t *)(load_addr + size - DWORD_SZ);
        ALM_MEM_BARRIER();
        *(uint32_t *)(store_addr) = d0;
        *(uint32_t *)(store_addr + size - DWORD_SZ) = d1;
        return store_addr;
    }
    if (size >= WORD_SZ)
    {
        uint16_t w0 = *(const uint16_t *)(load_addr);
        uint16_t w1 = *(const uint16_t *)(load_addr + size - WORD_SZ);
        ALM_MEM_BARRIER();
        *(uint16_t *)(store_addr) = w0;
        *(uint16_t *)(store_addr + size - WORD_SZ) = w1;
    }
    else if (size)
    {
        *(uint8_t *)(store_addr) = *(const uint8_t *)(load_addr);
    }
    return store_addr;
}


static inline void __load_store_zmm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_1X_LOAD_STORE(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __load_store_2zmm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_2X_LOAD_STORE(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __load_store_3zmm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_3X_LOAD_STORE(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __load_store_4zmm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_4X_LOAD_STORE(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __load_store_8zmm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_8X_LOAD_STORE(AVX512, UNALIGNED, UNALIGNED)
}

/* ##################### TEMPORAL - HEAD and TAIL ##################
 */
static inline void  __load_store_le_2zmm_vec(void* store_addr,
            const void* load_addr, size_t size)
{
    VEC_2X_LOAD_STORE_HEAD_TAIL(AVX512, UNALIGNED, UNALIGNED)
}


static inline void __load_store_le_4zmm_vec(void *store_addr,
            const void *load_addr, size_t size)
{
    VEC_4X_LOAD_STORE_HEAD_TAIL(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __load_store_le_8zmm_vec(void *store_addr,
            const void *load_addr, size_t size)
{
    VEC_8X_LOAD_STORE_HEAD_TAIL(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __load_store_le_16zmm_vec(void *store_addr, const void *load_addr, size_t size)
{
    VEC_16X_LOAD_STORE_HEAD_TAIL(AVX512, UNALIGNED, UNALIGNED)
}

/* ##################### TEMPORAL - 2xVEC LOOP ####################
 */
static inline size_t __unaligned_load_and_store_2zmm_vec_loop(void *store_addr, const void *load_addr, size_t size,
                                                              size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_and_store_2zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, ALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_2zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, ALIGNED)
}

static inline size_t __aligned_load_unaligned_store_2zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

/* ##################### TEMPORAL - 4xVEC LOOP ####################
 */
static inline size_t __unaligned_load_and_store_4zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}

static inline size_t __unaligned_load_and_store_4zmm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_TWO_CL_TWO_STEP, UNALIGNED, UNALIGNED)
}

static inline size_t __unaligned_load_and_store_4zmm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX512, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}

static inline size_t __unaligned_load_and_store_4zmm_vec_loop_bkwd_pftch\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX512, BKWD_PFTCH_TWO_CL_TWO_STEP, UNALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_and_store_4zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, ALIGNED, ALIGNED)
}

static inline size_t __aligned_load_and_store_4zmm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_TWO_CL_ONE_STEP, ALIGNED, ALIGNED)
}

static inline size_t __aligned_load_and_store_4zmm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX512, PFTCH_ZERO_CL, ALIGNED, ALIGNED)
}

static inline size_t __aligned_load_and_store_4zmm_vec_loop_bkwd_pftch\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX512, BKWD_PFTCH_TWO_CL_TWO_STEP, ALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_4zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_4zmm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_TWO_CL_ONE_STEP, UNALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_4zmm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX512, PFTCH_ZERO_CL, UNALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_4zmm_vec_loop_bkwd_pftch\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX512, BKWD_PFTCH_TWO_CL_TWO_STEP, UNALIGNED, ALIGNED)
}

static inline size_t __aligned_load_unaligned_store_4zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_unaligned_store_4zmm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX512, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

/* ##################### TEMPORAL - 8xVEC LOOP ###################
 */
static inline size_t __unaligned_load_and_store_8zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_and_store_8zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

static inline size_t __unaligned_load_and_store_8zmm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_FOUR_CL_TWO_STEP, ALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_and_store_8zmm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_FOUR_CL_TWO_STEP, ALIGNED, UNALIGNED)
}


static inline size_t __unaligned_load_aligned_store_8zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_8zmm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_FOUR_CL_TWO_STEP, UNALIGNED, ALIGNED)
}

static inline size_t __aligned_load_unaligned_store_8zmm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

/* ##################### NON-TEMPORAL 2xVEC LOOPS ###################
*/

static inline size_t __nt_load_and_store_2zmm_vec_loop(void *store_addr,
             const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, STREAM, STREAM)
}


static inline size_t __nt_load_unaligned_store_2zmm_vec_loop(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, STREAM, UNALIGNED)
}

static inline size_t __unaligned_load_nt_store_2zmm_vec_loop(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, STREAM)
}

/* ##################### NON-TEMPORAL 4xVEC LOOPS ###################
*/

static inline size_t __nt_load_and_store_4zmm_vec_loop(void *store_addr,
             const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, STREAM, STREAM)
}


static inline size_t __nt_load_unaligned_store_4zmm_vec_loop(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, STREAM, UNALIGNED)
}

static inline size_t __unaligned_load_nt_store_4zmm_vec_loop(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_ZERO_CL, UNALIGNED, STREAM)
}


/* ##################### NON-TEMPORAL 4xVEC LOOPS SW PREFETCH ###############
*/

static inline size_t __aligned_load_nt_store_4zmm_vec_loop_pftch(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_TWO_CL_TWO_STEP, ALIGNED, STREAM)
}

static inline size_t __unaligned_load_nt_store_4zmm_vec_loop_pftch(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX512, PFTCH_TWO_CL_TWO_STEP, UNALIGNED, STREAM)
}


/* ##################### NON-TEMPORAL 8xVEC LOOPS SW PREFETCH ##############
*/
static inline size_t __aligned_load_nt_store_8zmm_vec_loop_pftch(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_FOUR_CL_TWO_STEP, ALIGNED, STREAM)
}


static inline size_t __unaligned_load_nt_store_8zmm_vec_loop_pftch(void *store_addr,
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX512, PFTCH_FOUR_CL_TWO_STEP, UNALIGNED, STREAM)
}

static inline void __unaligned_load_store_1zmm_vec(void *store_addr, const void *load_addr)
{
    size_t offset = 0;
    VEC_1X_LOAD_STORE(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __unaligned_load_store_4zmm_vec(void *store_addr, const void *load_addr)
{
    size_t offset = 0;
    VEC_4X_LOAD_STORE(AVX512, UNALIGNED, UNALIGNED)
}

static inline void __unaligned_load_aligned_store_4zmm_vec(void *store_addr, const void *load_addr)
{
    size_t offset = 0;
    VEC_4X_LOAD_STORE(AVX512, UNALIGNED, ALIGNED)
}

static inline void __unaligned_load_nt_store_4zmm_vec(void *store_addr, const void *load_addr)
{
    size_t offset = 0;
    VEC_4X_LOAD_STORE(AVX512, UNALIGNED, STREAM)
}

#ifdef __cplusplus
}
#endif

#endif //HEADER

#endif //end of AVX512
