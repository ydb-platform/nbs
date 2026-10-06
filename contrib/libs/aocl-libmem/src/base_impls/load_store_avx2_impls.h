/* Copyright (C) 2023-25 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef _LIBMEM_LOAD_STORE_AVX2_IMPLS_H_
#define _LIBMEM_LOAD_STORE_AVX2_IMPLS_H_

#ifdef __cplusplus
extern "C" {
#endif


#include <stddef.h>
#include <stdint.h>

#define AVX2_LOAD_ALIGNED    _mm256_load_si256
#define AVX2_LOAD_UNALIGNED  _mm256_loadu_si256
#define AVX2_LOAD_STREAM     _mm256_stream_load_si256

#define AVX2_STORE_ALIGNED    _mm256_store_si256
#define AVX2_STORE_UNALIGNED  _mm256_storeu_si256
#define AVX2_STORE_STREAM     _mm256_stream_si256

#define AVX2(num)             ymm##num
#define VEC_DECL_AVX2         __m256i
#define VEC_SZ_AVX2           32

static inline void * __load_store_le_2ymm_vec(void* __restrict store_addr,
            const void* __restrict load_addr, uint8_t size)
{
    register void *ret asm("rax");
    ret = store_addr;

    if (size <= 2 * DWORD_SZ)
    {
        if (size <=  2 * WORD_SZ)
        {
            if (size <= 1)
            {
                if (size)
                    *(uint8_t*)store_addr = *(uint8_t*)load_addr;
                return ret;
            }
            *((uint16_t *)store_addr) = *((uint16_t *)load_addr);
                *((uint16_t *)(store_addr + size - WORD_SZ)) = \
                        *((uint16_t *)(load_addr + size - WORD_SZ));
            return ret;
        }
        *((uint32_t *)store_addr) = *((uint32_t *)load_addr);
        *((uint32_t *)(store_addr + size - DWORD_SZ)) = \
                    *((uint32_t *)(load_addr + size - DWORD_SZ));
        return ret;
    }
    if (size <= 2 * XMM_SZ)
    {
        if (size <= 2 * QWORD_SZ)
        {
            *((uint64_t *)store_addr) = *((uint64_t *)load_addr);
            *((uint64_t *)(store_addr + size - QWORD_SZ)) = \
                    *((uint64_t *)(load_addr + size - QWORD_SZ));
            return ret;
        }
        __m128i x0, x1;
        x0 = _mm_loadu_si128(load_addr);
        x1 = _mm_loadu_si128(load_addr + size - XMM_SZ);
        _mm_storeu_si128(store_addr, x0);
        _mm_storeu_si128(store_addr + size - XMM_SZ, x1);
        return ret;
    }
    __m256i y0, y1;
    y0 = _mm256_loadu_si256(load_addr);
    y1 = _mm256_loadu_si256(load_addr + size - YMM_SZ);
    _mm256_storeu_si256(store_addr, y0);
    _mm256_storeu_si256(store_addr + size - YMM_SZ, y1);
    return ret;
}

static inline void * __load_store_le_2ymm_vec_overlap(void *store_addr,
            const void* load_addr, size_t size)
{
    register void *ret asm("rax");
    ret = store_addr;

    if (size <= 2 * DWORD_SZ)
    {
        if (size <=  2 * WORD_SZ)
        {
            if (size <= 1)
            {
                if (size)
                    *(uint8_t*)store_addr = *(uint8_t*)load_addr;
                return ret;
            }
            uint16_t temp = *((uint16_t *)load_addr);
            *((uint16_t *)(store_addr + size - WORD_SZ)) = \
                        *((uint16_t *)(load_addr + size - WORD_SZ));
            *((uint16_t *)store_addr) = temp;
            return ret;
        }
        uint32_t temp = *((uint32_t *)load_addr);
        *((uint32_t *)(store_addr + size - DWORD_SZ)) = \
                    *((uint32_t *)(load_addr + size - DWORD_SZ));
        *((uint32_t *)store_addr) = temp;
        return ret;
    }
    if (size < 2 * XMM_SZ)
    {
        if (size <= 2 * QWORD_SZ)
        {
            uint64_t temp = *((uint64_t *)load_addr);
            *((uint64_t *)(store_addr + size - QWORD_SZ)) = \
                    *((uint64_t *)(load_addr + size - QWORD_SZ));
            *((uint64_t *)store_addr) = temp;
            return ret;
        }
        __m128i x0, x1;
        x0 = _mm_loadu_si128(load_addr);
        x1 = _mm_loadu_si128(load_addr + size - XMM_SZ);
        _mm_storeu_si128(store_addr, x0);
        _mm_storeu_si128(store_addr + size - XMM_SZ, x1);
        return ret;
    }
    __m256i y0, y1;
    y0 = _mm256_loadu_si256(load_addr);
    y1 = _mm256_loadu_si256(load_addr + size - YMM_SZ);
    _mm256_storeu_si256(store_addr, y0);
    _mm256_storeu_si256(store_addr + size - YMM_SZ, y1);
    return ret;
}

static inline void __load_store_ymm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_1X_LOAD_STORE(AVX2, UNALIGNED, UNALIGNED)
}

static inline void __load_store_2ymm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_2X_LOAD_STORE(AVX2, UNALIGNED, UNALIGNED)
}

static inline void __load_store_3ymm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_3X_LOAD_STORE(AVX2, UNALIGNED, UNALIGNED)
}

static inline void __load_store_4ymm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_4X_LOAD_STORE(AVX2, UNALIGNED, UNALIGNED)
}

static inline void __load_store_8ymm_vec(void *store_addr,
            const void *load_addr, size_t offset)
{
    VEC_8X_LOAD_STORE(AVX2, UNALIGNED, UNALIGNED)
}

/* ################## TEMPORAL HEAD_TAIL ####################
 */

static inline void __load_store_le_4ymm_vec(void *store_addr,
            const void *load_addr, size_t size)
{
    VEC_4X_LOAD_STORE_HEAD_TAIL(AVX2, UNALIGNED, UNALIGNED)
}

static inline void  __load_store_le_8ymm_vec(void *store_addr,
            const void *load_addr, size_t size)
{
    VEC_8X_LOAD_STORE_HEAD_TAIL(AVX2, UNALIGNED, UNALIGNED)
}

/* ##################### TEMPORAL - 2xVEC LOOP ###################
 */
static inline size_t __unaligned_load_and_store_2ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_and_store_2ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_2ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL,UNALIGNED, ALIGNED)
}

static inline size_t __aligned_load_unaligned_store_2ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_unaligned_store_2ymm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

static inline size_t __unaligned_load_nt_store_2ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, STREAM)
}


/* #####################TEMPORAL - 4xVEC LOOPS###################
 */
static inline size_t __unaligned_load_and_store_4ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}

static inline size_t __unaligned_load_and_store_4ymm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX2, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_and_store_4ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, ALIGNED)
}

static inline size_t __aligned_load_and_store_4ymm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX2, PFTCH_ZERO_CL, ALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_4ymm_vec_loop\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_aligned_store_4ymm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX2, PFTCH_ZERO_CL, UNALIGNED, ALIGNED)
}

static inline size_t __aligned_load_unaligned_store_4ymm_vec_loop\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_unaligned_store_4ymm_vec_loop_bkwd\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP_BKWD(AVX2, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

/* #####################TEMPORAL 4xVEC LOOPS SW-PREFETCH###########
 */
static inline size_t __unaligned_load_and_store_4ymm_vec_loop_pftch\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_ONE_STEP, UNALIGNED, UNALIGNED)
}

static inline size_t __aligned_load_and_store_4ymm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_ONE_STEP, ALIGNED, ALIGNED)
}


static inline size_t __unaligned_load_aligned_store_4ymm_vec_loop_pftch\
    (void *store_addr, const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_ONE_STEP, UNALIGNED, ALIGNED)
}


/* #####################TEMPORAL - 8xVEC LOOPS###################
 */
static inline size_t __unaligned_load_and_store_8ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, UNALIGNED)
}


static inline size_t __aligned_load_and_store_8ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, ALIGNED)
}


static inline size_t __unaligned_load_aligned_store_8ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, ALIGNED)
}


static inline size_t __aligned_load_unaligned_store_8ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, UNALIGNED)
}

/* ##################### TEMPORAL - 8xVEC LOOPS SW-PREFETCH ###########
 */
static inline size_t __unaligned_load_aligned_store_8ymm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_TWO_STEP, UNALIGNED, ALIGNED)
}

static inline size_t __aligned_load_and_store_8ymm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_TWO_STEP, ALIGNED, ALIGNED)
}

static inline size_t __unaligned_load_and_store_8ymm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_TWO_STEP, UNALIGNED, UNALIGNED)
}

/* #####################NON-TEMPORAL 2xVEC LOOPS###################
 */
static inline size_t __nt_load_and_store_2ymm_vec_loop(void *store_addr,\
             const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, STREAM, STREAM)
}

static inline size_t __nt_load_unaligned_store_2ymm_vec_loop(void *store_addr, \
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_2X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, STREAM, UNALIGNED)
}


/* #####################NON-TEMPORAL 4xVEC LOOPS###################
 */
static inline size_t __nt_load_and_store_4ymm_vec_loop(void *store_addr,\
             const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, STREAM, STREAM)
}

static inline size_t __nt_load_unaligned_store_4ymm_vec_loop(void *store_addr, \
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, STREAM, UNALIGNED)
}

static inline size_t __aligned_load_nt_store_4ymm_vec_loop(void *store_addr, \
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, STREAM)
}

static inline size_t __unaligned_load_nt_store_4ymm_vec_loop(void *store_addr, \
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, STREAM)
}

/* ##################### NON-TEMPORAL 4xVEC LOOPS SW PREFETCH ###################
 */
static inline size_t __aligned_load_nt_store_4ymm_vec_loop_pftch(void *store_addr, \
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_ONE_STEP, ALIGNED, STREAM)
}

static inline size_t __unaligned_load_nt_store_4ymm_vec_loop_pftch(void *store_addr, \
                      const void *load_addr, size_t size, size_t offset)
{
    VEC_4X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_ONE_STEP, UNALIGNED, STREAM)
}

/* ##################### NON-TEMPORAL 8xVEC LOOPS ###################
 */

static inline size_t __aligned_load_nt_store_8ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, ALIGNED, STREAM)
}

static inline size_t __unaligned_load_nt_store_8ymm_vec_loop(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_ZERO_CL, UNALIGNED, STREAM)
}


/* ##################### NON-TEMPORAL 8xVEC LOOPS SW PREFETCH ###################
 */
static inline size_t __aligned_load_nt_store_8ymm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_TWO_STEP, ALIGNED, STREAM)
}

static inline size_t __unaligned_load_nt_store_8ymm_vec_loop_pftch(void *store_addr,
            const void *load_addr, size_t size, size_t offset)
{
    VEC_8X_LOAD_STORE_LOOP(AVX2, PFTCH_TWO_CL_TWO_STEP, UNALIGNED, STREAM)
}

/* ##########################  END of AVX2 IMPL'S ##################################
 */
#ifdef __cplusplus
}
#endif

#endif //HEADER
