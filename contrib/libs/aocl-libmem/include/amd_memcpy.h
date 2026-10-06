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
#ifndef _MEMCPY_H_
#define _MEMCPY_H_
#include "almem_defs.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* (*amd_memcpy_fn)(void *, const void*, size_t);

//Micro architecture specifc implementations.
HIDDEN_SYMBOL extern void * __memcpy_zen1(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_zen2(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_zen3(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_zen4(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_zen5(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_zen6(void *dest, const void *src, size_t size);

//System solution which takes in system config and  threshold values.
HIDDEN_SYMBOL extern void * __memcpy_system(void *dest,const void *src, size_t size);

#ifdef ALMEM_TUNABLES
//Generic solution which takes in user threshold values.
HIDDEN_SYMBOL extern void * __memcpy_threshold(void *dest,const void *src, size_t size);

//CPU Feature:AVX2 and Alignment specifc implementations.
HIDDEN_SYMBOL extern void * __memcpy_avx2_unaligned(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx2_aligned(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx2_aligned_load(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx2_aligned_store(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx2_nt(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx2_nt_load(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx2_nt_store(void *dest,const void *src, size_t size);

//CPU Feature:AVX512 and Alignment specifc implementations.
HIDDEN_SYMBOL extern void * __memcpy_avx512_unaligned(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx512_aligned(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx512_aligned_load(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx512_aligned_store(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx512_nt(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx512_nt_load(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_avx512_nt_store(void *dest,const void *src, size_t size);

//CPU Feature:ERMS and Alignment specifc implementations.
HIDDEN_SYMBOL extern void * __memcpy_erms_b_aligned(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_erms_w_aligned(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_erms_d_aligned(void *dest,const void *src, size_t size);
HIDDEN_SYMBOL extern void * __memcpy_erms_q_aligned(void *dest,const void *src, size_t size);
#endif

#ifdef __cplusplus
}
#endif

#endif
