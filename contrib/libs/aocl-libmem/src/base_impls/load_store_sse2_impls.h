/* Copyright (C) 2023 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef _LIBMEM_LOAD_STORE_SSE2_IMPLS_H_
#define _LIBMEM_LOAD_STORE_SSE2_IMPLS_H_

#ifdef __cplusplus
extern "C" {
#endif

//SSE definitions
#define SSE2_LOAD_ALIGNED       _mm_load_si128
#define SSE2_LOAD_UNALIGNED     _mm_loadu_si128
#define SSE2_LOAD_STREAM        _mm128_stream_load_si128

#define SSE2_STORE_ALIGNED      _mm_store_si128
#define SSE2_STORE_UNALIGNED    _mm_storeu_si128
#define SSE2_STORE_STREAM       _mm128_stream_si128

#define SSE2(num)               xmm##num
#define VEC_DECL_SSE2           __m128i

#define VEC_SZ_SSE2             16


#ifdef __cplusplus
}
#endif

#endif //HEADER
