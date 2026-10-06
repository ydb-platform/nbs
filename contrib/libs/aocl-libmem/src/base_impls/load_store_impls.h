/* Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef _LIBMEM_LOAD_STORE_IMPLS_H_
#define _LIBMEM_LOAD_STORE_IMPLS_H_

#include "almem_defs.h"
#include "zen_cpu_info.h"
#include <immintrin.h>

#define CACHE_LINE_SZ 64

/* ---- Generic definitions ---- */
#define VEC_DECL(vec)           VEC_DECL_##vec

#define ALM_LOAD_INSTR(vec, type)   vec##_LOAD_##type

#define ALM_STORE_INSTR(vec, type)  vec##_STORE_##type

#define VEC_SZ(vec)             VEC_SZ_##vec

#define PFTCH_ZERO_CL

#define PFTCH_ONE_CL       \
  _mm_prefetch(load_addr + offset +  CACHE_LINE_SZ, _MM_HINT_NTA);


#define PFTCH_TWO_CL_ONE_STEP       \
  _mm_prefetch(load_addr + offset + 0 * CACHE_LINE_SZ, _MM_HINT_NTA);   \
  _mm_prefetch(load_addr + offset + 1 * CACHE_LINE_SZ, _MM_HINT_NTA);


#define PFTCH_TWO_CL_TWO_STEP       \
  _mm_prefetch(load_addr + offset + 1 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 3 * CACHE_LINE_SZ, _MM_HINT_NTA);

#define PFTCH_FOUR_CL_ONE_STEP      \
  _mm_prefetch(load_addr + offset + 0 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 1 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 2 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 3 * CACHE_LINE_SZ, _MM_HINT_NTA);

#define PFTCH_FOUR_CL_TWO_STEP      \
  _mm_prefetch(load_addr + offset + 1 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 3 * CACHE_LINE_SZ, _MM_HINT_NTA);

#define PFTCH_EIGHT_CL_ONE_STEP      \
  _mm_prefetch(load_addr + offset + 0 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 1 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 2 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 3 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 4 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 5 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 6 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 7 * CACHE_LINE_SZ, _MM_HINT_NTA);

#define PFTCH_EIGHT_CL_TWO_STEP      \
  _mm_prefetch(load_addr + offset + 1 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 3 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 5 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset + 7 * CACHE_LINE_SZ, _MM_HINT_NTA);


#define BKWD_PFTCH_TWO_CL_ONE_STEP       \
  _mm_prefetch(load_addr + offset - 1 * CACHE_LINE_SZ, _MM_HINT_NTA);   \
  _mm_prefetch(load_addr + offset - 2 * CACHE_LINE_SZ, _MM_HINT_NTA);


#define BKWD_PFTCH_TWO_CL_TWO_STEP                                   \
  _mm_prefetch(load_addr + offset - 1 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset - 3 * CACHE_LINE_SZ, _MM_HINT_NTA);

#define BKWD_PFTCH_FOUR_CL_ONE_STEP      \
  _mm_prefetch(load_addr + offset - 1 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset - 2 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset - 3 * CACHE_LINE_SZ, _MM_HINT_NTA); \
  _mm_prefetch(load_addr + offset - 4 * CACHE_LINE_SZ, _MM_HINT_NTA);


#define VEC_1X_LOAD_STORE(vec, load_type, store_type)                    \
  VEC_DECL(vec) vec(0) ;                                                 \
  vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);  \
  ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset, vec(0));


#define VEC_2X_LOAD_STORE(vec, load_type, store_type)                                    \
  VEC_DECL(vec) vec(0), vec(1) ;                                                         \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);                \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + VEC_SZ(vec));  \
    ALM_MEM_BARRIER();                                                                   \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset, vec(0));              \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + VEC_SZ(vec), vec(1));

#define VEC_3X_LOAD_STORE(vec, load_type, store_type)                                         \
  VEC_DECL(vec) vec(0), vec(1), vec(2);                                                       \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);                     \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + VEC_SZ(vec));       \
    vec(2) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 2 * VEC_SZ(vec));   \
    ALM_MEM_BARRIER();                                                                        \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset , vec(0));                  \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + VEC_SZ(vec), vec(1));     \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 2 * VEC_SZ(vec), vec(2));

#define VEC_4X_LOAD_STORE(vec, load_type, store_type)                                         \
  VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                               \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);                     \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + VEC_SZ(vec));       \
    vec(2) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 2 * VEC_SZ(vec));   \
    vec(3) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 3 * VEC_SZ(vec));   \
    ALM_MEM_BARRIER();                                                                        \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset , vec(0));                  \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + VEC_SZ(vec), vec(1));     \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 2 * VEC_SZ(vec), vec(2)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 3 * VEC_SZ(vec), vec(3));


#define VEC_8X_LOAD_STORE(vec, load_type, store_type)                          \
  VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                               \
  VEC_DECL(vec) vec(4), vec(5), vec(6), vec(7);                                               \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);                     \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + VEC_SZ(vec));       \
    vec(2) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 2 * VEC_SZ(vec));   \
    vec(3) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 3 * VEC_SZ(vec));   \
    vec(4) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 4 * VEC_SZ(vec));   \
    vec(5) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 5 * VEC_SZ(vec));   \
    vec(6) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 6 * VEC_SZ(vec));   \
    vec(7) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 7 * VEC_SZ(vec));   \
    ALM_MEM_BARRIER();                                                                        \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset, vec(0));                   \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset +  VEC_SZ(vec), vec(1));    \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 2 * VEC_SZ(vec), vec(2)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 3 * VEC_SZ(vec), vec(3)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 4 * VEC_SZ(vec), vec(4)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 5 * VEC_SZ(vec), vec(5)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 6 * VEC_SZ(vec), vec(6)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 7 * VEC_SZ(vec), vec(7));


#define VEC_2X_LOAD_STORE_LOOP(vec, pftch_cl,load_type, store_type)                      \
  VEC_DECL(vec) vec(0), vec(1) ;                                                         \
  while (offset < size) {                                                                \
    pftch_cl                                                                             \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);                \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + VEC_SZ(vec));  \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset, vec(0));              \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + VEC_SZ(vec), vec(1));\
    offset += 2 * VEC_SZ(vec);                                                           \
  }                                                                                      \
  return offset;

#define VEC_2X_LOAD_STORE_LOOP_BKWD(vec, pftch_cl, load_type, store_type)           \
  VEC_DECL(vec) vec(0), vec(1);                                                     \
  while (offset < size) {                                                           \
    pftch_cl                                                                        \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 1 * VEC_SZ(vec));   \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 2 * VEC_SZ(vec));   \
    ALM_MEM_BARRIER();                                                              \
    ALM_STORE_INSTR(vec, store_type) (store_addr + size - 1 * VEC_SZ(vec), vec(0)); \
    ALM_STORE_INSTR(vec, store_type) (store_addr + size - 2 * VEC_SZ(vec), vec(1)); \
    size -= 2 *  VEC_SZ(vec);                                                       \
  }                                                                                 \
  return size;



#define VEC_4X_LOAD_STORE_LOOP(vec, pftch_cl, load_type, store_type)                          \
  VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                               \
  while (offset < size) {                                                                     \
    pftch_cl                                                                                  \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);                     \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + VEC_SZ(vec));       \
    vec(2) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 2 * VEC_SZ(vec));   \
    vec(3) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 3 * VEC_SZ(vec));   \
    ALM_MEM_BARRIER();                                                                        \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset , vec(0));                  \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + VEC_SZ(vec), vec(1));     \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 2 * VEC_SZ(vec), vec(2)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 3 * VEC_SZ(vec), vec(3)); \
    offset += 4 *  VEC_SZ(vec);                                                               \
  }                                                                                           \
  return offset;

#define VEC_4X_LOAD_STORE_LOOP_BKWD(vec, pftch_cl, load_type, store_type)           \
  VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                     \
  while (offset < size) {                                                           \
    pftch_cl                                                                        \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 1 * VEC_SZ(vec));   \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 2 * VEC_SZ(vec));   \
    vec(2) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 3 * VEC_SZ(vec));   \
    vec(3) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 4 * VEC_SZ(vec));   \
    ALM_MEM_BARRIER();                                                              \
    ALM_STORE_INSTR(vec, store_type) (store_addr + size - 1 * VEC_SZ(vec), vec(0)); \
    ALM_STORE_INSTR(vec, store_type) (store_addr + size - 2 * VEC_SZ(vec), vec(1)); \
    ALM_STORE_INSTR(vec, store_type) (store_addr + size - 3 * VEC_SZ(vec), vec(2)); \
    ALM_STORE_INSTR(vec, store_type) (store_addr + size - 4 * VEC_SZ(vec), vec(3)); \
    size -= 4 *  VEC_SZ(vec);                                                       \
  }                                                                                 \
  return size;


#define VEC_8X_LOAD_STORE_LOOP(vec, pftch_cl, load_type, store_type)                          \
  VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                               \
  VEC_DECL(vec) vec(4), vec(5), vec(6), vec(7);                                               \
  while (offset < size) {                                                                     \
    pftch_cl                                                                                  \
    vec(0) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset);                     \
    vec(1) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + VEC_SZ(vec));       \
    vec(2) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 2 * VEC_SZ(vec));   \
    vec(3) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 3 * VEC_SZ(vec));   \
    vec(4) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 4 * VEC_SZ(vec));   \
    vec(5) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 5 * VEC_SZ(vec));   \
    vec(6) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 6 * VEC_SZ(vec));   \
    vec(7) = ALM_LOAD_INSTR(vec, load_type) ((void *)load_addr + offset + 7 * VEC_SZ(vec));   \
    ALM_MEM_BARRIER();                                                                        \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset, vec(0));                   \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset +  VEC_SZ(vec), vec(1));    \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 2 * VEC_SZ(vec), vec(2)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 3 * VEC_SZ(vec), vec(3)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 4 * VEC_SZ(vec), vec(4)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 5 * VEC_SZ(vec), vec(5)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 6 * VEC_SZ(vec), vec(6)); \
    ALM_STORE_INSTR(vec, store_type) ((void *)store_addr + offset + 7 * VEC_SZ(vec), vec(7)); \
    offset += 8 * VEC_SZ(vec);                                                                \
  }                                                                                           \
  return offset;


#define VEC_2X_LOAD_STORE_HEAD_TAIL(vec, load_type, store_type)                \
  VEC_DECL(vec)  vec(0) , vec(1) ;                                             \
  vec(0) = ALM_LOAD_INSTR(vec, load_type) (load_addr);                         \
  vec(1) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - VEC_SZ(vec));    \
  ALM_MEM_BARRIER();                                                           \
  ALM_STORE_INSTR(vec, store_type) (store_addr, vec(0));                       \
  ALM_STORE_INSTR(vec, store_type) (store_addr + size - VEC_SZ(vec), vec(1));


#define VEC_4X_LOAD_STORE_HEAD_TAIL(vec, load_type, store_type)                  \
  VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                  \
  vec(0) = ALM_LOAD_INSTR(vec, load_type) (load_addr);                           \
  vec(1) = ALM_LOAD_INSTR(vec, load_type) (load_addr + VEC_SZ(vec));             \
  vec(2) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 2 * VEC_SZ(vec));  \
  vec(3) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - VEC_SZ(vec));      \
  ALM_MEM_BARRIER();                                                             \
  ALM_STORE_INSTR(vec, store_type) (store_addr , vec(0));                        \
  ALM_STORE_INSTR(vec, store_type) (store_addr + VEC_SZ(vec), vec(1));           \
  ALM_STORE_INSTR(vec, store_type) (store_addr + size - 2 * VEC_SZ(vec), vec(2));\
  ALM_STORE_INSTR(vec, store_type) (store_addr + size - VEC_SZ(vec), vec(3));


#define VEC_8X_LOAD_STORE_HEAD_TAIL(vec, load_type, store_type)                  \
  VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                  \
  VEC_DECL(vec) vec(4), vec(5), vec(6), vec(7);                                  \
  vec(0) = ALM_LOAD_INSTR(vec, load_type) (load_addr);                           \
  vec(1) = ALM_LOAD_INSTR(vec, load_type) (load_addr + VEC_SZ(vec));             \
  vec(2) = ALM_LOAD_INSTR(vec, load_type) (load_addr + 2 * VEC_SZ(vec));         \
  vec(3) = ALM_LOAD_INSTR(vec, load_type) (load_addr + 3 * VEC_SZ(vec));         \
  vec(4) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 4 * VEC_SZ(vec));  \
  vec(5) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 3 * VEC_SZ(vec));  \
  vec(6) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 2 * VEC_SZ(vec));  \
  vec(7) = ALM_LOAD_INSTR(vec, load_type) (load_addr + size - 1 * VEC_SZ(vec));  \
  ALM_MEM_BARRIER();                                                             \
  ALM_STORE_INSTR(vec, store_type) (store_addr , vec(0));                        \
  ALM_STORE_INSTR(vec, store_type) (store_addr + VEC_SZ(vec), vec(1));           \
  ALM_STORE_INSTR(vec, store_type) (store_addr + 2 * VEC_SZ(vec), vec(2));       \
  ALM_STORE_INSTR(vec, store_type) (store_addr + 3 * VEC_SZ(vec), vec(3));       \
  ALM_STORE_INSTR(vec, store_type) (store_addr + size - 4 * VEC_SZ(vec), vec(4));\
  ALM_STORE_INSTR(vec, store_type) (store_addr + size - 3 * VEC_SZ(vec), vec(5));\
  ALM_STORE_INSTR(vec, store_type) (store_addr + size - 2 * VEC_SZ(vec), vec(6));\
  ALM_STORE_INSTR(vec, store_type) (store_addr + size - 1 * VEC_SZ(vec), vec(7));

#define VEC_16X_LOAD_STORE_HEAD_TAIL(vec, load_type, store_type)                                                       \
    VEC_DECL(vec) vec(0), vec(1), vec(2), vec(3);                                                                      \
    VEC_DECL(vec) vec(4), vec(5), vec(6), vec(7);                                                                      \
    VEC_DECL(vec) vec(8), vec(9), vec(10), vec(11);                                                                    \
    VEC_DECL(vec) vec(12), vec(13), vec(14), vec(15);                                                                  \
    vec(0) = ALM_LOAD_INSTR(vec, load_type)(load_addr);                                                                \
    vec(1) = ALM_LOAD_INSTR(vec, load_type)(load_addr + VEC_SZ(vec));                                                  \
    vec(2) = ALM_LOAD_INSTR(vec, load_type)(load_addr + 2 * VEC_SZ(vec));                                              \
    vec(3) = ALM_LOAD_INSTR(vec, load_type)(load_addr + 3 * VEC_SZ(vec));                                              \
    vec(4) = ALM_LOAD_INSTR(vec, load_type)(load_addr + 4 * VEC_SZ(vec));                                              \
    vec(5) = ALM_LOAD_INSTR(vec, load_type)(load_addr + 5 * VEC_SZ(vec));                                              \
    vec(6) = ALM_LOAD_INSTR(vec, load_type)(load_addr + 6 * VEC_SZ(vec));                                              \
    vec(7) = ALM_LOAD_INSTR(vec, load_type)(load_addr + 7 * VEC_SZ(vec));                                              \
    vec(8) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 8 * VEC_SZ(vec));                                       \
    vec(9) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 7 * VEC_SZ(vec));                                       \
    vec(10) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 6 * VEC_SZ(vec));                                      \
    vec(11) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 5 * VEC_SZ(vec));                                      \
    vec(12) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 4 * VEC_SZ(vec));                                      \
    vec(13) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 3 * VEC_SZ(vec));                                      \
    vec(14) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 2 * VEC_SZ(vec));                                      \
    vec(15) = ALM_LOAD_INSTR(vec, load_type)(load_addr + size - 1 * VEC_SZ(vec));                                      \
    ALM_MEM_BARRIER();                                                                                                 \
    ALM_STORE_INSTR(vec, store_type)(store_addr, vec(0));                                                              \
    ALM_STORE_INSTR(vec, store_type)(store_addr + VEC_SZ(vec), vec(1));                                                \
    ALM_STORE_INSTR(vec, store_type)(store_addr + 2 * VEC_SZ(vec), vec(2));                                            \
    ALM_STORE_INSTR(vec, store_type)(store_addr + 3 * VEC_SZ(vec), vec(3));                                            \
    ALM_STORE_INSTR(vec, store_type)(store_addr + 4 * VEC_SZ(vec), vec(4));                                            \
    ALM_STORE_INSTR(vec, store_type)(store_addr + 5 * VEC_SZ(vec), vec(5));                                            \
    ALM_STORE_INSTR(vec, store_type)(store_addr + 6 * VEC_SZ(vec), vec(6));                                            \
    ALM_STORE_INSTR(vec, store_type)(store_addr + 7 * VEC_SZ(vec), vec(7));                                            \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 8 * VEC_SZ(vec), vec(8));                                     \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 7 * VEC_SZ(vec), vec(9));                                     \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 6 * VEC_SZ(vec), vec(10));                                    \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 5 * VEC_SZ(vec), vec(11));                                    \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 4 * VEC_SZ(vec), vec(12));                                    \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 3 * VEC_SZ(vec), vec(13));                                    \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 2 * VEC_SZ(vec), vec(14));                                    \
    ALM_STORE_INSTR(vec, store_type)(store_addr + size - 1 * VEC_SZ(vec), vec(15));

#define VEC_1X_LOAD_STORE_AT(vec, load_type, store_type, d, s)                                                         \
    do                                                                                                                 \
    {                                                                                                                  \
        const void *load_addr = s;                                                                                     \
        void *store_addr = d;                                                                                          \
        size_t offset = 0;                                                                                             \
        VEC_1X_LOAD_STORE(vec, load_type, store_type)                                                                  \
    } while (0)

#define VEC_4X_LOAD_STORE_AT(vec, load_type, store_type, d, s)                                                         \
    do                                                                                                                 \
    {                                                                                                                  \
        const void *load_addr = s;                                                                                     \
        void *store_addr = d;                                                                                          \
        size_t offset = 0;                                                                                             \
        VEC_4X_LOAD_STORE(vec, load_type, store_type)                                                                  \
    } while (0)

#define VEC_2X_HEAD_TAIL(vec, d, s, sz)                                                                                \
    do                                                                                                                 \
    {                                                                                                                  \
        size_t _ht_sz = (sz);                                                                                          \
        void *store_addr = (d);                                                                                        \
        const void *load_addr = (s);                                                                                   \
        size_t size = _ht_sz;                                                                                          \
        VEC_2X_LOAD_STORE_HEAD_TAIL(vec, UNALIGNED, UNALIGNED)                                                         \
    } while (0)

#define VEC_4X_HEAD_TAIL(vec, d, s, sz)                                                                                \
    do                                                                                                                 \
    {                                                                                                                  \
        size_t _ht_sz = (sz);                                                                                          \
        void *store_addr = (d);                                                                                        \
        const void *load_addr = (s);                                                                                   \
        size_t size = _ht_sz;                                                                                          \
        VEC_4X_LOAD_STORE_HEAD_TAIL(vec, UNALIGNED, UNALIGNED)                                                         \
    } while (0)

#define VEC_8X_HEAD_TAIL(vec, d, s, sz)                                                                                \
    do                                                                                                                 \
    {                                                                                                                  \
        size_t _ht_sz = (sz);                                                                                          \
        void *store_addr = (d);                                                                                        \
        const void *load_addr = (s);                                                                                   \
        size_t size = _ht_sz;                                                                                          \
        VEC_8X_LOAD_STORE_HEAD_TAIL(vec, UNALIGNED, UNALIGNED)                                                         \
    } while (0)

#define VEC_16X_HEAD_TAIL(vec, d, s, sz)                                                                               \
    do                                                                                                                 \
    {                                                                                                                  \
        size_t _ht_sz = (sz);                                                                                          \
        void *store_addr = (d);                                                                                        \
        const void *load_addr = (s);                                                                                   \
        size_t size = _ht_sz;                                                                                          \
        VEC_16X_LOAD_STORE_HEAD_TAIL(vec, UNALIGNED, UNALIGNED)                                                        \
    } while (0)

/* ---- Pointer advance/retreat helpers for aligned loops ---- */

/* Advance sp/dp past the first cache-line-aligned store and compute remainder. */
#define ALIGN_ADVANCE_64(sp, dp, n, rem)                                                                               \
    do                                                                                                                 \
    {                                                                                                                  \
        size_t _off = -(size_t) (dp) & 63;                                                                             \
        if (_off == 0)                                                                                                 \
            _off = 64;                                                                                                 \
        sp += _off;                                                                                                    \
        dp += _off;                                                                                                    \
        rem = n - _off;                                                                                                \
    } while (0)

#define ADVANCE_4VEC(vec, sp, dp, rem)                                                                                 \
    do                                                                                                                 \
    {                                                                                                                  \
        sp += 4 * VEC_SZ(vec);                                                                                         \
        dp += 4 * VEC_SZ(vec);                                                                                         \
        rem -= 4 * VEC_SZ(vec);                                                                                        \
    } while (0)

/* Retreat sp/dp by 4 vectors (backward loop). */
#define RETREAT_4VEC(vec, sp, dp, rem)                                                                                 \
    do                                                                                                                 \
    {                                                                                                                  \
        sp -= 4 * VEC_SZ(vec);                                                                                         \
        dp -= 4 * VEC_SZ(vec);                                                                                         \
        rem -= 4 * VEC_SZ(vec);                                                                                        \
    } while (0)

/* Align end-of-dst down to 64B boundary and set up backward pointers. */
#define ALIGN_RETREAT_64(sp_base, dp_base, n, sp, dp, rem)                                                             \
    do                                                                                                                 \
    {                                                                                                                  \
        size_t _tail = ((size_t) (dp_base) + (n)) & 63;                                                                \
        if (_tail == 0)                                                                                                \
            _tail = 64;                                                                                                \
        rem = (n) - _tail;                                                                                             \
        sp = (const char *) (sp_base) + rem;                                                                           \
        dp = (char *) (dp_base) + rem;                                                                                 \
    } while (0)

/* ---- Overlap detection ---- */
#define REGIONS_OVERLAP(dst, src, size)                                                                                \
    (!((((const char *) (dst) + (size)) <= (const char *) (src)) ||                                                    \
       (((const char *) (src) + (size)) <= (char *) (dst))))

/* ---- Overlap vector save/restore macros ---- */
#define OVERLAP_SAVE_HEAD_4VEC(vec, src)                                                                               \
    VEC_DECL(vec) vec(0) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src));                                       \
    VEC_DECL(vec) vec(1) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + 1 * VEC_SZ(vec));                     \
    VEC_DECL(vec) vec(2) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + 2 * VEC_SZ(vec));                     \
    VEC_DECL(vec) vec(3) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + 3 * VEC_SZ(vec))

#define OVERLAP_SAVE_TAIL_1VEC(vec, src, size)                                                                         \
    VEC_DECL(vec) vec(4) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + (size) - 1 * VEC_SZ(vec))

#define OVERLAP_SAVE_HEAD_1VEC(vec, src) VEC_DECL(vec) vec(0) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src))

#define OVERLAP_SAVE_TAIL_4VEC(vec, src, size)                                                                         \
    VEC_DECL(vec) vec(1) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + (size) - 4 * VEC_SZ(vec));            \
    VEC_DECL(vec) vec(2) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + (size) - 3 * VEC_SZ(vec));            \
    VEC_DECL(vec) vec(3) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + (size) - 2 * VEC_SZ(vec));            \
    VEC_DECL(vec) vec(4) = ALM_LOAD_INSTR(vec, UNALIGNED)((const char *) (src) + (size) - 1 * VEC_SZ(vec))

#define OVERLAP_RESTORE_TAIL1_HEAD4(vec, dst, size)                                                                    \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + (size) - 1 * VEC_SZ(vec), vec(4));                                \
    ALM_MEM_BARRIER()                                                                                                  \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst), vec(0));                                                           \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + 1 * VEC_SZ(vec), vec(1));                                         \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + 2 * VEC_SZ(vec), vec(2));                                         \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + 3 * VEC_SZ(vec), vec(3))

#define OVERLAP_RESTORE_TAIL4_HEAD1(vec, dst, size)                                                                    \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + (size) - 4 * VEC_SZ(vec), vec(1));                                \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + (size) - 3 * VEC_SZ(vec), vec(2));                                \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + (size) - 2 * VEC_SZ(vec), vec(3));                                \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst) + (size) - 1 * VEC_SZ(vec), vec(4));                                \
    ALM_MEM_BARRIER()                                                                                                  \
    ALM_STORE_INSTR(vec, UNALIGNED)((char *) (dst), vec(0))

/* ---- Sub-ZMM head-tail copy macros ---- */

/*
 * Scalar head-tail copy for sizes 0..16.
 * Uses descending size checks: 8..16, 4..7, 2..3, 1.
 */
#define SCALAR_HEAD_TAIL(dst, src, size)                                                                               \
    do                                                                                                                 \
    {                                                                                                                  \
        if (likely((size) >= QWORD_SZ))                                                                                \
        {                                                                                                              \
            uint64_t _h = *(const uint64_t *) (src);                                                                   \
            uint64_t _t = *(const uint64_t *) ((const char *) (src) + (size) - QWORD_SZ);                              \
            *(uint64_t *) (dst) = _h;                                                                                  \
            *(uint64_t *) ((char *) (dst) + (size) - QWORD_SZ) = _t;                                                   \
        }                                                                                                              \
        else if ((size) >= DWORD_SZ)                                                                                   \
        {                                                                                                              \
            uint32_t _h = *(const uint32_t *) (src);                                                                   \
            uint32_t _t = *(const uint32_t *) ((const char *) (src) + (size) - DWORD_SZ);                              \
            *(uint32_t *) (dst) = _h;                                                                                  \
            *(uint32_t *) ((char *) (dst) + (size) - DWORD_SZ) = _t;                                                   \
        }                                                                                                              \
        else if ((size) >= WORD_SZ)                                                                                    \
        {                                                                                                              \
            uint16_t _h = *(const uint16_t *) (src);                                                                   \
            uint16_t _t = *(const uint16_t *) ((const char *) (src) + (size) - WORD_SZ);                               \
            *(uint16_t *) (dst) = _h;                                                                                  \
            *(uint16_t *) ((char *) (dst) + (size) - WORD_SZ) = _t;                                                    \
        }                                                                                                              \
        else if (size != 0)                                                                                            \
        {                                                                                                              \
            ((char *) (dst))[0] = ((const char *) (src))[0];                                                           \
        }                                                                                                              \
    } while (0)

#define SSE_HEAD_TAIL(dst, src, size)                                                                                  \
    do                                                                                                                 \
    {                                                                                                                  \
        __m128i _h = _mm_loadu_si128((const __m128i *) (src));                                                         \
        __m128i _t = _mm_loadu_si128((const __m128i *) ((const char *) (src) + (size) - XMM_SZ));                      \
        _mm_storeu_si128((__m128i *) (dst), _h);                                                                       \
        _mm_storeu_si128((__m128i *) ((char *) (dst) + (size) - XMM_SZ), _t);                                          \
    } while (0)

#define AVX_HEAD_TAIL(dst, src, size)                                                                                  \
    do                                                                                                                 \
    {                                                                                                                  \
        __m256i _h = _mm256_loadu_si256((const __m256i *) (src));                                                      \
        __m256i _t = _mm256_loadu_si256((const __m256i *) ((const char *) (src) + (size) - YMM_SZ));                   \
        _mm256_storeu_si256((__m256i *) (dst), _h);                                                                    \
        _mm256_storeu_si256((__m256i *) ((char *) (dst) + (size) - YMM_SZ), _t);                                       \
    } while (0)

/* ---- ISA-specific implementations ---- */
#include "load_store_sse2_impls.h"
#include "load_store_avx2_impls.h"
#include "load_store_avx512_impls.h"

#endif //HEADER
