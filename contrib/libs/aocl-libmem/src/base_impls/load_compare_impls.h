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

 #ifndef _LIBMEM_LOAD_COMPARE_IMPLS_H_
 #define _LIBMEM_LOAD_COMPARE_IMPLS_H_
 
 #include <immintrin.h>
 #include "almem_defs.h"
 #include "load_store_impls.h"
 
 
 #define AVX512_CMPEQ_EPU8_MASK  _mm512_cmpeq_epu8_mask
 #define AVX512_MASKZ_MOV_EPI8   _mm512_maskz_mov_epi8
 #define AVX512_MIN_EPU8         _mm512_min_epu8
 #define AVX512_TESTN_EPI8_MASK  _mm512_testn_epi8_mask
 
 //Generic compare instruction dispatchers
 #define ALM_CMPEQ_MASK(vec)         vec##_CMPEQ_EPU8_MASK
 
 #define ALM_MASKZ_MOV(vec)          vec##_MASKZ_MOV_EPI8
 
 #define ALM_MIN_EPU8(vec)           vec##_MIN_EPU8
 
 #define ALM_TESTN_MASK(vec)         vec##_TESTN_EPI8_MASK
 
 /*
  * 4x vector load-compare-reduce for strcmp.
  * Loads 4 vectors from str1 and str2, compares via cmpeq + maskz_mov,
  * reduces via VPMINUB, and resolves mismatch with VTESTNM.
  * On mismatch: sets ret (non-zero) and offset to the matching vector.
  * On match: sets ret to 0 and advances offset by 4 * VEC_SZ.
  *
  * Expects caller to have: str1, str2, offset, ret.
  *
  * vec:       vector family (e.g. AVX512)
  * load_type: ALIGNED or UNALIGNED
  */
 #define VEC_4X_LOAD_COMPARE_REDUCE(vec, load_type)                                               \
 {                                                                                                \
     VEC_DECL(vec) a0 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str1 + offset));                 \
     VEC_DECL(vec) b0 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str2 + offset));                 \
     VEC_DECL(vec) r0 = ALM_MASKZ_MOV(vec)(ALM_CMPEQ_MASK(vec)(a0, b0), a0);                     \
                                                                                                  \
     VEC_DECL(vec) a1 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str1 + offset + VEC_SZ(vec)));   \
     VEC_DECL(vec) b1 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str2 + offset + VEC_SZ(vec)));   \
     VEC_DECL(vec) r1 = ALM_MASKZ_MOV(vec)(ALM_CMPEQ_MASK(vec)(a1, b1), a1);                     \
                                                                                                  \
     VEC_DECL(vec) a2 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str1 + offset + 2*VEC_SZ(vec))); \
     VEC_DECL(vec) b2 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str2 + offset + 2*VEC_SZ(vec))); \
     VEC_DECL(vec) r2 = ALM_MASKZ_MOV(vec)(ALM_CMPEQ_MASK(vec)(a2, b2), a2);                     \
                                                                                                  \
     VEC_DECL(vec) a3 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str1 + offset + 3*VEC_SZ(vec))); \
     VEC_DECL(vec) b3 = ALM_LOAD_INSTR(vec, load_type) ((void *)(str2 + offset + 3*VEC_SZ(vec))); \
     VEC_DECL(vec) r3 = ALM_MASKZ_MOV(vec)(ALM_CMPEQ_MASK(vec)(a3, b3), a3);                     \
                                                                                                  \
     VEC_DECL(vec) reduced = ALM_MIN_EPU8(vec)(                                                   \
         ALM_MIN_EPU8(vec)(r0, r1), ALM_MIN_EPU8(vec)(r2, r3));                                   \
     ret = (uint64_t) ALM_TESTN_MASK(vec)(reduced, reduced);                                      \
                                                                                                  \
     if (unlikely(ret))                                                                           \
     {                                                                                            \
         uint64_t m0 = (uint64_t) ALM_TESTN_MASK(vec)(r0, r0);                                   \
         uint64_t m1 = (uint64_t) ALM_TESTN_MASK(vec)(r1, r1);                                   \
         uint64_t m2 = (uint64_t) ALM_TESTN_MASK(vec)(r2, r2);                                   \
                                                                                                  \
         if (m0)      { ret = m0; break; }                                                        \
         offset += VEC_SZ(vec);                                                                   \
         if (m1)      { ret = m1; break; }                                                        \
         offset += VEC_SZ(vec);                                                                   \
         if (m2)      { ret = m2; break; }                                                        \
         offset += VEC_SZ(vec);                                                                   \
         ret = (uint64_t) ALM_TESTN_MASK(vec)(r3, r3);                                           \
         break;                                                                                   \
     }                                                                                            \
     offset += 4 * VEC_SZ(vec);                                                                   \
 }
 
 #endif //HEADER
