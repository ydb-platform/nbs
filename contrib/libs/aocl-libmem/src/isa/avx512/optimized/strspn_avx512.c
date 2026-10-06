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

 #include "almem_defs.h"
 #include "logger.h"
 #include <immintrin.h>
 #include <nmmintrin.h>
 #include <stddef.h>
 #include <zen_cpu_info.h>
 
 // Common type definition for AVX512 LUT
 typedef struct {
     __m512i lut[4];
 } accept_tables_avx512;
 
 /* LUT membership test using permutexvar (for accept > 16 chars) */
 static inline __mmask64 membership_mask_lut(__m512i bytes, const accept_tables_avx512 *t)
 {
     __m512i idx = _mm512_and_si512(bytes, _mm512_set1_epi8(0x3F));
     __mmask64 r0 = _mm512_cmplt_epu8_mask(bytes, _mm512_set1_epi8(64));
     __mmask64 r1 = _mm512_cmplt_epu8_mask(bytes, _mm512_set1_epi8((char) 128));
     __mmask64 r2 = _mm512_cmplt_epu8_mask(bytes, _mm512_set1_epi8((char) 192));
 
     __m512i v0 = _mm512_permutexvar_epi8(idx, t->lut[0]);
     __m512i v1 = _mm512_permutexvar_epi8(idx, t->lut[1]);
     __m512i v2 = _mm512_permutexvar_epi8(idx, t->lut[2]);
     __m512i v3 = _mm512_permutexvar_epi8(idx, t->lut[3]);
 
     __m512i result = _mm512_mask_blend_epi8(r0, _mm512_mask_blend_epi8(r1, _mm512_mask_blend_epi8(r2, v3, v2), v1), v0);
     return _mm512_cmpneq_epu8_mask(result, _mm512_setzero_si512());
 }
 
 /* Main strspn implementation */
 static inline size_t __attribute__((flatten)) _strspn_avx512(const char *s, const char *accept)
 {
     if (unlikely(!accept[0] || !s[0]))
     {
         return 0;
     }
 
    // Single-character accept: align down + masked first load, then ZMM loop
    if (unlikely(!accept[1]))
    {
        __m512i vc = _mm512_set1_epi8(accept[0]);
        __m512i zero = _mm512_setzero_si512();

        // Align down — always safe, same page as s
        const char *ptr = (const char *) ((uintptr_t) s & ~((size_t)ZMM_SZ - 1));
        unsigned skip = (unsigned) ((uintptr_t) s & (ZMM_SZ-1));

        // First aligned load, mask out bytes before s
        __m512i block = _mm512_load_si512(ptr);
        __mmask64 neq = _mm512_cmpneq_epi8_mask(block, vc);
        __mmask64 nul = _mm512_cmpeq_epi8_mask(block, zero);
        __mmask64 stop = (neq | nul) >> skip;
        if (stop)
            return _tzcnt_u64(stop);
        ptr += ZMM_SZ;

        // Main loop — aligned, no page checks
        while (1)
        {
            block = _mm512_load_si512(ptr);
            neq = _mm512_cmpneq_epi8_mask(block, vc);
            nul = _mm512_cmpeq_epi8_mask(block, zero);
            stop = neq | nul;
            if (unlikely(stop))
                return (ptr - s) + _tzcnt_u64(stop);
            ptr += ZMM_SZ;
        }
    }
     // Load first 16 bytes of accept into XMM — check page boundary
     size_t accept_page_off = (uintptr_t) accept & (PAGE_SZ - 1);
     __m128i accept_vec;
 
     if (unlikely(accept_page_off > (PAGE_SZ - XMM_SZ)))
     {
         // Accept near page boundary — masked load of the safe bytes first.
         size_t safe_bytes = (PAGE_SZ - accept_page_off);
         __mmask16 safe_mask = _bzhi_u32(0xFFFF, safe_bytes);
         accept_vec =
             _mm512_castsi512_si128(_mm512_mask_loadu_epi8(_mm512_setzero_si512(), (__mmask64) safe_mask, accept));
 
         // If the null terminator is within the safe region, accept_vec
         int nulls_in_safe = _mm_movemask_epi8(_mm_cmpeq_epi8(accept_vec, _mm_setzero_si128())) & safe_mask;
         if (!nulls_in_safe)
         {
             accept_vec = _mm_loadu_si128((const __m128i *) accept);
         }
     } else
     {
         // Safe to load 16 bytes
         accept_vec = _mm_loadu_si128((const __m128i *) accept);
     }
 
     __m128i zero_xmm = _mm_setzero_si128();
     int null_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(accept_vec, zero_xmm));
 
     // accept ≤ 16 chars: use PCMPISTRI path
     if (null_mask || !accept[16])
     {
         const char *ptr = s;
         int idx;
 
         // First load — check page boundary for s
         size_t page_off = (uintptr_t) s & (PAGE_SZ - 1);
         if (likely(page_off <= (PAGE_SZ - XMM_SZ)))
         {
            /*
             * _mm_cmpistri with (_SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY):
             * - Compares each byte in the source operand against ALL bytes in the accept operand
             * - Returns the index of the first byte NOT found in accept (or the first null byte)
             * - Handles null terminators implicitly in both operands
             */
             idx = _mm_cmpistri(accept_vec, _mm_loadu_si128((const __m128i *) s),
                                _SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY);
             if (idx < XMM_SZ)
             {
                 return idx;
             }
             ptr = s + XMM_SZ;
         } else
         {
             size_t safe = (PAGE_SZ - page_off);
             __mmask16 mask = _bzhi_u32(0xFFFF, safe);
             __m128i block = _mm512_castsi512_si128(_mm512_mask_loadu_epi8(_mm512_setzero_si512(), (__mmask64) mask, s));
             idx = _mm_cmpistri(accept_vec, block, _SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY);
             if (idx < (int) safe)
             {
                 return idx;
             }
             ptr = s + safe;
         }
 
         // Align to 16-byte boundary — aligned loads never cross pages
         ptr = (const char *) ((uintptr_t) ptr & ~(XMM_SZ - 1));
 
         while (1)
         {
             idx = _mm_cmpistri(accept_vec, _mm_load_si128((const __m128i *) ptr),
                                _SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY);
             if (idx < XMM_SZ)
             {
                 return (ptr - s) + idx;
             }

             idx = _mm_cmpistri(accept_vec, _mm_load_si128((const __m128i *) (ptr + XMM_SZ)),
                                _SIDD_SBYTE_OPS | _SIDD_NEGATIVE_POLARITY);
             if (idx < XMM_SZ)
             {
                 return (ptr - s) + XMM_SZ + idx;
             }
 
             ptr += (2 * XMM_SZ);
         }
     }
 
     // AVX512 LUT path for accept > 16 chars
     // Build 256-byte lookup table
     uint8_t lut_bytes[256] __attribute__((aligned(ZMM_SZ))) = {0};
     for (const unsigned char *p = (const unsigned char *) accept; *p; ++p)
     {
         lut_bytes[*p] = 0xFF;
     }
 
     accept_tables_avx512 tbl;
     tbl.lut[0] = _mm512_load_si512((__m512i *) &lut_bytes[0]);
     tbl.lut[1] = _mm512_load_si512((__m512i *) &lut_bytes[64]);
     tbl.lut[2] = _mm512_load_si512((__m512i *) &lut_bytes[128]);
     tbl.lut[3] = _mm512_load_si512((__m512i *) &lut_bytes[192]);
 
     __m512i zero = _mm512_setzero_si512();
     const char *ptr = s;
 
     // First load - check page boundary only here
     size_t page_off = (uintptr_t) ptr & (PAGE_SZ - 1);
     if (unlikely(page_off > (PAGE_SZ - ZMM_SZ)))
     {
         // Near page boundary - use masked load
         size_t safe_bytes = (PAGE_SZ - page_off);
         __mmask64 load_mask = _bzhi_u64((uint64_t) -1, safe_bytes);
         __m512i block = _mm512_mask_loadu_epi8(_mm512_setzero_si512(), load_mask, ptr);
 
         __mmask64 null_mask_avx = _mm512_mask_cmpeq_epi8_mask(load_mask, block, zero);
         __mmask64 member = membership_mask_lut(block, &tbl);
         __mmask64 stop = (null_mask_avx | (~member)) & load_mask;
 
         if (stop)
         {
             return _tzcnt_u64(stop);
         }
         ptr += safe_bytes;
     } else
     {
         __m512i block = _mm512_loadu_si512(ptr);
         __mmask64 null_mask_avx = _mm512_cmpeq_epi8_mask(block, zero);
         __mmask64 member = membership_mask_lut(block, &tbl);
         __mmask64 stop = null_mask_avx | (~member);
 
         if (unlikely(stop))
         {
             return _tzcnt_u64(stop);
         }
         ptr += ZMM_SZ;
     }
 
     // Align to 64-byte boundary
     ptr = (const char *) ((uintptr_t) ptr & ~((size_t)ZMM_SZ - 1));
 
     // Main loop - all loads are 64-byte aligned, never cross pages
     while (1)
     {
         __m512i block = _mm512_load_si512(ptr);
         __mmask64 null_mask_avx = _mm512_cmpeq_epi8_mask(block, zero);
         __mmask64 member = membership_mask_lut(block, &tbl);
         __mmask64 stop = null_mask_avx | (~member);
 
         if (unlikely(stop))
         {
             return (ptr - s) + _tzcnt_u64(stop);
         }
         ptr += ZMM_SZ;
     }
 }
 