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
#include <stddef.h>
#include "logger.h"
#include <stdint.h>
#include <immintrin.h>
#include "almem_defs.h"
#include "zen_cpu_info.h"
/* This function is an optimized version of strcmp and strncmp using AVX-512 instructions.
It compares the two strings str1 and str2 and returns returns an integer
indicating the result of the comparison. */

#ifdef STRNCMP
static inline int __attribute__((flatten)) _strncmp_avx512(const char *str1, const char *str2, size_t size)
#else
static inline int __attribute__((flatten)) _strcmp_avx512(const char *str1, const char *str2)
#endif
{
#ifdef STRNCMP
    if (unlikely(size == 0))
        return 0;
#endif

    size_t offset1, offset2, offset;
    __m512i z0, z1, z2, z3, z4, z_mask;
    uint64_t  cmp_idx, ret, mask;

    // Initialize a zeroed AVX-512 register for comparisons against null terminators
    z0 = _mm512_setzero_epi32 ();

    // Handle cases where start of either of the string is close to the end of a memory page
    if (unlikely(((PAGE_SZ - ZMM_SZ) < ((PAGE_SZ - 1) & ((uintptr_t)str1 | (uintptr_t)str2)))))
    {
        offset1 = (uintptr_t)str1 & (ZMM_SZ - 1);
        offset2 = (uintptr_t)str2 & (ZMM_SZ - 1);
        offset = (offset1 >= offset2) ? offset1 : offset2;
        z_mask = _mm512_set1_epi8(0xff);

        // Create a mask that will ignore the first 'offset' bytes when loading data
        mask = UINT64_MAX >> offset;

        z1 =  _mm512_mask_loadu_epi8(z_mask, mask, str1);
        z2 =  _mm512_mask_loadu_epi8(z_mask, mask, str2);

        // Compare the vectors for equality and inequality
        ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);

        // If there is a difference or a null terminator, find the index of the first diff/null
        if (ret)
        {
            cmp_idx = _tzcnt_u64(ret);
#ifdef STRNCMP
            if (cmp_idx >= size)
                return 0;
#endif
            return (unsigned char)str1[cmp_idx] - (unsigned char)str2[cmp_idx];
        }
#ifdef STRNCMP
        else if (size <= (ZMM_SZ - offset))
            return 0;
#endif
    }
    // If the strings are not close to the end of a memory page, load the first 64 bytes
    else
    {
        z1 = _mm512_loadu_si512(str1);
        z2 = _mm512_loadu_si512(str2);

        ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1, z2);
        if (ret)
        {
            cmp_idx = _tzcnt_u64(ret);
#ifdef STRNCMP
            if (cmp_idx >= size)
                return 0;
#endif
            return (unsigned char)str1[cmp_idx] - (unsigned char)str2[cmp_idx];
        }
#ifdef STRNCMP
        else if (size <= ZMM_SZ)
            return 0;
#endif
        else
        {
            offset1 = (uintptr_t)str1 & (ZMM_SZ - 1);
            offset2 = (uintptr_t)str2 & (ZMM_SZ - 1);
            offset = (offset1 >= offset2) ? offset1 : offset2;
        }
    }
    // Adjust the offset to align with cache line for the next load operation
    offset = ZMM_SZ - offset;

    // Alignement of both the strings is same
    if (unlikely(offset1 == offset2))
    {
#ifdef STRNCMP
        while (offset < (size - 4 * ZMM_SZ))
#else
        while (1)
#endif
        {
            // Load 64 bytes from each string and compare till we find diff/null
            z1 = _mm512_load_si512(str1 + offset);
            z2 = _mm512_load_si512(str2 + offset);
            //Check for NULL
            ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);
            if (!ret)
            {
                offset += ZMM_SZ;
                z3 = _mm512_load_si512(str1 + offset);
                z4 = _mm512_load_si512(str2 + offset);
                //Check for NULL
                ret = _mm512_cmpeq_epu8_mask(z3, z0) | _mm512_cmpneq_epu8_mask(z3, z4);
                if (!ret)
                {
                    offset += ZMM_SZ;
                    z1 = _mm512_load_si512(str1 + offset);
                    z2 = _mm512_load_si512(str2 + offset);
                    //Check for NULL
                    ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);
                    if (!ret)
                    {
                        offset += ZMM_SZ;
                        z3 = _mm512_load_si512(str1 + offset);
                        z4 = _mm512_load_si512(str2 + offset);
                        //Check for NULL
                        ret = _mm512_cmpeq_epu8_mask(z3, z0) | _mm512_cmpneq_epu8_mask(z3, z4);
                    }
                }
            }
            if (ret)
                break;
            offset += ZMM_SZ;
#ifdef STRNCMP
            if (size <= offset)
                return 0;
#endif
        } //end of while for 4xVEC
        while (1)
        {
            z1 = _mm512_load_si512(str1 + offset);
            z2 = _mm512_load_si512(str2 + offset);
            //Check for NULL and mismatch
            ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);
            if (ret)
            {
                goto return_val;
            }
            offset += ZMM_SZ;
#ifdef STRNCMP
            if (size <= offset)
                return 0;
#endif
        } // end of while for 1xVEC
    }
    // Handle the case where alignments of the strings are different
    else
    {
        while (1)
        {
            // Check if either string is aligned at current offset
            char *aligned_str, *unaligned_str;
            
            if ((((uintptr_t)str1 + offset) & (ZMM_SZ - 1)) == 0)
            {
                aligned_str = (char *)str1;
                unaligned_str = (char *)str2;
            }
            else if ((((uintptr_t)str2 + offset) & (ZMM_SZ - 1)) == 0)
            {
                aligned_str = (char *)str2;
                unaligned_str = (char *)str1;
            }
            else
            {
                // Use unaligned loads for both strings
                z1 = _mm512_loadu_si512(str1 + offset);
                z2 = _mm512_loadu_si512(str2 + offset);
                ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1, z2);
                if (ret) {
                    goto return_val;
                }
                offset += ZMM_SZ;
#ifdef STRNCMP
                if (size <= offset)
                    return 0;
#endif
                continue; // Go back and check alignment again
            }
            // Calculate safe vectors based on page boundaries for BOTH strings
            uint16_t vecs_in_page  = (PAGE_SZ - ((PAGE_SZ - 1) & ((uintptr_t)unaligned_str + offset))) >> 6;
            
#ifdef STRNCMP
            while ((vecs_in_page >= 4) && (offset + 4 * ZMM_SZ <= size))
#else
            while (vecs_in_page >= 4)
#endif
            {
                z1 = _mm512_load_si512(aligned_str + offset);
                z2 = _mm512_loadu_si512(unaligned_str + offset);
                //Check for NULL
                ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);
                if (!ret)
                {
                    offset += ZMM_SZ;
                    z3 = _mm512_load_si512(aligned_str + offset);
                    z4 = _mm512_loadu_si512(unaligned_str + offset);
                    //Check for NULL
                    ret = _mm512_cmpeq_epu8_mask(z3, z0) | _mm512_cmpneq_epu8_mask(z3, z4);
                    if (!ret)
                    {
                        offset += ZMM_SZ;
                        z1 = _mm512_load_si512(aligned_str + offset);
                        z2 = _mm512_loadu_si512(unaligned_str + offset);
                        //Check for NULL
                        ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);
                        if (!ret)
                        {
                            offset += ZMM_SZ;
                            z3 = _mm512_load_si512(aligned_str + offset);
                            z4 = _mm512_loadu_si512(unaligned_str + offset);
                            //Check for NULL
                            ret = _mm512_cmpeq_epu8_mask(z3, z0) | _mm512_cmpneq_epu8_mask(z3, z4);
                            if (!ret)
                            {
                                offset += ZMM_SZ;
                                vecs_in_page -= 4;
                                continue;
                            }
                        }
                    }
                }
                goto return_val;
            } //end of while
#ifdef STRNCMP
            if (size <= offset)
                return 0;
#endif
            while (vecs_in_page--)
            {
                z1 = _mm512_load_si512(aligned_str + offset);
                z2 = _mm512_loadu_si512(unaligned_str + offset);
                //Check for NULL
                ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);
                if (ret)
                {
                    goto return_val;
                }
                offset += ZMM_SZ;
#ifdef STRNCMP
                if (size <= offset)
                    return 0;
#endif
            }
            // Handle the case where we are crossing a page boundary for the unaligned string
            z_mask = _mm512_set1_epi8(0xff);
            mask = UINT64_MAX >> ((uintptr_t)(unaligned_str + offset)& (ZMM_SZ - 1));

            z1 =  _mm512_mask_loadu_epi8(z_mask, mask, aligned_str + offset);
            z2 =  _mm512_mask_loadu_epi8(z_mask, mask, unaligned_str + offset);

            ret = _mm512_cmpeq_epu8_mask(z1, z0) | _mm512_cmpneq_epu8_mask(z1,z2);

            if (ret)
            {
                goto return_val;
            }
#ifdef STRNCMP
            if (size <= (offset + (ZMM_SZ - ((uintptr_t)(unaligned_str + offset)& (ZMM_SZ - 1)))))
                return 0;
#endif

            // Advance offset after processing page boundary crossing
            offset += ZMM_SZ - ((uintptr_t)(unaligned_str + offset) & (ZMM_SZ - 1));
        } //end of top level while
    }
return_val:
    cmp_idx = _tzcnt_u64(ret) + offset;
#ifdef STRNCMP
    if (cmp_idx >= size)
        return 0;
#endif
    return (unsigned char)str1[cmp_idx] - (unsigned char)str2[cmp_idx];
}
