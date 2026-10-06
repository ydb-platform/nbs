/* Copyright (C) 2024-25 Advanced Micro Devices, Inc. All rights reserved.
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

#include "strlen_avx512.c"
#include "strchr_avx512.c"
#define STRNCMP
#include "strcmp_avx512.c"
#undef STRNCMP


/* Page check for initial block load and null check */
static inline void load_and_check_first_block(const char* haystack, size_t offset, 
                                             __m512i* z1, __mmask64* null_mask, uint64_t* null_idx)
{

    if (unlikely((PAGE_SZ - ZMM_SZ) < ((PAGE_SZ - 1) & (uintptr_t)haystack))) {
        __m512i z_mask = _mm512_set1_epi8(0xff);
        __mmask64 haystack_load_mask = UINT64_MAX >> offset;
        *z1 = _mm512_mask_loadu_epi8(z_mask, haystack_load_mask, haystack);
        __m512i z0 = _mm512_setzero_si512();
        *null_mask = _mm512_cmpeq_epi8_mask(*z1, z0) & haystack_load_mask;
        *null_idx = *null_mask ? _tzcnt_u64(*null_mask) : ZMM_SZ;
    } else {
        *z1 = _mm512_loadu_si512((void*)haystack);

        // Check for null terminator in all bytes
        __m512i z0 = _mm512_setzero_si512();
        *null_mask = _mm512_cmpeq_epi8_mask(*z1, z0);
        *null_idx = *null_mask ? _tzcnt_u64(*null_mask) : ZMM_SZ;
    }
}

/* Filtering function */
static inline void apply_filter(const char *haystack, size_t offset, size_t needle_len,
                                __m512i needle_second, __m512i needle_last, __mmask64 *match_mask)
{
    if (unlikely(!*match_mask)) return;
    
    uintptr_t haystack_ptr = (uintptr_t)(haystack + offset);
    size_t bytes_to_page_end = PAGE_SZ - (haystack_ptr & (PAGE_SZ - 1));
    
    // Only apply second character filter if completely safe
    if (bytes_to_page_end > (1 + ZMM_SZ)) {
        __m512i z_second = _mm512_loadu_si512((const void *)(haystack + offset + 1));
        *match_mask &= _mm512_cmpeq_epi8_mask(needle_second, z_second);
    }
    
    // For last character filter, be very conservative for large needles
    if (needle_len <= 512 && bytes_to_page_end > (needle_len + ZMM_SZ)) {
        // Only use last character filter for small needles with plenty of page space
        uintptr_t last_ptr = haystack_ptr + needle_len - 1;
        __m512i z_last = _mm512_loadu_si512((const void *)last_ptr);
        *match_mask &= _mm512_cmpeq_epi8_mask(needle_last, z_last);
    }
}

/* Broadcasting approach for smaller needles (≤ 2KB) */
static inline char * _strstr_avx512_broadcasting(const char* haystack, const char* needle, size_t needle_len)
{
    __m512i needle_first = _mm512_set1_epi8(needle[0]);
    __m512i needle_second = _mm512_set1_epi8(needle[1]);
    __m512i needle_last = _mm512_set1_epi8(needle[needle_len - 1]);
    
    size_t offset = (uintptr_t)haystack & (ZMM_SZ - 1);
    
    __m512i z1;
    __mmask64 null_mask;
    uint64_t null_idx;
    
    // First block processing with page check
    load_and_check_first_block(haystack, offset, &z1, &null_mask, &null_idx);
    
    // Early termination check 
    if (null_mask && (null_idx < needle_len))
        return NULL;
    
    // Process first block matches 
    __mmask64 match_mask = _mm512_cmpeq_epi8_mask(needle_first, z1);
    
    // Apply filtering 
    if (likely( (match_mask & ~null_mask))){
        apply_filter(haystack, 0, needle_len, needle_second, needle_last, &match_mask);
    }
    
    // Loop through all matches in first block
    if (match_mask) {
        do {
            uint64_t match_idx = _tzcnt_u64(match_mask);
            
            // Check if we have enough characters before null terminator
            if (null_mask && (match_idx + needle_len) > null_idx) {
                break; // Not enough characters remaining
            }
            
            // Call strncmp directly for this position
            if (_strncmp_avx512(haystack + match_idx, needle, needle_len) == 0) {
                return (char*)(haystack + match_idx);
            }
            
            // Remove this match and continue
            match_mask = _blsr_u64(match_mask);
        } while (match_mask);
    }
    
    if (null_mask) return NULL;
    
    // Adjust offset for aligned loads
    offset = ZMM_SZ - offset;
    
    // Main search loop 
    while (1) {
        z1 = _mm512_loadu_si512((void *)(haystack + offset));
        match_mask = _mm512_cmpeq_epi8_mask(z1, needle_first);
        null_mask = _mm512_cmpeq_epi8_mask(z1, _mm512_setzero_si512());
        
        // Apply filtering
        if (likely((match_mask & ~null_mask))){
            apply_filter(haystack, offset, needle_len, needle_second, needle_last, &match_mask);
        }
        
        // Loop through all matches in this block
        if (match_mask & ~null_mask) {
            do {
                uint64_t match_idx = _tzcnt_u64(match_mask);
                size_t absolute_pos = offset + match_idx;
                
                // Check if we have enough characters before null terminator
                if (null_mask && (match_idx + needle_len) > _tzcnt_u64(null_mask)) {
                    break; // Not enough characters remaining
                }
                
                // Call strncmp directly for this position
                if (_strncmp_avx512(haystack + absolute_pos, needle, needle_len) == 0) {
                    return (char*)(haystack + absolute_pos);
                }
                
                // Remove this match and continue
                match_mask = _blsr_u64(match_mask);
            } while (match_mask);
        }
        
        // Termination check
        if (null_mask) return NULL;
        
        offset += ZMM_SZ;
    }
}


/* This function is an optimized version of strstr using AVX-512 instructions.
It finds the first occurrence of the substring `needle` in the string `haystack`. */
static inline char * __attribute__((flatten)) _strstr_avx512(const char* haystack, const char* needle)
{
    size_t needle_len = 0;

    // If the first character of the needle is the string terminator,
    // return the haystack (empty needle case)
    if (unlikely(needle[0] == STR_TERM_CHAR))
        return (char*)haystack;

    // If the first character of the haystack is the string terminator,
    // return NULL (empty haystack case)
    if (unlikely(haystack[0] == STR_TERM_CHAR))
        return NULL;

    // If the second character of the needle is the string terminator,
    // it is similar to finding a char in a string
    if (needle[1] == STR_TERM_CHAR)
        return (char*)_strchr_avx512(haystack, needle[0]);

    // Get needle length to decide which approach to use
    needle_len = _strlen_avx512(needle);

    // For smaller needles, use simple broadcasting approach
    return _strstr_avx512_broadcasting(haystack, needle, needle_len);
}