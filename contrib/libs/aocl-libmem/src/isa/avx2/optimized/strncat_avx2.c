/* Copyright (C) 2025 Advanced Micro Devices, Inc. All rights reserved.
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
#include "strlen_avx2.c"

// Include strnlen implementation (same file as strlen with macro)
#define STRNLEN_AVX2
#include "strlen_avx2.c"
#undef STRNLEN_AVX2

#include "memcpy_avx2.c"
#define NULL_TERM_CHAR '\0'

/* AVX2 optimized strncat using existing optimized strlen, strnlen, and memcpy. */
static inline char * __attribute__((flatten)) _strncat_avx2(char *dst, const char *src, size_t n)
{
    if (n == 0)
        return dst;

    // Find end of dst using optimized strlen
    size_t dst_len = _strlen_avx2(dst);
    
    // Find how many bytes to copy from src (at most n) using optimized strnlen
    size_t copy_len = _strnlen_avx2(src, n);
    
    // Use optimized memcpy to copy
    if (copy_len > 0)
        _memcpy_avx2(dst + dst_len, src, copy_len);
    
    // Add null terminator
    *(dst + dst_len + copy_len) = NULL_TERM_CHAR;
    
    return dst;
}
