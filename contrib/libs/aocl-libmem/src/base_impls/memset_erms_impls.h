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
#ifndef _LIBMEM_MEMSET_ERMS_IMPLS_H_
#define _LIBMEM_MEMSET_ERMS_IMPLS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

static inline void * __erms_stosb(void *mem, int val, size_t size)
{
    asm volatile (
    "movq %%rdi, %%rdx\n\t"
    "cld\n\t"
    "rep stosb"
    : "=d"(mem)
    : "D"(mem), "a"(val), "c"(size)
    : "memory"
    );
    return mem;
}

static inline void * __erms_stosw(void *mem, uint16_t val, size_t size)
{
    val = val | val << 8;

    asm volatile (
    "movq %%rdi, %%rdx\n\t"
    "sar $1, %%rcx\n\t"
    "cld\n\t"
    "rep stosw"
    : "=d"(mem)
    : "D"(mem), "a"(val), "c"(size)
    : "memory"
    );
    return mem;
}

static inline void * __erms_stosq(void *mem, uint64_t val, size_t size)
{
    val = val | val << 8;
    val = val | val << 16;
    val = val | val << 32;

    asm volatile (
    "movq %%rdi, %%rdx\n\t"
    "sar $3, %%rcx\n\t"
    "cld\n\t"
    "rep stosq"
    : "=d"(mem)
    : "D"(mem), "a"(val), "c"(size)
    : "memory"
    );
    return mem;
}

#ifdef __cplusplus
}
#endif

#endif //HEADER
