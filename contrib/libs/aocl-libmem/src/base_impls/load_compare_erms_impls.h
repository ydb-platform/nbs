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
#ifndef _LIBMEM_LOAD_COMPARE_ERMS_IMPLS_H_
#define _LIBMEM_LOAD_COMPARE_ERMS_IMPLS_H_


#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

static inline int __erms_cmpsb(const void * mem1, const void * mem2, int size)
{
    int ret;

    asm volatile (
    "xorq %%rax, %%rax\n\t"
    "xorq %%rdx, %%rdx\n\t"
    "cld\n\t"
    "repz cmpsb\n\t"
    "movzx -1(%%rsi), %%ax\n\t"
    "movzx -1(%%rdi), %%dx\n\t"
    "sub  %%rdx, %%rax"
    : "=rax"(ret)
    : "S"(mem1), "D"(mem2), "c"(size)
    : "memory"
    );
    return ret;
}

static inline int __erms_cmpsw(const void * mem1, const void * mem2, int size)
{
    int ret;

    asm volatile (
    "xorq %%rax, %%rax\n\t"
    "xorq %%rdx, %%rdx\n\t"
    "sar $1, %%rcx\n\t"
    "cld\n\t"
    "repz cmpsw\n\t"
    "movzx -2(%%rsi), %%ax\n\t"
    "movzx -2(%%rdi), %%dx\n\t"
    "sub  %%rdx, %%rax"
    : "=rax"(ret)
    : "S"(mem1), "D"(mem2), "c"(size)
    : "memory"
    );
    return ret;
}

//rep cmpsd not supported in Clang
#ifndef ALMEM_CMPSD_CLANG_WORKAROUND
static inline int __erms_cmpsd(const void * mem1, const void * mem2, int size)
{
    int ret;

    asm volatile (
    "xorq %%rax, %%rax\n\t"
    "xorq %%rdx, %%rdx\n\t"
    "sar $2, %%rcx\n\t"
    "cld\n\t"
    "repz cmpsd\n\t"
    "movzx -4(%%rsi), %%ax\n\t"
    "movzx -4(%%rdi), %%dx\n\t"
    "sub  %%rdx, %%rax"
    : "=rax"(ret)
    : "S"(mem1), "D"(mem2), "c"(size)
    : "memory"
    );
    return ret;
}
#endif

static inline int __erms_cmpsq(const void * mem1, const void * mem2, int size)
{
    int ret;

    asm volatile (
    "xorq %%rax, %%rax\n\t"
    "xorq %%rdx, %%rdx\n\t"
    "sar $3, %%rcx\n\t"
    "cld\n\t"
    "repz cmpsq\n\t"
    "movzx -8(%%rsi), %%ax\n\t"
    "movzx -8(%%rdi), %%dx\n\t"
    "sub  %%rdx, %%rax"
    : "=rax"(ret)
    : "S"(mem1), "D"(mem2), "c"(size)
    : "memory"
    );
    return ret;
}

#ifdef __cplusplus
}
#endif

#endif //HEADER
