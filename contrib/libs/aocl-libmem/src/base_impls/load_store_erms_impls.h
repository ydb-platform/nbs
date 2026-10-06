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
#ifndef _LIBMEM_LOAD_STORE_ERMS_IMPLS_H_
#define _LIBMEM_LOAD_STORE_ERMS_IMPLS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
#include <zen_cpu_info.h>

static inline void *__erms_movsb(void *dst, const void *src, size_t len)
{
    asm volatile("cld\n\t"
                 "rep movsb"
                 : "+D"(dst), "+S"(src), "+c"(len)
                 :
                 : "memory", "cc");
    return dst;
}

static inline void *__erms_movsw(void *dst, const void *src, size_t len)
{
    asm volatile("sar $1, %%rcx\n\t"
                 "cld\n\t"
                 "rep movsw"
                 : "+D"(dst), "+S"(src), "+c"(len)
                 :
                 : "memory", "cc");
    return dst;
}

static inline void *__erms_movsd(void *dst, const void *src, size_t len)
{
    asm volatile("sar $2, %%rcx\n\t"
                 "cld\n\t"
                 "rep movsd"
                 : "+D"(dst), "+S"(src), "+c"(len)
                 :
                 : "memory", "cc");
    return dst;
}

static inline void *__erms_movsq(void *dst, const void *src, size_t len)
{
    asm volatile("sar $3, %%rcx\n\t"
                 "cld\n\t"
                 "rep movsq"
                 : "+D"(dst), "+S"(src), "+c"(len)
                 :
                 : "memory", "cc");
    return dst;
}

static inline void *__erms_movsb_last_byte(void *dst, const void *src, size_t len)
{
    asm volatile("cld\n\t"
                 "rep movsb"
                 : "+D"(dst)
                 : "S"(src), "c"(len)
                 : "memory");
    return dst;
}

static inline void *__erms_movsw_last_byte(void *dst, const void *src, size_t len)
{
    asm volatile("sar $1, %%rcx\n\t"
                 "cld\n\t"
                 "rep movsw"
                 : "+D"(dst)
                 : "S"(src), "c"(len)
                 : "memory");
    return dst;
}

static inline void *__erms_movsd_last_byte(void *dst, const void *src, size_t len)
{
    asm volatile("sar $2, %%rcx\n\t"
                 "cld\n\t"
                 "rep movsd"
                 : "+D"(dst)
                 : "S"(src), "c"(len)
                 : "memory");
    return dst;
}

static inline void *__erms_movsq_last_byte(void *dst, const void *src, size_t len)
{
    asm volatile("sar $3, %%rcx\n\t"
                 "cld\n\t"
                 "rep movsq"
                 : "+D"(dst)
                 : "S"(src), "c"(len)
                 : "memory");
    return dst;
}

static inline void *__erms_movsb_back(void *dst, const void *src, size_t len)
{
    asm volatile("movq %%rdi, %%rax\n\t"
                 "std\n\t"
                 "rep movsb\n\t"
                 "cld"
                 :
                 : "D"(dst + len - 1), "S"(src + len - 1), "c"(len)
                 : "memory");
    return dst;
}

static inline void *__erms_movsw_back(void *dst, const void *src, size_t len)
{
    asm volatile("sar $1, %%rcx\n\t"
                 "std\n\t"
                 "rep movsw\n\t"
                 "cld"
                 :
                 : "D"(dst + len - WORD_SZ), "S"(src + len - WORD_SZ), "c"(len)
                 : "memory");
    return dst;
}

static inline void *__erms_movsd_back(void *dst, const void *src, size_t len)
{
    asm volatile("sar $2, %%rcx\n\t"
                 "std\n\t"
                 "rep movsd\n\t"
                 "cld"
                 :
                 : "D"(dst + len - DWORD_SZ), "S"(src + len - DWORD_SZ), "c"(len)
                 : "memory");
    return dst;
}

static inline void *__erms_movsq_back(void *dst, const void *src, size_t len)
{
    asm volatile("sar $3, %%rcx\n\t"
                 "std\n\t"
                 "rep movsq\n\t"
                 "cld"
                 :
                 : "D"(dst + len - QWORD_SZ), "S"(src + len - QWORD_SZ), "c"(len)
                 : "memory");
    return dst;
}

#ifdef __cplusplus
}
#endif

#endif // HEADER
