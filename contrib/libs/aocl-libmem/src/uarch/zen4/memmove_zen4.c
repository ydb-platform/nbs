/* Copyright (C) 2024-26 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef MEMMOVE_ZEN4
#define MEMMOVE_ZEN4

#include "logger.h"
#include "memcpy_impl_zen4.c"

HIDDEN_SYMBOL void *__attribute__((flatten)) __memmove_zen4(void *dst, const void *src, size_t size)
{
    register void *ret asm("rax");
    ret = dst;

    LOG_INFO("\n");

    if (likely(size <= 2 * ZMM_SZ))
    {
        if (likely(size < ZMM_SZ))
        {
            const char *cs = src;
            char *cd = dst;

            if (size < WORD_SZ)
            {
                if (size == 1)
                    *cd = *cs;
                return ret;
            }
            if (size < DWORD_SZ)
            {
                uint16_t head = *(const uint16_t *) cs;
                uint16_t tail = *(const uint16_t *) (cs + size - WORD_SZ);
                ALM_MEM_BARRIER();
                *(uint16_t *) cd = head;
                *(uint16_t *) (cd + size - WORD_SZ) = tail;
                return ret;
            }
            if (size < QWORD_SZ)
            {
                uint32_t head = *(const uint32_t *) cs;
                uint32_t tail = *(const uint32_t *) (cs + size - DWORD_SZ);
                ALM_MEM_BARRIER();
                *(uint32_t *) cd = head;
                *(uint32_t *) (cd + size - DWORD_SZ) = tail;
                return ret;
            }
            if (size < XMM_SZ)
            {
                uint64_t head = *(const uint64_t *) cs;
                uint64_t tail = *(const uint64_t *) (cs + size - QWORD_SZ);
                ALM_MEM_BARRIER();
                *(uint64_t *) cd = head;
                *(uint64_t *) (cd + size - QWORD_SZ) = tail;
                return ret;
            }
            if (size < YMM_SZ)
            {
                __m128i x0 = _mm_loadu_si128((const __m128i *) cs);
                __m128i x1 = _mm_loadu_si128((const __m128i *) (cs + size - XMM_SZ));
                ALM_MEM_BARRIER();
                _mm_storeu_si128((__m128i *) cd, x0);
                _mm_storeu_si128((__m128i *) (cd + size - XMM_SZ), x1);
                return ret;
            }
            __m256i y0 = _mm256_loadu_si256((const __m256i *) cs);
            __m256i y1 = _mm256_loadu_si256((const __m256i *) (cs + size - YMM_SZ));
            ALM_MEM_BARRIER();
            _mm256_storeu_si256((__m256i *) cd, y0);
            _mm256_storeu_si256((__m256i *) (cd + size - YMM_SZ), y1);
            return ret;
        }
        /* 64B .. 128B: overlap-safe ZMM head + tail */
        __m512i z0 = _mm512_loadu_si512(src);
        __m512i z1 = _mm512_loadu_si512(src + size - ZMM_SZ);
        ALM_MEM_BARRIER();
        _mm512_storeu_si512(dst, z0);
        _mm512_storeu_si512(dst + size - ZMM_SZ, z1);
        return ret;
    }
    return _memmove_zen4_impl(dst, src, size);
}

#ifndef ALMEM_DYN_DISPATCH
void *memmove(void *, const void *, size_t) __attribute__((weak,
                        alias("__memmove_zen4")));
#endif

#endif // MEMMOVE_ZEN4
