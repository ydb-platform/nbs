/* Copyright (C) 2022 Advanced Micro Devices, Inc. All rights reserved.
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
#include "../../base_impls/load_compare_erms_impls.h"
// memcmp with byte rep move instruciton:REP CMPSB
int __memcmp_erms_b_aligned(const void * mem1, const void *mem2, size_t size)
{
    LOG_INFO("\n");
    return __erms_cmpsb(mem1, mem2, size);
}

// memcmp with word rep move instruciton:REP CMPSW
int __memcmp_erms_w_aligned(const void * mem1, const void *mem2, size_t size)
{
    LOG_INFO("\n");
    return __erms_cmpsw(mem1, mem2, size);
}

// memcmp with double word rep move instruciton:REP CMPSD
int __memcmp_erms_d_aligned(const void * mem1, const void *mem2, size_t size)
{
    LOG_INFO("\n");
#ifdef ALMEM_CMPSD_CLANG_WORKAROUND //Work around for rep-cmpsd in Clang.
    return __erms_cmpsw(mem1, mem2, size);
#else
    return __erms_cmpsd(mem1, mem2, size);
#endif
}

// memcmp with quad word rep move instruciton:REP CMPSQ
int __memcmp_erms_q_aligned(const void * mem1, const void *mem2, size_t size)
{
    LOG_INFO("\n");
    return __erms_cmpsq(mem1, mem2, size);
}

