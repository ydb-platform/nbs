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
#include <immintrin.h>
#include "amd_memcpy.h"
#include "logger.h"
#include "threshold.h"
#include "zen_cpu_info.h"
extern cpu_info zen_info;

void * __memcpy_threshold(void *dst, const void *src, size_t size)
{
    LOG_INFO("\n");

    if (size > __repmov_start_threshold && size < __repmov_stop_threshold)
        return __memcpy_erms_b_aligned(dst, src, size);
    else if (size > __nt_start_threshold && size < __nt_stop_threshold)
    {
        if (zen_info.zen_cpu_features.avx512 == ENABLED)
            return __memcpy_avx512_nt_store(dst, src, size);
        return __memcpy_avx2_nt_store(dst, src, size);
    }
    else
    {
        if (zen_info.zen_cpu_features.avx512 == ENABLED)
            return __memcpy_avx512_unaligned(dst, src, size);
        return __memcpy_avx2_unaligned(dst, src, size);
    }
}

