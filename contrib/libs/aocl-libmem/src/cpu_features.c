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
#include "zen_cpu_info.h"

HIDDEN_SYMBOL cpu_info zen_info;

static inline void __get_cpu_features(cpuid_registers *cpuid_regs)
{
    asm volatile
    (
     "cpuid"
     :"=a"(cpuid_regs->eax), "=b"(cpuid_regs->ebx), "=c"(cpuid_regs->ecx), "=d"(cpuid_regs->edx)
     :"0"(cpuid_regs->eax), "2"(cpuid_regs->ecx)
    );
}

static inline bool is_amd()
{
    cpuid_registers cpuid_regs;

    cpuid_regs.eax= 0x0;
    cpuid_regs.ecx = 0;
    __get_cpu_features(&cpuid_regs);
    if ((cpuid_regs.ebx ^ 0x68747541) |
            (cpuid_regs.edx ^ 0x69746E65) |
                 (cpuid_regs.ecx ^ 0x444D4163))
    {
        return false;
    }
    return true;
}

static inline void get_cpu_capabilities()
{
    cpuid_registers cpuid_regs;

    cpuid_regs.eax= 0x7;
    cpuid_regs.ecx = 0;
    __get_cpu_features(&cpuid_regs);

    if (cpuid_regs.ebx & AVX512_MASK)
    {
        zen_info.zen_cpu_features.avx512 = ENABLED;
        LOG_INFO("CPU feature AVX512 Enabled\n");
    }
    if (cpuid_regs.ebx & AVX2_MASK)
    {
        zen_info.zen_cpu_features.avx2 = ENABLED;
        LOG_INFO("CPU feature AVX2 Enabled\n");
    }
    if (cpuid_regs.ebx & ERMS_MASK)
    {
        zen_info.zen_cpu_features.erms = ENABLED;
        LOG_INFO("CPU feature ERMS Enabled\n");
    }
    if (cpuid_regs.edx & FSRM_MASK)
    {
        zen_info.zen_cpu_features.fsrm = ENABLED;
        LOG_INFO("CPU feature FSRM Enabled\n");
    }
    if (cpuid_regs.ecx & MOVDIRI_MASK)
    {
        zen_info.zen_cpu_features.movdiri = ENABLED;
        LOG_INFO("CPU feature MOVDIRI Enabled\n");
    }
    if (cpuid_regs.ecx & VPCLMULQDQ_MASK)
    {
        zen_info.zen_cpu_features.vpclmul = ENABLED;
        LOG_INFO("CPU feature VPCLMULQDQ Enabled\n");
    }
    if (cpuid_regs.ecx & RDPID_MASK)
    {
        zen_info.zen_cpu_features.rdpid = ENABLED;
        LOG_INFO("CPU feature RDPID Enabled\n");
    }
    if (cpuid_regs.ecx & RDSEED_MASK)
    {
        zen_info.zen_cpu_features.rdseed = ENABLED;
        LOG_INFO("CPU feature RDSEED Enabled\n");
    }
    if (cpuid_regs.edx & AVX512_FP16_MASK)
    {
        zen_info.zen_cpu_features.avx512_fp16 = ENABLED;
        LOG_INFO("CPU feature AVX512_FP16 Enabled\n");
    }
}
