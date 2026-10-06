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

#include "../include/threshold.h"
#include "../include/zen_cpu_info.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ARCH_ZEN1 1
#define ARCH_ZEN2 2
#define ARCH_ZEN3 3
#define ARCH_ZEN4 4
#define ARCH_ZEN5 5
#define ARCH_ZEN6 6
#define ARCH_UNDF 255

#define NON_AMD_CPU 0
#define AMD_CPU 1

static void __get_cpu_id(cpuid_registers *cpuid_regs)
{
    asm volatile("cpuid"
                 : "=a"(cpuid_regs->eax), "=b"(cpuid_regs->ebx), "=c"(cpuid_regs->ecx), "=d"(cpuid_regs->edx)
                 : "0"(cpuid_regs->eax), "2"(cpuid_regs->ecx));
}

static uint64_t get_cache_bytes(cpuid_registers *cpuid_regs, uint8_t cache_level)
{
    cpuid_regs->eax = 0x8000001D;
    cpuid_regs->ecx = cache_level;
    __get_cpu_id(cpuid_regs);
    return (uint64_t) ((((cpuid_regs->ebx >> 22) & 0x3ff) + 1) *
                       (((cpuid_regs->ebx & 0xfff) + 1) * (cpuid_regs->ecx + 1)));
}

int main(int argc, char **argv)
{
    cpuid_registers cpuid_regs;

    if (argc < 2)
        return -1;

    const char *cmd = argv[1];

    /* Architecture */
    if (strcmp(cmd, "arch") == 0)
    {
        cpuid_regs.eax = 0x7;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        if (cpuid_regs.ebx & AVX512_MASK)
        {
            if (cpuid_regs.edx & AVX512_FP16_MASK)
                return ARCH_ZEN6;
            if (cpuid_regs.ecx & MOVDIRI_MASK)
                return ARCH_ZEN5;
            return ARCH_ZEN4;
        } else if (cpuid_regs.ebx & AVX2_MASK)
        {
            if (cpuid_regs.ecx & VPCLMULQDQ_MASK)
                return ARCH_ZEN3;
            if (cpuid_regs.ecx & RDPID_MASK)
                return ARCH_ZEN2;
            if (cpuid_regs.ebx & RDSEED_MASK)
                return ARCH_ZEN1;
        }
        return ARCH_UNDF;
    }

    if (strcmp(cmd, "vendor") == 0)
    {
        cpuid_regs.eax = 0x0;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        if ((cpuid_regs.ebx ^ 0x68747541) | (cpuid_regs.edx ^ 0x69746E65) | (cpuid_regs.ecx ^ 0x444D4163))
            return NON_AMD_CPU;
        return AMD_CPU;
    }

    /* Features */
    if (strcmp(cmd, "feature_avx2") == 0)
    {
        cpuid_regs.eax = 0x7;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        return !!(cpuid_regs.ebx & AVX2_MASK);
    }

    if (strcmp(cmd, "feature_avx512") == 0)
    {
        cpuid_regs.eax = 0x7;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        return !!(cpuid_regs.ebx & AVX512_MASK);
    }

    if (strcmp(cmd, "feature_erms") == 0)
    {
        cpuid_regs.eax = 0x7;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        return !!(cpuid_regs.ebx & ERMS_MASK);
    }

    if (strcmp(cmd, "feature_fsrm") == 0)
    {
        cpuid_regs.eax = 0x7;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        return !!(cpuid_regs.edx & FSRM_MASK);
    }

    if (strcmp(cmd, "feature_movdiri") == 0)
    {
        cpuid_regs.eax = 0x7;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        return !!(cpuid_regs.ecx & MOVDIRI_MASK);
    }

    /* Cache sizes */
    if (strcmp(cmd, "l1_dcache") == 0)
        return (int) (get_cache_bytes(&cpuid_regs, 0x0) >> 10);

    if (strcmp(cmd, "l2_cache") == 0)
        return (int) (get_cache_bytes(&cpuid_regs, 0x2) >> 19);

    if (strcmp(cmd, "l3_cache") == 0)
        return (int) (get_cache_bytes(&cpuid_regs, 0x3) >> 20);

    if (strcmp(cmd, "nt_threshold") == 0)
    {
        uint64_t l3_bytes = get_cache_bytes(&cpuid_regs, 0x3);
        cpuid_regs.eax = 0x7;
        cpuid_regs.ecx = 0;
        __get_cpu_id(&cpuid_regs);
        /* Zen5/Zen6: NT moves/stores start from L3 */
        if ((cpuid_regs.ecx & MOVDIRI_MASK) ||
            (cpuid_regs.edx & AVX512_FP16_MASK))
            return (int) (COMPUTE_NT_THRESHOLD_ZEN5(l3_bytes) >> 20); // TODO: Check for zen6 thresholds 
        /* Pre-Zen5: NT moves start from 3/4 of L3 */
        return (int) (COMPUTE_NT_MOV_THRESHOLD(l3_bytes) >> 20);
    }

    if (strcmp(cmd, "aligned_vec_cpy_threshold") == 0)
        return (int) (COMPUTE_ALIGNED_VEC_MOV_TH(get_cache_bytes(&cpuid_regs, 0x0)) >> 10);

    if (strcmp(cmd, "aligned_vec_set_threshold") == 0)
        return (int) (COMPUTE_ALIGNED_VEC_STORE_TH(get_cache_bytes(&cpuid_regs, 0x0)) >> 10);

    if (strcmp(cmd, "zen5_nt_store_threshold") == 0)
        return (int) (COMPUTE_NT_THRESHOLD_ZEN5(get_cache_bytes(&cpuid_regs, 0x3)) >> 20);

    fprintf(stderr, "Error: Unknown command '%s'\n", cmd);
    return -1;
}
