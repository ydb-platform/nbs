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

#include "cpu_features.c"

static inline void get_cache_info(cpu_info *zen_info)
{
    cpuid_registers cpuid_regs;

    cache_info *zen_cache = &(zen_info->zen_cache_info);

    //get L1D cache details
    cpuid_regs.eax = 0x8000001D;
    cpuid_regs.ecx = 0x0;
    __get_cpu_features(&cpuid_regs);

    //Compute L1D Cache Info
    zen_cache->l1d_per_core = (((cpuid_regs.ebx>>22) & 0x3ff)+1)*((cpuid_regs.ebx & 0xfff)+1)*(cpuid_regs.ecx+1);


    //get L2 cache details
    cpuid_regs.eax = 0x8000001D;
    cpuid_regs.ecx = 0x2;
    __get_cpu_features(&cpuid_regs);

    //Compute L2 Cache Info
    zen_cache->l2_per_core = (((cpuid_regs.ebx>>22) & 0x3ff)+1)*((cpuid_regs.ebx & 0xfff)+1)*(cpuid_regs.ecx+1);

    //get L3 cache details
    cpuid_regs.eax = 0x8000001D;
    cpuid_regs.ecx = 0x3;
    __get_cpu_features(&cpuid_regs);

    //Compute L3 Cache info
    zen_cache->l3_per_ccx =  (((cpuid_regs.ebx>>22) & 0x3ff)+1)*((cpuid_regs.ebx & 0xfff)+1)*(cpuid_regs.ecx+1);

    LOG_DEBUG("L2 Cache/CORE: %lu, L3 Cache/CCX: %lu\n", zen_cache->l2_per_core, zen_cache->l3_per_ccx);
}
