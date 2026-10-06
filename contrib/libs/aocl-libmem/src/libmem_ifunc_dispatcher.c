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

#include "zen_cpu_info.h"
#include "libmem_impls.h"


/* __builtin_cpu_is("znver6") is only recognized starting with GCC 16.
 * On older compilers fall back to the manual CPUID-based resolver below,
 * which detects Zen6 via the AVX512_FP16 feature bit. */
#if defined(__GNUC__) && (__GNUC__ >= 16)

#define AMD_ZEN_CPU_RESOLVER(func) \
    __builtin_cpu_init(); \
    do { \
        if (__builtin_cpu_is("znver6")) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen6); \
        if (__builtin_cpu_is("znver5")) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen5); \
        if (__builtin_cpu_is("znver4")) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen4); \
        if (__builtin_cpu_is("znver3")) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen3); \
        if (__builtin_cpu_is("znver2")) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen2); \
        if (__builtin_cpu_is("znver1")) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen1); \
        return __ALMEM_CONCAT(__ALMEM_PREFIX(func), system); \
    } while (0)

#else // Compilers that do not detect the latest architecture: 'znver6'
#define AMD_ZEN_CPU_RESOLVER(func) \
   do { \
    uint32_t eax= 0x7;        \
    uint32_t ebx = 0;         \
    uint32_t ecx = 0;         \
    uint32_t edx = 0;         \
      asm volatile ("cpuid"   \
     :"=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) \
     :"0"(eax), "2"(ecx) ); \
    if (ebx & AVX512_MASK)   \
    {       \
         if (edx & AVX512_FP16_MASK) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen6); \
         if (ecx & MOVDIRI_MASK) \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen5); \
         return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen4); \
    }   \
    if (ebx & AVX2_MASK) \
    {   \
        if (ecx & VPCLMULQDQ_MASK)   \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen3); \
        else if (ecx & RDPID_MASK)   \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen2); \
        else if (ecx & RDSEED_MASK)  \
            return __ALMEM_CONCAT(__ALMEM_PREFIX(func), zen1); \
    }   \
    return __ALMEM_CONCAT(__ALMEM_PREFIX(func), system); \
} while(0)

#endif


/* Macro to define ifunc resolver functions using predefined typedefs */
#define DEF_IFUNC_RESOLVER(func) \
__attribute__((ifunc("libmem_" #func "_resolver"))) \
    extern typeof(*(amd_##func##_fn)0) func; \
amd_##func##_fn libmem_##func##_resolver(void) \
{ \
    AMD_ZEN_CPU_RESOLVER(func); \
}

/* Define all IFUNC resolvers using the simplified macro */
DEF_IFUNC_RESOLVER(memcpy)
DEF_IFUNC_RESOLVER(mempcpy)
DEF_IFUNC_RESOLVER(memmove)
DEF_IFUNC_RESOLVER(memset)
DEF_IFUNC_RESOLVER(memcmp)
DEF_IFUNC_RESOLVER(memchr)
DEF_IFUNC_RESOLVER(strcpy)
DEF_IFUNC_RESOLVER(strncpy)
DEF_IFUNC_RESOLVER(strcmp)
DEF_IFUNC_RESOLVER(strncmp)
DEF_IFUNC_RESOLVER(strcat)
DEF_IFUNC_RESOLVER(strncat)
DEF_IFUNC_RESOLVER(strstr)
DEF_IFUNC_RESOLVER(strlen)
DEF_IFUNC_RESOLVER(strnlen)
DEF_IFUNC_RESOLVER(strchr)
DEF_IFUNC_RESOLVER(strrchr)
DEF_IFUNC_RESOLVER(strspn)
