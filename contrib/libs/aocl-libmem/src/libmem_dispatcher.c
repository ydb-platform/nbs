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

#include "libmem_impls.h"
#include "almem_defs.h"
#include "libmem.h"
#include "libmem_iface.c"

extern cpu_info zen_info;

/* Resolver to identify the zen cpu version
 * returns: cpu variant index
 */
static inline cpu_variant_idx libmem_cpu_resolver(void)
{
    cpu_variant_idx cpu_var_idx = SYSTEM;
    // Zen4 and above cpu detection
    if (zen_info.zen_cpu_features.avx512 == ENABLED)
    {
        if (zen_info.zen_cpu_features.avx512_fp16 == ENABLED) // Zen6
        {
            cpu_var_idx = ARCH_ZEN6;
            LOG_INFO("Detected CPU uArch: Zen6\n");
        }
        else if (zen_info.zen_cpu_features.movdiri == ENABLED) // Zen5
        {
            cpu_var_idx = ARCH_ZEN5;
            LOG_INFO("Detected CPU uArch: Zen5\n");
        }
        else
        {
            cpu_var_idx = ARCH_ZEN4;
            LOG_INFO("Detected CPU uArch: Zen4\n");
        }
    }
    // Zen3 and below cpu detection
    else if (zen_info.zen_cpu_features.avx2 == ENABLED)
    {
        if (zen_info.zen_cpu_features.vpclmul == ENABLED) // Zen3
        {
            cpu_var_idx = ARCH_ZEN3;
            LOG_INFO("Detected CPU uArch: Zen3\n");
        }
        else if (zen_info.zen_cpu_features.rdpid == ENABLED) // Zen2
        {
            cpu_var_idx = ARCH_ZEN2;
            LOG_INFO("Detected CPU uArch: Zen2\n");
        }
        else if (zen_info.zen_cpu_features.rdseed == ENABLED) // Zen1
        {
            cpu_var_idx = ARCH_ZEN1;
            LOG_INFO("Detected CPU uArch: Zen1\n");
        }
        else
        {
            //System operation Config
            LOG_INFO("System Operation CFG\n");
            cpu_var_idx = SYSTEM;
        }
    }
    return cpu_var_idx;
}

#ifdef ALMEM_TUNABLES
/* Resolver to identify the tunable config
 * returns: tunable varaint index
 */
static inline tunable_variant_idx libmem_tunable_resolver(void)
{
    tunable_variant_idx tun_var_idx = UNKNOWN;

    if (active_operation_cfg == USR_CFG) //User Operation Config
    {
        LOG_INFO("User Operation Config\n");
        if (user_config.user_operation.avx2) //AVX2 operations
        {
            LOG_DEBUG("AVX2 config\n");
            if (user_config.src_aln == n_align)
            {
                if (user_config.dst_aln == n_align)
                    tun_var_idx = AVX2_NON_TEMPORAL;
                else
                    tun_var_idx = AVX2_NON_TEMPORAL_LOAD;
            }
            else if (user_config.dst_aln == n_align)
                tun_var_idx = AVX2_NON_TEMPORAL_STORE;
            else if (user_config.src_aln == y_align)
            {
                if (user_config.dst_aln == y_align)
                    tun_var_idx = AVX2_ALIGNED;
                else
                    tun_var_idx = AVX2_ALIGNED_LOAD;
            }
            else if (user_config.dst_aln == y_align)
                tun_var_idx = AVX2_ALIGNED_STORE;
            else
                tun_var_idx = AVX2_UNALIGNED;
        }
        else if (user_config.user_operation.avx512) //AVX512 operations
        {
            LOG_DEBUG("AVX512 config\n");
            if (user_config.src_aln == n_align)
            {
                if (user_config.dst_aln == n_align)
                    tun_var_idx = AVX512_NON_TEMPORAL;
                else
                    tun_var_idx = AVX512_NON_TEMPORAL_LOAD;
            }
            else if (user_config.dst_aln == n_align)
                tun_var_idx = AVX512_NON_TEMPORAL_STORE;
            else if (user_config.src_aln == y_align)
            {
                if (user_config.dst_aln == y_align)
                    tun_var_idx = AVX512_ALIGNED;
                else
                    tun_var_idx = AVX512_ALIGNED_LOAD;
            }
            else if (user_config.dst_aln == y_align)
                tun_var_idx = AVX512_ALIGNED_STORE;
            else
                tun_var_idx = AVX512_UNALIGNED;
        }
        else if (user_config.user_operation.erms) //ERMS operations
        {
            LOG_DEBUG("ERMS config\n");
            tun_var_idx = ERMS_MOVSB;
            if (user_config.src_aln == user_config.dst_aln)
            {
                if (user_config.src_aln == q_align \
                    || user_config.src_aln == x_align \
                    || user_config.src_aln == y_align)
                    tun_var_idx = ERMS_MOVSQ;
                else if (user_config.src_aln == d_align)
                    tun_var_idx = ERMS_MOVSD;
                else if (user_config.src_aln == w_align)
                    tun_var_idx = ERMS_MOVSW;
            }
        }
    }
    else if (active_threshold_cfg == USR_CFG) // User Threshold Config
    {
        LOG_INFO("User Threshold CFG.\n");
        tun_var_idx = THRESHOLD;
    }
    return tun_var_idx;
}
#endif // end of tunable resolver

static inline void dispatcher_init()
{
    cpu_variant_idx cpu_var_idx = SYSTEM;
    cpu_var_idx  = libmem_cpu_resolver();

    _memcpy_variant     = (amd_memcpy_fn) libmem_cpu_impls[MEMCPY][cpu_var_idx];
    _mempcpy_variant    = (amd_mempcpy_fn) libmem_cpu_impls[MEMPCPY][cpu_var_idx];
    _memmove_variant    = (amd_memmove_fn) libmem_cpu_impls[MEMMOVE][cpu_var_idx];
    _memset_variant     = (amd_memset_fn) libmem_cpu_impls[MEMSET][cpu_var_idx];
    _memcmp_variant     = (amd_memcmp_fn) libmem_cpu_impls[MEMCMP][cpu_var_idx];
    _memchr_variant     = (amd_memchr_fn) libmem_cpu_impls[MEMCHR][cpu_var_idx];
    _strcpy_variant     = (amd_strcpy_fn) libmem_cpu_impls[STRCPY][cpu_var_idx];
    _strncpy_variant    = (amd_strncpy_fn) libmem_cpu_impls[STRNCPY][cpu_var_idx];
    _strcmp_variant     = (amd_strcmp_fn) libmem_cpu_impls[STRCMP][cpu_var_idx];
    _strncmp_variant    = (amd_strncmp_fn) libmem_cpu_impls[STRNCMP][cpu_var_idx];
    _strcat_variant     = (amd_strcat_fn) libmem_cpu_impls[STRCAT][cpu_var_idx];
    _strncat_variant    = (amd_strncat_fn) libmem_cpu_impls[STRNCAT][cpu_var_idx];
    _strstr_variant     = (amd_strstr_fn) libmem_cpu_impls[STRSTR][cpu_var_idx];
    _strlen_variant     = (amd_strlen_fn) libmem_cpu_impls[STRLEN][cpu_var_idx];
    _strnlen_variant    = (amd_strnlen_fn) libmem_cpu_impls[STRNLEN][cpu_var_idx];
    _strchr_variant     = (amd_strchr_fn) libmem_cpu_impls[STRCHR][cpu_var_idx];
    _strrchr_variant    = (amd_strrchr_fn) libmem_cpu_impls[STRRCHR][cpu_var_idx];
    _strspn_variant     = (amd_strspn_fn) libmem_cpu_impls[STRSPN][cpu_var_idx];

#ifdef ALMEM_TUNABLES
    tunable_variant_idx tun_var_idx = libmem_tunable_resolver();

    //pick the tunable implementation only with valid tunable config
    if (tun_var_idx != UNKNOWN)
    {
        _memcpy_variant     = (amd_memcpy_fn) libmem_cpu_impls[MEMCPY][cpu_var_idx];
        _mempcpy_variant    = (amd_mempcpy_fn) libmem_cpu_impls[MEMPCPY][cpu_var_idx];
        _memmove_variant    = (amd_memmove_fn) libmem_cpu_impls[MEMMOVE][cpu_var_idx];
        _memset_variant     = (amd_memset_fn) libmem_cpu_impls[MEMSET][cpu_var_idx];
        _memcmp_variant     = (amd_memcmp_fn) libmem_cpu_impls[MEMCMP][cpu_var_idx];
    }
#endif //end of tunables
}
