/* Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.
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

#include "threshold.h"
#include "logger.h"
#include "zen_cpu_info.h"

HIDDEN_SYMBOL uint64_t __repmov_start_threshold __attribute__((aligned(64))) = 0xffffffff;
HIDDEN_SYMBOL uint64_t __repmov_stop_threshold __attribute__((aligned(64))) = 0xffffffff;
HIDDEN_SYMBOL uint64_t __repstore_start_threshold __attribute__((aligned(64))) = 0xffffffff;
HIDDEN_SYMBOL uint64_t __repstore_stop_threshold __attribute__((aligned(64))) = 0xffffffff;
HIDDEN_SYMBOL uint64_t __nt_start_threshold __attribute__((aligned(64))) = 0xffffffff;
HIDDEN_SYMBOL uint64_t __nt_stop_threshold __attribute__((aligned(64))) = 0xffffffff;

static inline void compute_sys_thresholds(cpu_info *zen_info)
{
    get_cache_info(zen_info);
    if (zen_info->zen_cpu_features.erms == ENABLED)
    {
        // Zen5/Zen6 rep-movsb crossover is later than earlier arches
        if (zen_info->zen_cpu_features.movdiri == ENABLED || zen_info->zen_cpu_features.avx512_fp16 == ENABLED)
            zen_info->zen_thresholds.repmov_start_threshold = REPMOV_START_THRESHOLD_ZEN5;
        else
            zen_info->zen_thresholds.repmov_start_threshold = REPMOV_START_THRESHOLD_DEFAULT;
        zen_info->zen_thresholds.repmov_stop_threshold = zen_info->zen_cache_info.l2_per_core;
        zen_info->zen_thresholds.repstore_start_threshold = zen_info->zen_cache_info.l2_per_core;
        zen_info->zen_thresholds.repstore_stop_threshold = zen_info->zen_cache_info.l3_per_ccx;
    } else
    {
        // ERMS unavailable: set repmov_start past any valid size so the runtime
        // threshold (not a compile-time guard) keeps the rep-movsb tier off.
        zen_info->zen_thresholds.repmov_start_threshold = REPMOV_DISABLED;
        zen_info->zen_thresholds.repmov_stop_threshold = 0;
        zen_info->zen_thresholds.repstore_start_threshold = 0;
        zen_info->zen_thresholds.repstore_stop_threshold = 0;
    }
    // Zen5/Zen6
    if (zen_info->zen_cpu_features.movdiri == ENABLED ||
        zen_info->zen_cpu_features.avx512_fp16 == ENABLED) // TODO: Check for zen6 thresholds 
        zen_info->zen_thresholds.nt_start_threshold = COMPUTE_NT_THRESHOLD_ZEN5(zen_info->zen_cache_info.l3_per_ccx);
    // Zen4 and earlier (NT stores start from 3/4 of L3 cache)
    else
        zen_info->zen_thresholds.nt_start_threshold = COMPUTE_NT_MOV_THRESHOLD(zen_info->zen_cache_info.l3_per_ccx);
    zen_info->zen_thresholds.nt_stop_threshold = -1;
}

static inline void configure_thresholds()
{
    if (active_threshold_cfg == SYS_CFG)
    {
        __repmov_start_threshold = zen_info.zen_thresholds.repmov_start_threshold;
        __repmov_stop_threshold = zen_info.zen_thresholds.repmov_stop_threshold;
        __repstore_start_threshold = zen_info.zen_thresholds.repstore_start_threshold;
        __repstore_stop_threshold = zen_info.zen_thresholds.repstore_stop_threshold;
        __nt_start_threshold = zen_info.zen_thresholds.nt_start_threshold;
        __nt_stop_threshold = zen_info.zen_thresholds.nt_stop_threshold;
    }

#ifdef ALMEM_TUNABLES

    else if (active_threshold_cfg == USR_CFG)
    {
        __repmov_start_threshold = user_config.user_threshold.repmov_start_threshold;
        __repmov_stop_threshold = user_config.user_threshold.repmov_stop_threshold;
        __nt_start_threshold = user_config.user_threshold.nt_start_threshold;
        __nt_stop_threshold = user_config.user_threshold.nt_stop_threshold;
    }
#endif
    LOG_DEBUG("%s: repmov[%lu, %lu], repstore[%lu, %lu], non_temporal[%lu, %lu]\n",
              active_threshold_cfg == SYS_CFG ? "System " : "User ", __repmov_start_threshold, __repmov_stop_threshold,
              __repstore_start_threshold, __repstore_stop_threshold, __nt_start_threshold, __nt_stop_threshold);
}
