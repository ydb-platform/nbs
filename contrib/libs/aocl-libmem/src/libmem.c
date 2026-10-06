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
#include "logger.h"
#include "almem_defs.h"

HIDDEN_SYMBOL config active_operation_cfg = SYS_CFG;
HIDDEN_SYMBOL config active_threshold_cfg = SYS_CFG;
extern user_cfg user_config;

#include "cache_info.c"
#include "threshold.c"

#ifdef ALMEM_DYN_DISPATCH
#ifdef ALMEM_TUNABLES
#include "libmem_dispatcher.c"
#else //ifunc-dispatching
#include "libmem_ifunc_dispatcher.c"
#endif
#endif //eof DYN_DISPATCH

/* Constructor for libmem library
 * returns: void
 */
static __attribute__((constructor)) void libmem_init()
{
    LOG_INFO("aocl-libmem Version: %s\n", LIBMEM_BUILD_VERSION);
    bool is_amd_cpu = is_amd();
    if (is_amd_cpu == true)
    {
        LOG_INFO("Is AMD CPU\n");
        get_cpu_capabilities();
#ifdef ALMEM_TUNABLES
        parse_env_operation_cfg();
        if (active_operation_cfg == SYS_CFG)
            parse_env_threshold_cfg();
#endif
        if (active_operation_cfg == SYS_CFG && active_threshold_cfg == SYS_CFG)
            compute_sys_thresholds(&zen_info);

        configure_thresholds();
    }
#ifdef ALMEM_TUNABLES
    dispatcher_init();
#endif
}
