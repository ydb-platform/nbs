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

#include <string.h>
#include <stdlib.h>
#include "logger.h"
#include "zen_cpu_info.h"

extern config active_operation_cfg;
extern config active_threshold_cfg;
HIDDEN_SYMBOL user_cfg user_config;

extern char **environ;
extern char **_dl_argv;

static inline char ** get_environ() {
    int argc = *(int*)(_dl_argv - 1);
    char **my_environ = (char**)(_dl_argv + argc + 1);
    return my_environ;
}

static inline int string_compare(char *str1, char *str2)
{
	while(*str1 && *(str1++) == *(str2++));
	return (*(unsigned char *)str1 - *(unsigned char *)str2);
}

void parse_env_operation_cfg(void)
{
    char * user_operation_cfgs, *token;
    environ = get_environ();

    user_operation_cfgs = getenv("LIBMEM_OPERATION");
    if (user_operation_cfgs == NULL)
    {
        LOG_DEBUG("Environment variable LIBMEM_OPERATION is not set.\n");
        return;
    }
    LOG_DEBUG("LIBMEM_OPERATION: %s\n", user_operation_cfgs);

    token = strtok(user_operation_cfgs,",");

    if (token == NULL)
    {
        LOG_WARN("Failed to parse <CPU_OPERATION>\n");
        return;
    }

    if (!string_compare(token, "avx512"))
        user_config.user_operation.avx512 = 1;
    else if (!string_compare(token, "avx2"))
        user_config.user_operation.avx2 = 1;
    else if (!string_compare(token, "erms"))
        user_config.user_operation.erms = 1;
    else
    {
        LOG_WARN("invalid CPU operation: %s\n", token);
        return;
    }

    token = (char *)strtok(NULL, ",");
    if(token == NULL)
    {
        LOG_WARN("Failed to parse <SRC_ALIGNMENT>.\n");
        return;
    }
    //invalid input is considered as unaligned.
    user_config.src_aln = (*token=='w')?w_align:(*token=='d')?d_align: \
                  (*token=='q')?q_align:(*token=='x')?x_align: \
                  (*token=='y')?y_align:(*token=='n')?n_align: \
                  u_align;

    token = (char *)strtok(NULL, ":");

    if(token == NULL)
    {
        LOG_WARN("Failed to parse <DST_ALIGNMENT>.\n");
        return;
    }
    //invalid input is considered as unaligned.
    user_config.dst_aln = (*token=='w')?w_align:(*token=='d')?d_align: \
                  (*token=='q')?q_align:(*token=='x')?x_align: \
                  (*token=='y')?y_align:(*token=='n')?n_align: \
                  u_align;

    active_operation_cfg = USR_CFG;
}

void parse_env_threshold_cfg(void)
{
    char * user_threshold_cfg, *token;

    user_threshold_cfg = getenv("LIBMEM_THRESHOLD");
    if (user_threshold_cfg == NULL)
    {
        LOG_DEBUG("Environment variable LIBMEM_THRESHOLD is not set.\n");
        return;
    }

    token = (char *)strtok(user_threshold_cfg, ",");
    if (token == NULL)
    {
        LOG_WARN("Failed to parse <repmov_start_threshold>.\n");
        return;
    }
    user_config.user_threshold.repmov_start_threshold = strtoull(token, NULL, 10);

    token = (char *)strtok(NULL, ",");
    if (token == NULL)
    {
        LOG_WARN("Failed to parse <repmov_stop_threshold>.\n");
        return;
    }
    user_config.user_threshold.repmov_stop_threshold = strtoull(token,NULL,10);

    token = (char *)strtok(NULL, ",");
    if (token == NULL)
    {
        LOG_WARN("Failed to parse <nt_start_threshold>.\n");
        return;
    }
    user_config.user_threshold.nt_start_threshold = strtoull(token,NULL,0);
    token = (char *)strtok(NULL, ":");
    if (token == NULL)
    {
        LOG_WARN("Failed to parse <nt_stop_threshold>.\n");
        return;
    }
    user_config.user_threshold.nt_stop_threshold = strtoull(token,NULL,0);

    active_threshold_cfg = USR_CFG;
}
