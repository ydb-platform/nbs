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
#ifndef _LOGGER_H_
#define _LOGGER_H_

#ifdef __cplusplus
extern "C" {
#endif


#ifdef ALMEM_LOGGER

#include <stdio.h>
#include <stdlib.h>

extern FILE *logfile;
extern const char * log_string[];

#define LOG_FILE "/tmp/libmem.log"

#define CRITICAL 0
#define WARNING  1
#define NOTICE   2
#define INFO     3
#define DEBUG    4

#ifndef LOG_LEVEL
// Setting default logging level to INFO;
#define LOG_LEVEL    INFO
#endif

#define LOG(level, fmt, ...) do {                               \
                         if (level > LOG_LEVEL)                 \
                             break;                             \
                         logfile = fopen(LOG_FILE, "a");        \
                         if (!logfile)                          \
                             break;                             \
                         fprintf(logfile, "[%s] %s@%d: " fmt,   \
                          log_string[level], __func__,          \
                                 __LINE__, ##__VA_ARGS__);      \
                         fclose(logfile);                       \
                         } while (0)


#else
#define LOG(level, fmt, ...) do {        \
                             } while (0)

#endif //ENABLE_LOGGING

#define LOG_DEBUG(fmt, ...)       LOG(DEBUG, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)        LOG(INFO, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(fmt, ...)      LOG(NOTICE, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)        LOG(WARNING, fmt, ##__VA_ARGS__)
#define LOG_CRITICAL(fmt, ...)    LOG(CRITICAL, fmt, ##__VA_ARGS__)


#ifdef __cplusplus
}
#endif

#endif //header
