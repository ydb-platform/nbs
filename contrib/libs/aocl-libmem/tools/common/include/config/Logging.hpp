/* Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef LIBMEM_COMMON_LOGGING_HPP
#define LIBMEM_COMMON_LOGGING_HPP

#include <cstdio>

#ifdef LIBMEM_VERBOSE
    #define LIBMEM_DEBUG(fmt, ...)      std::printf("[DEBUG] " fmt "\n", ##__VA_ARGS__)
    #define LIBMEM_INFO(fmt, ...)       std::printf("[INFO]  " fmt "\n", ##__VA_ARGS__)
    #define LIBMEM_PASS(fmt, ...)       std::printf("[PASS]  " fmt "\n", ##__VA_ARGS__)
    #define LIBMEM_SEPARATOR()          std::printf("[DEBUG] ========================================\n")
    #define LIBMEM_SUBSEPARATOR()       std::printf("[DEBUG] ----------------------------------------\n")
#else
    #define LIBMEM_DEBUG(fmt, ...)      do {} while(0)
    #define LIBMEM_INFO(fmt, ...)       do {} while(0)
    #define LIBMEM_PASS(fmt, ...)       do {} while(0)
    #define LIBMEM_SEPARATOR()          do {} while(0)
    #define LIBMEM_SUBSEPARATOR()       do {} while(0)
#endif

// Always-on summary line (printed even with LIBMEM_VERBOSE off).
#define LIBMEM_SUMMARY(fmt, ...)        std::printf("[SUMMARY] " fmt "\n", ##__VA_ARGS__)

#endif // LIBMEM_COMMON_LOGGING_HPP
