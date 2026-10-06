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
#ifndef _STRNCPY_H_
#define _STRNCPY_H_
#include "almem_defs.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef char * (*amd_strncpy_fn)(char *, const char *, size_t);

//Micro architecture specifc implementations.
HIDDEN_SYMBOL extern char * __strncpy_zen1(char *,const char *, size_t);
HIDDEN_SYMBOL extern char * __strncpy_zen2(char *,const char *, size_t);
HIDDEN_SYMBOL extern char * __strncpy_zen3(char *,const char *, size_t);
HIDDEN_SYMBOL extern char * __strncpy_zen4(char *,const char *, size_t);
HIDDEN_SYMBOL extern char * __strncpy_zen5(char *,const char *, size_t);
HIDDEN_SYMBOL extern char * __strncpy_zen6(char *, const char *, size_t);

//System solution which takes in system config and  threshold values.
HIDDEN_SYMBOL extern char * __strncpy_system(char *,const char *, size_t);

#ifdef __cplusplus
}
#endif

#endif
