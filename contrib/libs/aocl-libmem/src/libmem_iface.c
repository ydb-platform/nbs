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
#ifdef ALMEM_DYN_DISPATCH

#include <stddef.h>
#include "libmem_iface.h"

__attribute__((visibility("default"))) amd_memcpy_fn _memcpy_variant = __memcpy_system;

// memcpy mapping
LIBMEM_FN_MAP(memcpy);
WEAK_ALIAS(memcpy, MK_FN_NAME(memcpy));

__attribute__((visibility("default"))) amd_mempcpy_fn _mempcpy_variant = __mempcpy_system;
// mempcpy mapping
LIBMEM_FN_MAP(mempcpy);
WEAK_ALIAS(mempcpy, MK_FN_NAME(mempcpy));

__attribute__((visibility("default"))) amd_memmove_fn _memmove_variant = __memmove_system;
// memmove mapping
LIBMEM_FN_MAP(memmove);
WEAK_ALIAS(memmove, MK_FN_NAME(memmove));

__attribute__((visibility("default"))) amd_memset_fn _memset_variant = __memset_system;
// memset mapping
LIBMEM_FN_MAP(memset);
WEAK_ALIAS(memset, MK_FN_NAME(memset));

__attribute__((visibility("default"))) amd_memcmp_fn _memcmp_variant = __memcmp_system;
// memcmp mapping
LIBMEM_FN_MAP(memcmp);
WEAK_ALIAS(memcmp, MK_FN_NAME(memcmp));

__attribute__((visibility("default")))  amd_memchr_fn _memchr_variant = __memchr_system;
// memchr mapping
LIBMEM_FN_MAP(memchr);
WEAK_ALIAS(memchr, MK_FN_NAME(memchr));

__attribute__((visibility("default")))  amd_strcpy_fn _strcpy_variant = __strcpy_system;
// strcpy mapping
LIBMEM_FN_MAP(strcpy);
WEAK_ALIAS(strcpy, MK_FN_NAME(strcpy));

__attribute__((visibility("default"))) amd_strncpy_fn _strncpy_variant = __strncpy_system;
// strncpy mapping
LIBMEM_FN_MAP(strncpy);
WEAK_ALIAS(strncpy, MK_FN_NAME(strncpy));

__attribute__((visibility("default"))) amd_strcmp_fn _strcmp_variant  = __strcmp_system;
// strcmp mapping
LIBMEM_FN_MAP(strcmp);
WEAK_ALIAS(strcmp, MK_FN_NAME(strcmp));

__attribute__((visibility("default"))) amd_strncmp_fn _strncmp_variant = __strncmp_system;
// strncmp mapping
LIBMEM_FN_MAP(strncmp);
WEAK_ALIAS(strncmp, MK_FN_NAME(strncmp));

__attribute__((visibility("default"))) amd_strcat_fn _strcat_variant = __strcat_system;
// strcat mapping
LIBMEM_FN_MAP(strcat);
WEAK_ALIAS(strcat, MK_FN_NAME(strcat));

__attribute__((visibility("default"))) amd_strncat_fn _strncat_variant  = __strncat_system;
// strncat mapping
LIBMEM_FN_MAP(strncat);
WEAK_ALIAS(strncat, MK_FN_NAME(strncat));

__attribute__((visibility("default"))) amd_strstr_fn _strstr_variant  = __strstr_system;
// strstr mapping
LIBMEM_FN_MAP(strstr);
WEAK_ALIAS(strstr, MK_FN_NAME(strstr));

__attribute__((visibility("default"))) amd_strlen_fn _strlen_variant  = __strlen_system;
// strlen mapping
LIBMEM_FN_MAP(strlen);
WEAK_ALIAS(strlen, MK_FN_NAME(strlen));

__attribute__((visibility("default"))) amd_strnlen_fn _strnlen_variant  = __strnlen_system;
// strnlen mapping
LIBMEM_FN_MAP(strnlen);
WEAK_ALIAS(strnlen, MK_FN_NAME(strnlen));

__attribute__((visibility("default"))) amd_strchr_fn _strchr_variant  = __strchr_system;
// strchr mapping
LIBMEM_FN_MAP(strchr);
WEAK_ALIAS(strchr, MK_FN_NAME(strchr));

__attribute__((visibility("default"))) amd_strrchr_fn _strrchr_variant  = __strrchr_system;
// strrchr mapping
LIBMEM_FN_MAP(strrchr);
WEAK_ALIAS(strrchr, MK_FN_NAME(strrchr));

__attribute__((visibility("default"))) amd_strspn_fn _strspn_variant = __strspn_system;
// strspn mapping
LIBMEM_FN_MAP(strspn);
WEAK_ALIAS(strspn, MK_FN_NAME(strspn));

#endif
