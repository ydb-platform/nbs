/* SPDX-License-Identifier: MIT */
/* SPDX-FileCopyrightText: 2024 Michael Jeanson <mjeanson@efficios.com> */
/* SPDX-FileCopyrightText: 2023 Huang Pei <huangpei@loongson.cn> */

/*
 * rseq/arch/loongarch/thread-pointer.h
 */

#ifndef _RSEQ_LOONGARCH_THREAD_POINTER
#define _RSEQ_LOONGARCH_THREAD_POINTER

#include <features.h>

#ifdef __cplusplus
extern "C" {
#endif

#if __GNUC_PREREQ (13, 3)
static inline __attribute__((always_inline))
void *rseq_thread_pointer(void)
{
	return __builtin_thread_pointer();
}
#else
static inline __attribute__((always_inline))
void *rseq_thread_pointer(void)
{
	register void *__result asm ("$2");
	asm ("" : "=r" (__result));
	return __result;
}
#endif /* !GCC 13.3 */

#ifdef __cplusplus
}
#endif

#endif
