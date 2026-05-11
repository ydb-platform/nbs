// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2016 Mathieu Desnoyers <mathieu.desnoyers@efficios.com>
// SPDX-FileCopyrightText: 2026 Michael Jeanson <mjeanson@efficios.com>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <syscall.h>
#include <assert.h>
#include <signal.h>
#include <limits.h>
#include <dlfcn.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/auxv.h>
#include <linux/auxvec.h>

#include <rseq/rseq.h>
#include "smp.h"

#ifndef AT_RSEQ_FEATURE_SIZE
# define AT_RSEQ_FEATURE_SIZE		27
#endif

#ifndef AT_RSEQ_ALIGN
# define AT_RSEQ_ALIGN			28
#endif


/*
 * Private internal variables.
 */

static pthread_mutex_t init_lock = PTHREAD_MUTEX_INITIALIZER;
static int init_done;

static const ptrdiff_t *libc_rseq_offset_p;
static const unsigned int *libc_rseq_size_p;
static const unsigned int *libc_rseq_flags_p;


/*
 * Public API variables.
 */

/* Offset from the thread pointer to the rseq area. */
ptrdiff_t rseq_offset = PTRDIFF_MIN;

/* Size of the active rseq features. 0 if the registration failed. */
unsigned int rseq_size = -1U;

/* Flags used at rseq registration. */
unsigned int rseq_flags = 0;


/*
 * Private util functions.
 */

/* rseq syscall wrapper. */
static
int sys_rseq(struct rseq_abi *rseq_abi, uint32_t rseq_len,
		    int flags, uint32_t sig)
{
	return syscall(__NR_rseq, rseq_abi, rseq_len, flags, sig);
}

/* getcpu syscall wrapper. */
static
int sys_getcpu(unsigned int *cpu, unsigned int *node)
{
	return syscall(__NR_getcpu, cpu, node, NULL);
}

/*
 * Return the feature size supported by the kernel.
 *
 * Depending on the value returned by getauxval(AT_RSEQ_FEATURE_SIZE):
 *
 * 0:   Return RSEQ_ABI_ORIG_FEATURE_SIZE (20)
 * > 0: Return the value from getauxval(AT_RSEQ_FEATURE_SIZE).
 *
 * It never returns a value below RSEQ_ABI_ORIG_FEATURE_SIZE.
 */
static
unsigned int get_rseq_kernel_feature_size(void)
{
	unsigned long auxv_rseq_feature_size = getauxval(AT_RSEQ_FEATURE_SIZE);

	if (auxv_rseq_feature_size)
		return auxv_rseq_feature_size;
	else
		return RSEQ_ABI_ORIG_FEATURE_SIZE;
}


/*
 * Public API functions.
 */

/*
 * Initialize librseq, must be called once per process.
 */
int rseq_init(void)
{
	/*
	 * Ensure initialization is only done once. Use load-acquire to
	 * observe the initialization performed by a concurrently
	 * running thread.
	 */
	if (rseq_smp_load_acquire(&init_done))
		return RSEQ_INIT_OK;

	/*
	 * Take the mutex, check the initialization flag again and atomically
	 * set it to ensure we are the only thread doing the initialization.
	 */
	pthread_mutex_lock(&init_lock);
	if (rseq_smp_load_acquire(&init_done))
		goto unlock_ok;

	/*
	 * Get the libc rseq public symbols, all 3 are required for a
	 * successful initialization.
	 */
	libc_rseq_offset_p = dlsym(RTLD_NEXT, "__rseq_offset");
	libc_rseq_size_p = dlsym(RTLD_NEXT, "__rseq_size");
	libc_rseq_flags_p = dlsym(RTLD_NEXT, "__rseq_flags");
	if (!libc_rseq_size_p || !libc_rseq_offset_p || !libc_rseq_flags_p) {
		pthread_mutex_unlock(&init_lock);
		return RSEQ_INIT_ERROR_MISSING_SYMBOLS;
	}

	/*
	 * Older versions of Glibc expose a rseq feature size of 32 bytes even
	 * though the kernel only supported 20 bytes initially. Glibc 2.40
	 * exposes a fixed feature size of 20 bytes, while still allocating a
	 * 32 bytes area.
	 *
	 * Treat both 32 and 20 bytes as special-cases using the following
	 * value as active feature size:
	 *
	 *   rseq_size = min(32, max(20, getauxval(AT_RSEQ_FEATURE_SIZE)));
	 *
	 * Otherwise, use the rseq_size from libc directly.
	 */
	switch (*libc_rseq_size_p) {
	case RSEQ_ABI_ORIG_FEATURE_SIZE:	/* Fallthrough. */
	case RSEQ_ABI_ORIG_ALLOC_SIZE:
	{
		unsigned int rseq_kernel_feature_size = get_rseq_kernel_feature_size();

		if (rseq_kernel_feature_size < RSEQ_ABI_ORIG_ALLOC_SIZE)
			rseq_size = rseq_kernel_feature_size;
		else
			rseq_size = RSEQ_ABI_ORIG_ALLOC_SIZE;
		break;
	}
	default:
		/* Otherwise just use the __rseq_size from libc as rseq_size. */
		rseq_size = *libc_rseq_size_p;
		break;
	}

	/* Copy the libc rseq offset and flags values. */
	rseq_offset = *libc_rseq_offset_p;
	rseq_flags = *libc_rseq_flags_p;

	/*
	 * Set init_done with store-release, to make sure concurrently
	 * running threads observe the initialized state.
	 */
	rseq_smp_store_release(&init_done, 1);
unlock_ok:
	pthread_mutex_unlock(&init_lock);
	return RSEQ_INIT_OK;
}

/*
 * Returns true if rseq is supported. The query types are:
 *
 *   RSEQ_AVAILABLE_QUERY_KERNEL:
 *     Returns true if the rseq syscall is available.
 *
 *   RSEQ_AVAILABLE_QUERY_LIBC:
 *     Returns true if the libc exposes the rseq symbols.
 */
bool rseq_available(unsigned int query)
{
	int rc;

	switch (query) {
	case RSEQ_AVAILABLE_QUERY_KERNEL:
		rc = sys_rseq(NULL, 0, 0, 0);
		if (rc != -1)
			abort();
		switch (errno) {
		case ENOSYS:
			break;
		case EINVAL:
			return true;
		default:
			abort();
		}
		break;
	case RSEQ_AVAILABLE_QUERY_LIBC:
		libc_rseq_offset_p = dlsym(RTLD_NEXT, "__rseq_offset");
		libc_rseq_size_p = dlsym(RTLD_NEXT, "__rseq_size");
		libc_rseq_flags_p = dlsym(RTLD_NEXT, "__rseq_flags");
		if (libc_rseq_offset_p && libc_rseq_size_p && libc_rseq_flags_p)
			return true;
		break;
	default:
		break;
	}
	return false;
}

/*
 * Slow fallback to get the current CPU number.
 */
int32_t rseq_fallback_current_cpu(void)
{
	int32_t cpu;

	cpu = sched_getcpu();
	if (cpu < 0) {
		perror("sched_getcpu()");
		abort();
	}
	return cpu;
}

/*
 * Slow fallback to get the current node number.
 */
int32_t rseq_fallback_current_node(void)
{
	uint32_t cpu_id, node_id;
	int ret;

	ret = sys_getcpu(&cpu_id, &node_id);
	if (ret) {
		perror("sys_getcpu()");
		return ret;
	}
	return (int32_t) node_id;
}

/*
 * Returns the max_nr_cpus auto-detected at pool creation when invoked
 * with @nr_max_cpus=0 argument.
 */
int rseq_get_max_nr_cpus(void)
{
	return get_possible_cpus_array_len();
}
