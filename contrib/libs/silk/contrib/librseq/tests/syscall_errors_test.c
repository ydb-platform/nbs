// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 Michael Jeanson <mjeanson@efficios.com>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/auxv.h>
#include <syscall.h>
#include <unistd.h>

/*
 * This test is not linked on librseq but uses the headers for some convenience
 * defines like 'ARCH_RSEQ_*'.
 */
#include <rseq/rseq.h>

#include "tap.h"

#define __max(a,b) ((a)>(b)?(a):(b))

#ifndef AT_RSEQ_FEATURE_SIZE
# define AT_RSEQ_FEATURE_SIZE		27
#endif

#ifndef AT_RSEQ_ALIGN
# define AT_RSEQ_ALIGN			28
#endif

#if (RSEQ_BITS_PER_LONG == 64) && (!defined(RSEQ_ARCH_S390))
#define NR_BASE_TESTS 18
#define RUN_RSEQ_INVALID_ADDRESS_TEST 1
#else
#define NR_BASE_TESTS 16
#endif

struct local_rseq {
	uint32_t cpu_id_start;
	uint32_t cpu_id;
	union {
		uint64_t ptr64;

		/*
		 * The "arch" field provides architecture accessor for
		 * the ptr field based on architecture pointer size and
		 * endianness.
		 */
		struct {
#ifdef __LP64__
			uint64_t ptr;
#elif defined(__BYTE_ORDER) ? (__BYTE_ORDER == __BIG_ENDIAN) : defined(__BIG_ENDIAN)
			uint32_t padding;		/* Initialized to zero. */
			uint32_t ptr;
#else
			uint32_t ptr;
			uint32_t padding;		/* Initialized to zero. */
#endif
		} arch;
	} rseq_cs;
	uint32_t flags;
	uint32_t node_id;
	uint32_t mm_cid;
	/* Pad to 1024 bytes. */
	uint8_t padding[992];
};

static
__thread struct local_rseq local_rseq __attribute__((tls_model("initial-exec"), aligned(1024))) = {
	.cpu_id_start = 0,
        .cpu_id = (uint32_t) RSEQ_ABI_CPU_ID_UNINITIALIZED,
	.rseq_cs = {0},
	.flags = 0,
	.node_id = 0,
	.mm_cid = 0,
	.padding = {0},
};

static struct rseq_abi_cs local_rseq_cs = {
        .version = 0,
        .flags = 0,
        .start_ip = 0,
        .post_commit_offset = 0,
        .abort_ip = 0,
};

/*
 * version == 0
 * start_ip < TASK_SIZE
 * start_ip + post_commit_offset < TASK_SIZE
 * abort_ip < TASK_SIZE
 *
 * start_ip + post_commit_offset must not overflow
 * abort_ip - start_ip >= post_commit_offset
 * RSEQ_SIG must be present 4bytes before abort_ip
 */
struct fake_critical_section {
	uint64_t start_ip;
	uint32_t rseq_sig;
	uint64_t abort_ip;
} __attribute__((packed));

static struct fake_critical_section fake_cs = {
	.start_ip = 0,
	.rseq_sig = RSEQ_SIG,
	.abort_ip = 0,
};

static void clear_local_rseq(void)
{
	local_rseq.cpu_id_start = 0;
        local_rseq.cpu_id = (uint32_t) RSEQ_ABI_CPU_ID_UNINITIALIZED;
	local_rseq.rseq_cs.arch.ptr = 0;
	local_rseq.flags = 0;
	local_rseq.node_id = 0;
	local_rseq.mm_cid = 0;
}

static void clear_local_rseq_cs(void)
{
	local_rseq_cs.version = 0;
	local_rseq_cs.flags = 0;
	local_rseq_cs.start_ip = 0;
	local_rseq_cs.post_commit_offset = 0;
	local_rseq_cs.abort_ip = 0;
}

static bool is_local_rseq_clean(void)
{
	if (local_rseq.cpu_id_start != 0 ||
	        local_rseq.cpu_id != (uint32_t) RSEQ_ABI_CPU_ID_UNINITIALIZED ||
		local_rseq.rseq_cs.arch.ptr != 0 ||
		local_rseq.flags != 0 ||
		local_rseq.node_id != 0 ||
		local_rseq.mm_cid != 0)
		return false;

	return true;
}

static int sys_rseq(void *rseq_abi, uint32_t rseq_len,
		    int flags, uint32_t sig)
{
	return syscall(__NR_rseq, rseq_abi, rseq_len, flags, sig);
}

static bool rseq_syscall_available(void)
{
	if (sys_rseq(NULL, 0, 0, 0) == -1) {
		if (errno == EINVAL) {
			return true;
		}
	}
	return false;
}

/*
 * Check the value of errno on some expected failures of the rseq syscall.
 */
static void test_errors(uint32_t rseq_reg_size)
{
	int ret;
	int errno_copy;

	diag("Simple registration with rseq_size == %d", rseq_reg_size);

	/* The current thread is NOT registered. */

	/* EINVAL */
	clear_local_rseq();
	errno = 0;
	ret = sys_rseq(&local_rseq, rseq_reg_size, -1, RSEQ_SIG);
	errno_copy = errno;
	ok(ret != 0 && errno_copy == EINVAL, "Registration with invalid flag fails with errno set to EINVAL (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));
	ok(is_local_rseq_clean(), "Failed registration doesn't write to the rseq area");

	clear_local_rseq();
	errno = 0;
	ret = sys_rseq((char *) &local_rseq + 1, rseq_reg_size, 0, RSEQ_SIG);
	errno_copy = errno;
	ok(ret != 0 && errno_copy == EINVAL, "Registration with unaligned rseq_abi fails with errno set to EINVAL (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));
	ok(is_local_rseq_clean(), "Failed registration doesn't write to the rseq area");

	clear_local_rseq();
	errno = 0;
	ret = sys_rseq(&local_rseq, 31, 0, RSEQ_SIG);
	errno_copy = errno;
	ok(ret != 0 && errno_copy == EINVAL, "Registration with invalid size fails with errno set to EINVAL (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));
	ok(is_local_rseq_clean(), "Failed registration doesn't write to the rseq area");

#if defined(RUN_RSEQ_INVALID_ADDRESS_TEST)
	/*
	 * We haven't found a reliable way to find an invalid address when
	 * running a 32bit userspace on a 64bit kernel, so only run this test
	 * on 64bit builds for the moment.
	 *
	 * Also exclude architectures that select
	 * CONFIG_ALTERNATE_USER_ADDRESS_SPACE where the kernel and userspace
	 * have their own address space and this failure can't happen.
	 */

	/* EFAULT */
	clear_local_rseq();
	errno = 0;
	ret = sys_rseq((void *) -4096UL, rseq_reg_size, 0, RSEQ_SIG);
	errno_copy = errno;
	ok(ret != 0 && errno_copy == EFAULT, "Registration with invalid address fails with errno set to EFAULT (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));
	ok(is_local_rseq_clean(), "Failed registration doesn't write to the rseq area");
#endif

	clear_local_rseq();
	errno = 0;
	ret = sys_rseq(&local_rseq, rseq_reg_size, 0, RSEQ_SIG);
	errno_copy = errno;
	ok(ret == 0, "Registration succeeds for the current thread (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));
	ok((int32_t) RSEQ_READ_ONCE(local_rseq.cpu_id) >= 0, "Successful registration updates cpu_id");

	/* The current thread is registered. */

	/* EBUSY */
	errno = 0;
	ret = sys_rseq(&local_rseq, rseq_reg_size, 0, RSEQ_SIG);
	errno_copy = errno;
	ok(ret != 0 && errno_copy == EBUSY, "Double registration fails with errno set to EBUSY (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));

	/* EPERM */
	errno = 0;
	ret = sys_rseq(&local_rseq, rseq_reg_size, RSEQ_ABI_FLAG_UNREGISTER, RSEQ_SIG + 1);
	errno_copy = errno;
	ok(ret != 0 && errno_copy == EPERM, "Unregistration with wrong RSEQ_SIG fails with errno set to EPERM (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));

	/* EINVAL */
	errno = 0;
	ret = sys_rseq(&local_rseq, rseq_reg_size, RSEQ_ABI_FLAG_UNREGISTER | (1 << 1), RSEQ_SIG);
	errno_copy = errno;
	ok(ret != 0 && errno_copy == EINVAL, "Unregistration with additional flags fails with errno set to EINVAL (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));

	/* Unregister the current thread. */
	errno = 0;
	ret = sys_rseq(&local_rseq, rseq_reg_size, RSEQ_ABI_FLAG_UNREGISTER, RSEQ_SIG);
	errno_copy = errno;
	ok(ret == 0, "Unregistration succeeds for the current thread (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));
}

/*
 * Check that registration succeeds when the rseq_cs field points to a valid
 * 'struct rseq_cs'.
 */
static void test_registration_with_rseq_cs(uint32_t rseq_reg_size)
{
	int ret;
	int errno_copy;

	diag("Registration with non-zero rseq_cs and rseq_size == %d", rseq_reg_size);

	/* The current thread is NOT registered. */

	clear_local_rseq();
	clear_local_rseq_cs();

	/* Craft a dummy critical section that will pass kernel validation. */
	local_rseq_cs.start_ip = (uint64_t) &fake_cs.start_ip;
	local_rseq_cs.post_commit_offset = 8;
	local_rseq_cs.abort_ip = (uint64_t) &fake_cs.abort_ip;

	errno = 0;
	local_rseq.rseq_cs.arch.ptr = (unsigned long) &local_rseq_cs;
	ret = sys_rseq(&local_rseq, rseq_reg_size, 0, RSEQ_SIG);
	errno_copy = errno;
	ok(ret == 0, "Registration succeeds with 'rseq_cs != NULL' (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));

	ok((int32_t) RSEQ_READ_ONCE(local_rseq.cpu_id) >= 0, "Successful registration updates cpu_id");
	ok(RSEQ_READ_ONCE(local_rseq.rseq_cs.arch.ptr) == 0, "Successful registration clears rseq_cs");

	errno = 0;
	ret = sys_rseq(&local_rseq, rseq_reg_size, RSEQ_ABI_FLAG_UNREGISTER, RSEQ_SIG);
	errno_copy = errno;
	ok(ret == 0, "Unregistration succeeds for the current thread (ret = %d, errno = %s)", ret, strerrorname_np(errno_copy));
}

int main(void)
{
	int nr_tests = NR_BASE_TESTS;
	unsigned long auxv_rseq_feature_size;
	unsigned long auxv_rseq_align;

	/* The size used for registration, defaults to RSEQ_ABI_ORIG_ALLOC_SIZE(32). */
	uint32_t rseq_reg_size = RSEQ_ABI_ORIG_ALLOC_SIZE;

	diag("Test the rseq syscall error handling");

	auxv_rseq_feature_size = getauxval(AT_RSEQ_FEATURE_SIZE);
	auxv_rseq_align = getauxval(AT_RSEQ_ALIGN);

	diag(" The kernel has rseq extended ABI support: %s", auxv_rseq_feature_size ? "yes" : "no");
	if (auxv_rseq_feature_size) {
		diag("  rseq_feature_size: %lu", auxv_rseq_feature_size);
		diag("  rseq_align:        %lu", auxv_rseq_align);
		diag("  rseq_area size:    %lu", sizeof(local_rseq));
		diag("  rseq_area align:   %lu", __alignof__(local_rseq));

		rseq_reg_size = __max(rseq_reg_size, auxv_rseq_feature_size);
	}

	if (rseq_reg_size > RSEQ_ABI_ORIG_ALLOC_SIZE) {
		nr_tests *= 2;
	}

	plan_tests(nr_tests);

	if (!rseq_syscall_available()) {
		skip(nr_tests, "rseq syscall unavailable");
		goto end;
	}

	test_errors(rseq_reg_size);
	test_registration_with_rseq_cs(rseq_reg_size);

	/*
	 * If the current kernel feature size exceeds 32 bytes, run the tests
	 * again with a registration of 32 bytes to validate backwards compat.
	 */
	if (rseq_reg_size > RSEQ_ABI_ORIG_ALLOC_SIZE) {
		test_errors(RSEQ_ABI_ORIG_ALLOC_SIZE);
		test_registration_with_rseq_cs(RSEQ_ABI_ORIG_ALLOC_SIZE);
	}
end:
	exit(exit_status());
}
