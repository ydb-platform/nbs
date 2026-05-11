// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 Michael Jeanson <mjeanson@efficios.com>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdint.h>

#include <rseq/rseq.h>

#include "tap.h"

#define NR_TESTS 5

/*
 * Ensure the main executable has at least one TLS variable which will be
 * allocated before the rseq area, making sure the rseq_offset is not 0.  This
 * allows testing that the rseq_offset variable is properly initialized by
 * checking it is not 0.
 *
 * Most toolchains will add at least one main exec TLS variable but it's
 * currently not the case on RISC-V.
 */
__thread int dummy_tls = -1;

/*
 * Check the state of the public symbols when the rseq syscall is unavailable.
 *
 * This test must be used with an LD_PRELOAD library to deny access to the
 * syscall, or on a kernel that doesn't implement the syscall.
 */

int main(void)
{
	struct rseq_abi *rseq_abi;

	diag("Test the library init when libc rseq support is present but the syscall is unavailable");
	plan_tests(NR_TESTS);

	/*
	 * Skip all tests if the libc doesn't have rseq support
	 */
	if (!rseq_available(RSEQ_AVAILABLE_QUERY_LIBC)) {
		skip(NR_TESTS, "The libc doesn't have rseq support");
		goto end;
	}

	if (rseq_init() == RSEQ_INIT_OK) {
		pass("Initialize librseq")
	} else {
		fail("Initialize librseq")
		skip(NR_TESTS - 1, "Error: librseq initialization failed");
		goto end;
	}

	/* The rseq syscall is disabled, no registration is possible. */

	ok(rseq_flags == 0, "rseq_flags prior to registration is 0 (%d)", rseq_flags);
	ok(rseq_size == 0, "rseq_size prior to registration is 0 (%d)", rseq_size);
	ok(rseq_offset != 0, "rseq_offset prior to registration is not 0 (%td)", rseq_offset);

	rseq_abi = rseq_get_abi();
	ok((int32_t) rseq_abi->cpu_id == RSEQ_ABI_CPU_ID_REGISTRATION_FAILED,
			"rseq->cpu_id is set to RSEQ_ABI_CPU_ID_REGISTRATION_FAILED (%d)",
			(int32_t) rseq_abi->cpu_id);

end:
	exit(exit_status());
}
