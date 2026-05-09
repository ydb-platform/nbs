// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 Michael Jeanson <mjeanson@efficios.com>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdint.h>

#include <rseq/rseq.h>

#include "tap.h"

#define NR_TESTS 8

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
 * Check the state of the public symbols when the rseq syscall is available but
 * no thread has registered.
 */

int main(void)
{
	struct rseq_abi *rseq_abi;

	diag("Test the library init when libc rseq support is present but disabled by tunable");
	plan_tests(NR_TESTS);

	/*
	 * Skip all tests if the libc doesn't have rseq support
	 */
	if (!rseq_available(RSEQ_AVAILABLE_QUERY_LIBC)) {
		skip(NR_TESTS, "The libc doesn't have rseq support");
		goto end;
	}

	/* Check the state of the library symbols before initialization. */
	ok(rseq_flags == 0, "rseq_flags prior to library initialization is 0 (%d)", rseq_flags);
	ok(rseq_size == -1U, "rseq_size prior to library initialization is -1U (%d)", rseq_size);
	ok(rseq_offset == PTRDIFF_MIN, "rseq_offset prior to library initialization is PTRDIFF_MIN (%td)", rseq_offset);

	/* Initialize librseq */
	if (rseq_init() == RSEQ_INIT_OK) {
		pass("Initialize librseq")
	} else {
		fail("Initialize librseq")
		skip(NR_TESTS - 1, "Error: librseq initialization failed");
		goto end;
	}

	/* Check the state of the library symbols after initialization. */
	ok(rseq_flags == 0, "rseq_flags after library initialization is 0 (%d)", rseq_flags);
	ok(rseq_size == 0, "rseq_size after library initialization is 0 (%d)", rseq_size);
	ok(rseq_offset != 0, "rseq_offset after library initialization is not 0 (%td)", rseq_offset);

	/* Check the state of the rseq area. */
	rseq_abi = rseq_get_abi();
	ok((int32_t) rseq_abi->cpu_id == RSEQ_ABI_CPU_ID_REGISTRATION_FAILED,
			"rseq->cpu_id is set to RSEQ_ABI_CPU_ID_REGISTRATION_FAILED (%d)",
			(int32_t) rseq_abi->cpu_id);

end:
	exit(exit_status());
}
