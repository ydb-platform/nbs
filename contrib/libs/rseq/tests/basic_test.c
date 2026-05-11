// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2020 Mathieu Desnoyers <mathieu.desnoyers@efficios.com>
/*
 * Basic test coverage for critical regions and rseq_current_cpu().
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <assert.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include <rseq/rseq.h>

#include "tap.h"

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

static void test_registered(void)
{
	struct rseq_abi *rseq_abi = rseq_get_abi();

	ok(rseq_flags == 0, "rseq_flags after registration is 0 (%d)", rseq_flags);
	ok(rseq_size >= 20, "rseq_size after registration is 20 or greater (%d)", rseq_size);
	ok(rseq_offset != 0, "rseq_offset after registration is not 0 (%td)", rseq_offset);

	ok((int32_t) rseq_abi->cpu_id >= 0,
			"rseq->cpu_id after registration is 0 or greater (%d)",
			(int32_t) rseq_abi->cpu_id);
}

static void test_cpu_pointer(void)
{
	cpu_set_t affinity, test_affinity;
	int ret, i;

	ret = sched_getaffinity(0, sizeof(affinity), &affinity);
	ok(ret == 0, "Get current thread affinity mask");

	CPU_ZERO(&test_affinity);
	for (i = 0; i < CPU_SETSIZE; i++) {
		if (CPU_ISSET(i, &affinity)) {
			int node;

			CPU_SET(i, &test_affinity);

			ret = sched_setaffinity(0, sizeof(test_affinity),
					&test_affinity);
			ok(ret == 0, "Set affinity mask to CPU %d exclusively", i);

			ok(sched_getcpu() == i, "sched_getcpu returns CPU %d", i);
			ok(rseq_current_cpu() == (unsigned int) i, "rseq_current_cpu returns CPU %d", i);
			ok(rseq_current_cpu_raw() == i, "rseq_current_cpu_raw returns CPU %d", i);
			ok(rseq_cpu_start() == (unsigned int) i, "rseq_cpu_start returns CPU %d", i);
			node = rseq_fallback_current_node();
			ok(rseq_fallback_current_node() == node, "rseq_fallback_current_node returns node %d", node);
			CPU_CLR(i, &test_affinity);
		}
	}

	ret = sched_setaffinity(0, sizeof(affinity), &affinity);
	ok(ret == 0, "Restore current thread initial affinity mask");
}

int main(void)
{
	diag("Basic test coverage for critical regions and rseq_current_cpu()");

	/*
	 * Skip all tests if the libc doesn't have rseq support
	 */
	if (rseq_available(RSEQ_AVAILABLE_QUERY_LIBC)) {
		plan_no_plan();
	} else {
		plan_skip_all("The libc doesn't have rseq support");
	}

	if (rseq_init() == RSEQ_INIT_OK) {
		pass("Initialized librseq");
	} else {
		fail("Initialized librseq")
		goto end;
	}

	test_registered();
	test_cpu_pointer();

end:
	exit(exit_status());
}
