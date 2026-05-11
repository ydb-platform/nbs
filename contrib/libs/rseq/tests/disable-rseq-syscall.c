// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 Michael Jeanson <mjeanson@efficios.com>

#include <errno.h>
#include <seccomp.h>

/*
 * Library constructor.
 *
 * Apply a seccomp policy that blocks access to the rseq syscall and returns
 * ENOSYS.
 */
static __attribute__((constructor))
void disable_rseq_syscall(void)
{
	scmp_filter_ctx ctx;

	ctx = seccomp_init(SCMP_ACT_ALLOW);
	seccomp_rule_add(ctx, SCMP_ACT_ERRNO(ENOSYS), SCMP_SYS(rseq), 0);
	seccomp_load(ctx);
}
