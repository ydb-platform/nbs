/* SPDX-License-Identifier: MIT */
/* SPDX-FileCopyrightText: 2026 Michael Jeanson <mjeanson@efficios.com> */

#include <rseq/rseq.h>
#include <stdio.h>
#include <sys/auxv.h>

int main(void)
{
	printf("Librseq:\n");
	printf(" Initialization success: %s\n\n", rseq_init() == RSEQ_INIT_OK ? "yes" : "no");

	printf("Kernel:\n");
	printf(" RSEQ syscall available: %s\n", rseq_available(RSEQ_AVAILABLE_QUERY_KERNEL) ? "yes" : "no");
	printf(" AT_RSEQ_FEATURE_SIZE:   %lu\n", getauxval(AT_RSEQ_FEATURE_SIZE));
	printf(" AT_RSEQ_ALIGN:          %lu\n\n", getauxval(AT_RSEQ_ALIGN));

	printf("LIBC:\n");
	printf(" RSEQ support available: %s\n", rseq_available(RSEQ_AVAILABLE_QUERY_LIBC) ? "yes" : "no");
	printf(" rseq_size:              %u\n", rseq_size);
	printf(" rseq_offset:            %ld\n", rseq_offset);
	printf(" rseq_flags:             %u\n\n", rseq_flags);

	printf("Extended features:\n");
	printf(" node_id:                %s\n", rseq_node_id_available() ? "yes" : "no");
	printf(" mm_cid:                 %s\n", rseq_mm_cid_available() ? "yes" : "no");
	printf(" slice_ctrl:             %s\n", rseq_slice_ctrl_available() ? "yes" : "no");
}
