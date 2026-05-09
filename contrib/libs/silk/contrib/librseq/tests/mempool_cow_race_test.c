// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 Mathieu Desnoyers <mathieu.desnoyers@efficios.com>

/*
 * rseq memory pool COW race test.
 *
 * Test that the entire malloc init value is visible in CPU mappings. If
 * the COW page copy race vs init happens while init is in the middle of
 * storing to the newly allocated area, iteration on all CPUs comparing
 * the visible content to the init value is responsible for detecting
 * and mitigating uninitialized or partially initialized init value from
 * the point of view of a CPU. Validate that this scheme has the
 * intended effect wrt a concurrent COW caused by storing to a nearby
 * per-cpu area on the same page.
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
#include <inttypes.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>

#include <rseq/rseq.h>
#include <rseq/mempool.h>
#include "../src/rseq-utils.h"

#include "tap.h"

#define TEST_DURATION_S	10	/* seconds */
#define TEST_ARRAY_LEN	256

enum phase {
	PHASE_RESET_POOL,
	PHASE_WRITE_POOL,
};

struct test_data {
	char c[TEST_ARRAY_LEN];
};

struct test_thread_args {
	struct rseq_mempool *mempool;
	int phase;	/* enum phase */
	int stop_init_thread;
	int stop_writer_thread;
	struct test_data *ptr1;
	struct test_data *ptr2;
};

struct test_data init_value;

static void *test_init_thread(void *arg)
{
	struct test_thread_args *thread_args = (struct test_thread_args *) arg;

	while (!RSEQ_READ_ONCE(thread_args->stop_init_thread)) {
		struct rseq_mempool_attr *attr;
		struct rseq_mempool *mempool;
		struct test_data *p;
		int ret, i;

		attr = rseq_mempool_attr_create();
		ret = rseq_mempool_attr_set_robust(attr);
		if (ret)
			abort();
		ret = rseq_mempool_attr_set_percpu(attr, 0, 1);
		if (ret)
			abort();
		ret = rseq_mempool_attr_set_max_nr_ranges(attr, 1);
		if (ret)
			abort();
		ret = rseq_mempool_attr_set_populate_policy(attr, RSEQ_MEMPOOL_POPULATE_COW_INIT);
		if (ret)
			abort();
		mempool = rseq_mempool_create("test_data", sizeof(struct test_data), attr);
		if (!mempool)
			abort();
		thread_args->mempool = mempool;
		rseq_mempool_attr_destroy(attr);

		thread_args->ptr1 = (struct test_data __rseq_percpu *) rseq_mempool_percpu_malloc(mempool);
		if (!thread_args->ptr1)
			abort();

		rseq_smp_store_release(&thread_args->phase, PHASE_WRITE_POOL);

		/* malloc init runs concurrently with COW. */
		thread_args->ptr2 = (struct test_data __rseq_percpu *)
			rseq_mempool_percpu_malloc_init(mempool,
				&init_value, sizeof(struct test_data));
		if (!thread_args->ptr2)
			abort();

		p = rseq_percpu_ptr(thread_args->ptr2, 0);
		for (i = 0; i < TEST_ARRAY_LEN; i++) {
			if (p->c[i] != 0x22) {
				fprintf(stderr, "Unexpected value\n");
				abort();
			}
		}

		while (rseq_smp_load_acquire(&thread_args->phase) != PHASE_RESET_POOL) { }

		rseq_mempool_percpu_free(thread_args->ptr2);
		rseq_mempool_percpu_free(thread_args->ptr1);

		if (rseq_mempool_destroy(mempool))
			abort();
	}
	RSEQ_WRITE_ONCE(thread_args->stop_writer_thread, 1);
	rseq_smp_store_release(&thread_args->phase, PHASE_WRITE_POOL);
	return NULL;
}

static void *test_writer_thread(void *arg)
{
	struct test_thread_args *thread_args = (struct test_thread_args *) arg;

	for (;;) {
		unsigned int loop, delay;

		delay = rand() % 10000;
		while (rseq_smp_load_acquire(&thread_args->phase) != PHASE_WRITE_POOL) { }

		if (RSEQ_READ_ONCE(thread_args->stop_writer_thread))
			break;

		for (loop = 0; loop < delay; loop++)
			rseq_barrier();

		/* Trigger COW. */
		rseq_percpu_ptr(thread_args->ptr1, 0)->c[0] = 0x33;

		rseq_smp_store_release(&thread_args->phase, PHASE_RESET_POOL);
	}

	return NULL;
}

int main(void)
{
	struct test_thread_args thread_args = {};
	pthread_t writer_thread, init_thread;
	unsigned int remain;
	int ret;

	plan_no_plan();

	diag("Beginning COW vs malloc init race validation (%u seconds)...", TEST_DURATION_S);
	srand(0x42);

	memset(&init_value.c, 0x22, TEST_ARRAY_LEN);

	thread_args.phase = PHASE_RESET_POOL;

	ret = pthread_create(&init_thread, NULL, test_init_thread, &thread_args);
	if (ret) {
		errno = ret;
		perror("pthread_create");
		abort();
	}

	ret = pthread_create(&writer_thread, NULL, test_writer_thread, &thread_args);
	if (ret) {
		errno = ret;
		perror("pthread_create");
		abort();
	}

	remain = TEST_DURATION_S;
	do {
		remain = sleep(remain);
	} while (remain > 0);

	RSEQ_WRITE_ONCE(thread_args.stop_init_thread, 1);

	ret = pthread_join(writer_thread, NULL);
	if (ret) {
		errno = ret;
		perror("pthread_join");
		abort();
	}

	ret = pthread_join(init_thread, NULL);
	if (ret) {
		errno = ret;
		perror("pthread_join");
		abort();
	}

	ok(1, "Validate COW vs malloc init race");

	exit(exit_status());
}
