// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2018-2022 Mathieu Desnoyers <mathieu.desnoyers@efficios.com>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include <rseq/rseq.h>

#include "tap.h"

#define NR_TESTS 3

#define ARRAY_SIZE(arr)	(sizeof(arr) / sizeof((arr)[0]))

#ifdef BUILDOPT_RSEQ_PERCPU_MM_CID
# define RSEQ_PERCPU	RSEQ_PERCPU_MM_CID
static
int get_current_cpu_id(void)
{
	return rseq_current_mm_cid();
}
static
bool rseq_validate_cpu_id(void)
{
	return rseq_mm_cid_available();
}
#else
# define RSEQ_PERCPU	RSEQ_PERCPU_CPU_ID
static
int get_current_cpu_id(void)
{
	return rseq_cpu_start();
}
static
bool rseq_validate_cpu_id(void)
{
	return rseq_current_cpu_raw() >= 0;
}
#endif

struct percpu_lock_entry {
	intptr_t v;
} __attribute__((aligned(128)));

struct percpu_lock {
	struct percpu_lock_entry c[CPU_SETSIZE];
};

struct test_data_entry {
	intptr_t count;
} __attribute__((aligned(128)));

struct spinlock_test_data {
	struct percpu_lock lock;
	struct test_data_entry c[CPU_SETSIZE];
	int reps;
};

struct percpu_list_node {
	intptr_t data;
	struct percpu_list_node *next;
};

struct percpu_list_entry {
	struct percpu_list_node *head;
} __attribute__((aligned(128)));

struct percpu_list {
	struct percpu_list_entry c[CPU_SETSIZE];
};

/* A simple percpu spinlock.  Returns the cpu lock was acquired on. */
static int rseq_this_cpu_lock(struct percpu_lock *lock)
{
	int cpu;

	for (;;) {
		int ret;

		cpu = get_current_cpu_id();
		ret = rseq_load_cbne_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
					 &lock->c[cpu].v, 0, 1, cpu);
		if (rseq_likely(!ret))
			break;
		/* Retry if comparison fails or rseq aborts. */
	}
	/*
	 * Acquire semantic when taking lock after control dependency.
	 * Matches rseq_smp_store_release().
	 */
	rseq_smp_acquire__after_ctrl_dep();
	return cpu;
}

static void rseq_percpu_unlock(struct percpu_lock *lock, int cpu)
{
	assert(lock->c[cpu].v == 1);
	/*
	 * Release lock, with release semantic. Matches
	 * rseq_smp_acquire__after_ctrl_dep().
	 */
	rseq_smp_store_release(&lock->c[cpu].v, 0);
}

static void *test_percpu_spinlock_thread(void *arg)
{
	struct spinlock_test_data *data = (struct spinlock_test_data *) arg;
	int i, cpu;

	for (i = 0; i < data->reps; i++) {
		cpu = rseq_this_cpu_lock(&data->lock);
		data->c[cpu].count++;
		rseq_percpu_unlock(&data->lock, cpu);
	}

	return NULL;
}

/*
 * A simple test which implements a sharded counter using a per-cpu
 * lock.  Obviously real applications might prefer to simply use a
 * per-cpu increment; however, this is reasonable for a test and the
 * lock can be extended to synchronize more complicated operations.
 */
static void test_percpu_spinlock(void)
{
	const int num_threads = 200;
	int i;
	uint64_t sum, expected_sum;
	pthread_t test_threads[num_threads];
	struct spinlock_test_data data;

	diag("spinlock");

	memset(&data, 0, sizeof(data));
	data.reps = 5000;

	for (i = 0; i < num_threads; i++)
		pthread_create(&test_threads[i], NULL,
			       test_percpu_spinlock_thread, &data);

	for (i = 0; i < num_threads; i++)
		pthread_join(test_threads[i], NULL);

	sum = 0;
	for (i = 0; i < CPU_SETSIZE; i++)
		sum += data.c[i].count;

	expected_sum = (uint64_t)data.reps * num_threads;

	ok(sum == expected_sum, "spinlock - sum (%" PRIu64 " == %" PRIu64 ")", sum, expected_sum);
}

static void this_cpu_list_push(struct percpu_list *list,
			struct percpu_list_node *node,
			int *_cpu)
{
	int cpu;

	for (;;) {
		intptr_t *targetptr, newval, expect;
		int ret;

		cpu = get_current_cpu_id();
		/* Load list->c[cpu].head with single-copy atomicity. */
		expect = (intptr_t)RSEQ_READ_ONCE(list->c[cpu].head);
		newval = (intptr_t)node;
		targetptr = (intptr_t *)&list->c[cpu].head;
		node->next = (struct percpu_list_node *)expect;
		ret = rseq_load_cbne_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
					 targetptr, expect, newval, cpu);
		if (rseq_likely(!ret))
			break;
		/* Retry if comparison fails or rseq aborts. */
	}
	if (_cpu)
		*_cpu = cpu;
}

/*
 * Unlike a traditional lock-less linked list; the availability of a
 * rseq primitive allows us to implement pop without concerns over
 * ABA-type races.
 */
static struct percpu_list_node *this_cpu_list_pop(struct percpu_list *list,
					   int *_cpu)
{
	for (;;) {
		struct percpu_list_node *head;
		intptr_t *targetptr, expectnot, *load;
		long offset;
		int ret, cpu;

		cpu = get_current_cpu_id();
		targetptr = (intptr_t *)&list->c[cpu].head;
		expectnot = (intptr_t)NULL;
		offset = offsetof(struct percpu_list_node, next);
		load = (intptr_t *)&head;
		ret = rseq_load_cbeq_store_add_load_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
						 targetptr, expectnot,
						 offset, load, cpu);
		if (rseq_likely(!ret)) {
			if (_cpu)
				*_cpu = cpu;
			return head;
		}
		if (ret > 0)
			return NULL;
		/* Retry if rseq aborts. */
	}
}

/*
 * __percpu_list_pop is not safe against concurrent accesses. Should
 * only be used on lists that are not concurrently modified.
 */
static struct percpu_list_node *__percpu_list_pop(struct percpu_list *list, int cpu)
{
	struct percpu_list_node *node;

	node = list->c[cpu].head;
	if (!node)
		return NULL;
	list->c[cpu].head = node->next;
	return node;
}

static void *test_percpu_list_thread(void *arg)
{
	int i;
	struct percpu_list *list = (struct percpu_list *)arg;

	for (i = 0; i < 100000; i++) {
		struct percpu_list_node *node;

		node = this_cpu_list_pop(list, NULL);
		sched_yield();  /* encourage shuffling */
		if (node)
			this_cpu_list_push(list, node, NULL);
	}

	return NULL;
}

/* Simultaneous modification to a per-cpu linked list from many threads.  */
static void test_percpu_list(void)
{
	int i, j;
	uint64_t sum = 0, expected_sum = 0;
	struct percpu_list list;
	pthread_t test_threads[200];

	diag("percpu_list");

	memset(&list, 0, sizeof(list));

	/* Generate list entries for every possible cpu. */
	for (i = 0; i < CPU_SETSIZE; i++) {
		for (j = 1; j <= 100; j++) {
			struct percpu_list_node *node;

			expected_sum += j;

			node = (struct percpu_list_node *) malloc(sizeof(*node));
			assert(node);
			node->data = j;
			node->next = list.c[i].head;
			list.c[i].head = node;
		}
	}

	diag(" Create threads");
	for (i = 0; i < 200; i++)
		pthread_create(&test_threads[i], NULL,
		       test_percpu_list_thread, &list);

	diag(" Join threads");
	for (i = 0; i < 200; i++)
		pthread_join(test_threads[i], NULL);

	for (i = 0; i < CPU_SETSIZE; i++) {
		struct percpu_list_node *node;

		while ((node = __percpu_list_pop(&list, i))) {
			sum += node->data;
			free(node);
		}
	}

	/*
	 * All entries should now be accounted for.
	 */
	ok(sum == expected_sum, "percpu_list - sum (%" PRIu64 " == %" PRIu64 ")", sum, expected_sum);
}

int main(void)
{
	plan_tests(NR_TESTS);

	/*
	 * Skip all tests if the libc doesn't have rseq support
	 */
	if (!rseq_available(RSEQ_AVAILABLE_QUERY_LIBC)) {
		skip(NR_TESTS, "The libc doesn't have rseq support");
	}

	if (rseq_init() == RSEQ_INIT_OK) {
		pass("Initialized librseq");
	} else {
		fail("Initialized librseq")
		skip(NR_TESTS - 1, "Error: librseq initialization failed");
		goto end;
	}

	if (!rseq_validate_cpu_id()) {
		skip(NR_TESTS - 2, "Error: cpu id getter unavailable");
		goto end;
	}

	test_percpu_spinlock();
	test_percpu_list();

end:
	exit(exit_status());
}
