// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 Mathieu Desnoyers <mathieu.desnoyers@efficios.com>
/*
 * rseq memory pool test.
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

#include <rseq/mempool.h>
#include "../src/rseq-utils.h"

#include "../src/list.h"
#include "tap.h"

#if RSEQ_BITS_PER_LONG == 64
# define POISON_VALUE	0xABCDABCDABCDABCDULL
#else
# define POISON_VALUE	0xABCDABCDUL
#endif

struct test_data {
	uintptr_t value[2];
	struct test_data __rseq_percpu *backref;
	struct list_head node;
};

static void test_mempool_fill(enum rseq_mempool_populate_policy policy,
		unsigned long max_nr_ranges, size_t stride)
{
	struct test_data __rseq_percpu *ptr;
	struct test_data *iter, *tmp;
	struct rseq_mempool *mempool;
	struct rseq_mempool_attr *attr;
	uint64_t count = 0;
	LIST_HEAD(list);
	int ret, i, size_order;
	struct test_data init_value = {
		.value = {
			123,
			456,
		},
		.backref = NULL,
		.node = {},
	};

	attr = rseq_mempool_attr_create();
	ok(attr, "Create pool attribute");
	ret = rseq_mempool_attr_set_robust(attr);
	ok(ret == 0, "Setting mempool robust attribute");
	ret = rseq_mempool_attr_set_percpu(attr, stride, CPU_SETSIZE);
	ok(ret == 0, "Setting mempool percpu type");
	ret = rseq_mempool_attr_set_max_nr_ranges(attr, max_nr_ranges);
	ok(ret == 0, "Setting mempool max_nr_ranges=%lu", max_nr_ranges);
	ret = rseq_mempool_attr_set_poison(attr, POISON_VALUE);
	ok(ret == 0, "Setting mempool poison");
	ret = rseq_mempool_attr_set_populate_policy(attr, policy);
	ok(ret == 0, "Setting mempool populate policy to %s",
		policy == RSEQ_MEMPOOL_POPULATE_COW_INIT ? "COW_INIT" : "COW_ZERO");
	mempool = rseq_mempool_create("test_data",
			sizeof(struct test_data), attr);
	ok(mempool, "Create mempool of size %zu", stride);
	rseq_mempool_attr_destroy(attr);

	for (;;) {
		struct test_data *cpuptr;

		ptr = (struct test_data __rseq_percpu *) rseq_mempool_percpu_zmalloc(mempool);
		if (!ptr)
			break;
		/* Link items in cpu 0. */
		cpuptr = rseq_percpu_ptr(ptr, 0, stride);
		cpuptr->backref = ptr;
		/* Randomize items in list. */
		if (count & 1)
			list_add(&cpuptr->node, &list);
		else
			list_add_tail(&cpuptr->node, &list);
		count++;
	}

	size_order = rseq_get_count_order_ulong(sizeof(struct test_data));
	ok(count * (1U << size_order) == stride * max_nr_ranges,
		"Allocated %" PRIu64 " objects in pool", count);

	list_for_each_entry(iter, &list, node) {
		ptr = iter->backref;
		for (i = 0; i < CPU_SETSIZE; i++) {
			struct test_data *cpuptr = rseq_percpu_ptr(ptr, i, stride);

			if (cpuptr->value[0] != 0)
				abort();
			cpuptr->value[0]++;
		}
	}
	ok(1, "Check for pool content corruption");

	list_for_each_entry_safe(iter, tmp, &list, node) {
		ptr = iter->backref;
		rseq_mempool_percpu_free(ptr, stride);
	}
	ok(1, "Free all objects");

	ptr = (struct test_data __rseq_percpu *) rseq_mempool_percpu_zmalloc(mempool);
	if (!ptr)
		abort();
	ok(1, "Allocate one object");

	rseq_mempool_percpu_free(ptr, stride);
	ok(1, "Free one object");

	ptr = (struct test_data __rseq_percpu *)
		rseq_mempool_percpu_malloc_init(mempool,
			&init_value, sizeof(struct test_data));
	if (!ptr)
		abort();
	ok(1, "Allocate one initialized object");

	ok(ptr->value[0] == 123 && ptr->value[1] == 456, "Validate initial values");

	rseq_mempool_percpu_free(ptr, stride);
	ok(1, "Free one object");

	ret = rseq_mempool_destroy(mempool);
	ok(ret == 0, "Destroy mempool");
}

static void test_robust_double_free(struct rseq_mempool *pool,
		enum rseq_mempool_populate_policy policy __attribute__((unused)))
{
	struct test_data __rseq_percpu *ptr;

	ptr = (struct test_data __rseq_percpu *) rseq_mempool_percpu_malloc(pool);

	rseq_mempool_percpu_free(ptr);
	rseq_mempool_percpu_free(ptr);
}

static void test_robust_corrupt_after_free(struct rseq_mempool *pool,
		enum rseq_mempool_populate_policy policy)
{
	struct test_data __rseq_percpu *ptr;
	struct test_data *cpuptr;

	ptr = (struct test_data __rseq_percpu *) rseq_mempool_percpu_malloc(pool);
	/*
	 * Corrupt free list: For robust pools, the free list is located
	 * after the last cpu memory range for COW_ZERO, and after the init
	 * values memory range for COW_INIT.
	 */
	if (policy == RSEQ_MEMPOOL_POPULATE_COW_ZERO)
		cpuptr = (struct test_data *) rseq_percpu_ptr(ptr, rseq_mempool_get_max_nr_cpus(pool));
	else
		cpuptr = (struct test_data *) rseq_percpu_ptr(ptr, rseq_mempool_get_max_nr_cpus(pool) + 1);

	rseq_mempool_percpu_free(ptr);
	cpuptr->value[0] = (uintptr_t) test_robust_corrupt_after_free;

	rseq_mempool_destroy(pool);
}

static void test_robust_memory_leak(struct rseq_mempool *pool,
		enum rseq_mempool_populate_policy policy __attribute__((unused)))
{
	(void) rseq_mempool_percpu_malloc(pool);

	rseq_mempool_destroy(pool);
}

static void test_robust_free_list_corruption(struct rseq_mempool *pool,
		enum rseq_mempool_populate_policy policy)
{
	struct test_data __rseq_percpu *ptr;
	struct test_data *cpuptr;

	ptr = (struct test_data __rseq_percpu *) rseq_mempool_percpu_malloc(pool);
	/*
	 * Corrupt free list: For robust pools, the free list is located
	 * after the last cpu memory range for COW_ZERO, and after the init
	 * values memory range for COW_INIT.
	 */
	if (policy == RSEQ_MEMPOOL_POPULATE_COW_ZERO)
		cpuptr = (struct test_data *) rseq_percpu_ptr(ptr, rseq_mempool_get_max_nr_cpus(pool));
	else
		cpuptr = (struct test_data *) rseq_percpu_ptr(ptr, rseq_mempool_get_max_nr_cpus(pool) + 1);

	rseq_mempool_percpu_free(ptr);

	cpuptr->value[0] = (uintptr_t) cpuptr;

	(void) rseq_mempool_percpu_malloc(pool);
	(void) rseq_mempool_percpu_malloc(pool);
}

static void test_robust_poison_corruption_malloc(struct rseq_mempool *pool,
		enum rseq_mempool_populate_policy policy __attribute__((unused)))
{
	struct test_data __rseq_percpu *ptr;
	struct test_data *cpuptr;

	ptr = (struct test_data __rseq_percpu *) rseq_mempool_percpu_malloc(pool);
	cpuptr = (struct test_data *) rseq_percpu_ptr(ptr, 0);

	rseq_mempool_percpu_free(ptr);

	cpuptr->value[0] = 1;

	(void) rseq_mempool_percpu_malloc(pool);
}

static void test_robust_poison_corruption_destroy(struct rseq_mempool *pool,
		enum rseq_mempool_populate_policy policy __attribute__((unused)))
{
	struct test_data __rseq_percpu *ptr;
	struct test_data *cpuptr;

	ptr = (struct test_data __rseq_percpu *) rseq_mempool_percpu_malloc(pool);
	cpuptr = (struct test_data *) rseq_percpu_ptr(ptr, 0);

	rseq_mempool_percpu_free(ptr);

	cpuptr->value[0] = 1;

	rseq_mempool_destroy(pool);
}

static struct rseq_mempool *make_test_pool(enum rseq_mempool_populate_policy policy)
{
	struct rseq_mempool_attr *attr;
	struct rseq_mempool *pool;
	int ret;

	pool = NULL;

	attr = rseq_mempool_attr_create();

	if (!attr) {
		goto out;
	}

	ret = rseq_mempool_attr_set_robust(attr);

	if (0 != ret) {
		goto err_attr;
	}

	ret = rseq_mempool_attr_set_percpu(attr, RSEQ_MEMPOOL_STRIDE, 1);

	if (0 != ret) {
		goto err_attr;
	}

	ret = rseq_mempool_attr_set_populate_policy(attr, policy);

	if (0 != ret) {
		goto err_attr;
	}

	pool = rseq_mempool_create("mempool-robust",
				sizeof(struct test_data), attr);
err_attr:
	rseq_mempool_attr_destroy(attr);
out:
	return pool;

}

static int run_robust_test(void (*test)(struct rseq_mempool *, enum rseq_mempool_populate_policy),
			enum rseq_mempool_populate_policy policy)
{
	pid_t cpid;
	int status;
	struct rseq_mempool *pool;

	cpid = fork();

	switch (cpid) {
	case -1:
		return 0;
	case 0:
		/*
		 * Intentional leak of test pool because some tests might want
		 * to do an explicit destroy on it.
		 */
		pool = make_test_pool(policy);
		if (!pool)
			_exit(EXIT_FAILURE);
		test(pool, policy);
		_exit(EXIT_FAILURE);
	default:
		waitpid(cpid, &status, 0);
	}

	if (WIFSIGNALED(status) &&
	    (SIGABRT == WTERMSIG(status)))
		return 1;

	return 0;
}

static void run_robust_tests(enum rseq_mempool_populate_policy policy)
{

	ok(run_robust_test(test_robust_double_free, policy),
		"robust-double-free");

	ok(run_robust_test(test_robust_memory_leak, policy),
		"robust-memory-leak");

	ok(run_robust_test(test_robust_poison_corruption_malloc, policy),
		"robust-poison-corruption-malloc");

	ok(run_robust_test(test_robust_poison_corruption_destroy, policy),
		"robust-poison-corruption-destroy");

	ok(run_robust_test(test_robust_corrupt_after_free, policy),
		"robust-corrupt-after-free");

	ok(run_robust_test(test_robust_free_list_corruption, policy),
		"robust-free-list-corruption");
}

static void fork_child(struct rseq_mempool *pool,
		enum rseq_mempool_populate_policy policy __attribute__((unused)))
{
	rseq_mempool_destroy(pool);
}

/*
 * Test that destroying a mempool works in child after fork.
 */
static int run_fork_destroy_pool_test(void (*test)(struct rseq_mempool *, enum rseq_mempool_populate_policy),
			enum rseq_mempool_populate_policy policy)
{
	pid_t cpid;
	int status;
	struct rseq_mempool *pool;

	pool = make_test_pool(policy);
	if (!pool)
		_exit(EXIT_FAILURE);

	cpid = fork();

	switch (cpid) {
	case -1:
		return 0;
	case 0:
		test(pool, policy);
		_exit(EXIT_SUCCESS);
	default:
		waitpid(cpid, &status, 0);
	}

	if (WIFSIGNALED(status))
		return 0;

	return 1;
}

int main(void)
{
	size_t len;
	unsigned long nr_ranges;

	plan_no_plan();

	for (nr_ranges = 1; nr_ranges < 32; nr_ranges <<= 1) {
		/* From page size to 64kB */
		for (len = rseq_get_page_len(); len < 65536; len <<= 1) {
			test_mempool_fill(RSEQ_MEMPOOL_POPULATE_COW_ZERO, nr_ranges, len);
			test_mempool_fill(RSEQ_MEMPOOL_POPULATE_COW_INIT, nr_ranges, len);
		}
	}

	len = rseq_get_page_len();
	if (len < 65536)
		len = 65536;
	/* From min(page size, 64kB) to 4MB */
	for (; len < 4096 * 1024; len <<= 1) {
		test_mempool_fill(RSEQ_MEMPOOL_POPULATE_COW_ZERO, 1, len);
		test_mempool_fill(RSEQ_MEMPOOL_POPULATE_COW_INIT, 1, len);
	}

	run_robust_tests(RSEQ_MEMPOOL_POPULATE_COW_ZERO);
	run_robust_tests(RSEQ_MEMPOOL_POPULATE_COW_INIT);
	ok(run_fork_destroy_pool_test(fork_child, RSEQ_MEMPOOL_POPULATE_COW_ZERO),
		"fork destroy pool test populate COW_ZERO");
	ok(run_fork_destroy_pool_test(fork_child, RSEQ_MEMPOOL_POPULATE_COW_INIT),
		"fork destroy pool test populate COW_INIT");

	exit(exit_status());
}
