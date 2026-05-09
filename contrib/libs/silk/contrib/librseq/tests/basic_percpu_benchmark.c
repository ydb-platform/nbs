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
#include <poll.h>
#include <time.h>

#include <rseq/rseq.h>
#include <rseq/mempool.h>
#include "tap.h"

/*
 * Test intermittent workloads. Invoke with the number of ms as delay
 * between individual thread execution as parameter.
 */

#define NR_CPUS 1024

static int rand_order[NR_CPUS];

/* AREA_LEN must not fill stride. */
#define AREA_LEN	(16 * 1024)	/* 16 kB */

/* Delay between each thread */
static int thread_delay = 200;

#define NR_TESTS 2
#define LOOPS_PER_THREAD 5

#define ARRAY_SIZE(arr)	(sizeof(arr) / sizeof((arr)[0]))

static int nr_threads;

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

static struct rseq_mempool *mempool;

static char __rseq_percpu *percpudata;

static int nr_active_threads, test_go, test_stop;

static unsigned int cpu_affinities[NR_CPUS];
static unsigned int next_aff = 0;

pthread_mutex_t affinity_mutex = PTHREAD_MUTEX_INITIALIZER;

static void set_affinity(void)
{
	cpu_set_t mask;
	int cpu, ret;

	ret = pthread_mutex_lock(&affinity_mutex);
	if (ret) {
		perror("Error in pthread mutex lock");
		exit(-1);
	}
	cpu = cpu_affinities[next_aff++];
	ret = pthread_mutex_unlock(&affinity_mutex);
	if (ret) {
		perror("Error in pthread mutex unlock");
		exit(-1);
	}

	CPU_ZERO(&mask);
	CPU_SET(cpu, &mask);
	sched_setaffinity(0, sizeof(mask), &mask);
}

static void init_affinity(void)
{
	cpu_set_t allowed_cpus;
	int cpu;

	if (sched_getaffinity(0, sizeof(allowed_cpus), &allowed_cpus)) {
		perror("sched_getaffinity");
		abort();
	}
	for (cpu = 0; cpu < NR_CPUS; cpu++) {
		if (CPU_ISSET(cpu, &allowed_cpus))
			cpu_affinities[next_aff++] = cpu;
	}
	next_aff = 0;
}

static int get_affinity_weight(void)
{
	cpu_set_t allowed_cpus;

	if (sched_getaffinity(0, sizeof(allowed_cpus), &allowed_cpus)) {
		perror("sched_getaffinity");
		abort();
	}
	return CPU_COUNT(&allowed_cpus);
}

struct test_data {
	int64_t total_time;
	int thread_id;
};

static int64_t difftimespec_ns(const struct timespec after, const struct timespec before)
{
	return ((int64_t)after.tv_sec - (int64_t)before.tv_sec) * 1000000000LL
		+ ((int64_t)after.tv_nsec - (int64_t)before.tv_nsec);
}

static void *test_percpu_benchmark_thread(void *arg)
{
	struct test_data *data = (struct test_data *) arg;
	struct timespec t1, t2;
	int64_t total_time = 0;
	int i, cpu;
	int thread_index = rand_order[data->thread_id];

	set_affinity();

	/*
	 * Rendez-vous across all threads to make sure the number of
	 * threads >= number of possible CPUs for the entire test duration.
	 */
	if (__atomic_add_fetch(&nr_active_threads, 1, __ATOMIC_RELAXED) == nr_threads)
		__atomic_store_n(&test_go, 1, __ATOMIC_RELAXED);
	while (!__atomic_load_n(&test_go, __ATOMIC_RELAXED))
		rseq_barrier();

	printf("Thread %d running on cpu: %d delay: %dms\n",
		thread_index, rseq_current_cpu_raw(), thread_delay * thread_index);
	poll(NULL, 0, thread_delay * thread_index);

	for (i = 0; i < 20000; i++) {
		/* Access pages once to improve initial cache locality. */
		char *pdata;
		int j;

		cpu = get_current_cpu_id();
		pdata = rseq_percpu_ptr(percpudata, cpu);
		for (j = 0; j < AREA_LEN; j++)
			pdata[j]++;
	}
	for (i = 0; i < LOOPS_PER_THREAD; i++) {
		char *pdata;
		int j;

		cpu = get_current_cpu_id();
		pdata = rseq_percpu_ptr(percpudata, cpu);
		clock_gettime(CLOCK_MONOTONIC, &t1);
		for (j = 0; j < AREA_LEN; j++)
			pdata[j]++;
		clock_gettime(CLOCK_MONOTONIC, &t2);
		total_time += difftimespec_ns(t2, t1);
		poll(NULL, 0, thread_delay * nr_threads);
	}

	/*
	 * Rendez-vous before exiting all threads to make sure the
	 * number of threads >= number of possible CPUs for the entire
	 * test duration.
	 */
	if (__atomic_sub_fetch(&nr_active_threads, 1, __ATOMIC_RELAXED) == 0)
		__atomic_store_n(&test_stop, 1, __ATOMIC_RELAXED);
	while (!__atomic_load_n(&test_stop, __ATOMIC_RELAXED))
		rseq_barrier();

	data->total_time += total_time;

	return NULL;
}

static void test_percpu_benchmark(void)
{
	int i;
	pthread_t test_threads[nr_threads];
	struct test_data test_data[nr_threads];
	int64_t total_time = 0;

	diag("benchmark");

	memset(test_data, 0, sizeof(struct test_data) * nr_threads);

	for (i = 0; i < nr_threads; i++) {
		test_data[i].thread_id = i;
		pthread_create(&test_threads[i], NULL,
			       test_percpu_benchmark_thread, &test_data[i]);
	}

	for (i = 0; i < nr_threads; i++)
		pthread_join(test_threads[i], NULL);
	for (i = 0; i < nr_threads; i++)
		total_time += test_data[i].total_time;
	diag("Thread delay: %dms, total time: %" PRId64 "ns over %d threads, %d loops per thread -- %" PRId64 "ns per loop",
		thread_delay, total_time, nr_threads, LOOPS_PER_THREAD,
		total_time / (int64_t)(nr_threads * LOOPS_PER_THREAD));
}

int main(int argc, char **argv)
{
	struct rseq_mempool_attr *attr = rseq_mempool_attr_create();
	int i;

	if (argc < 2) {
		printf("Missing thread delay first argument\n");
		abort();
	}
	thread_delay = atoi(argv[1]);

	printf("Thread delay: %dms\n", thread_delay);

	plan_tests(NR_TESTS);

	mempool = rseq_mempool_create(NULL, AREA_LEN, NULL);
	if (!mempool)
		abort();
	rseq_mempool_attr_destroy(attr);
	percpudata = (char __rseq_percpu *) rseq_mempool_percpu_zmalloc(mempool);

	init_affinity();
	nr_threads = get_affinity_weight();

	srand(time(NULL));

	for (i = 0; i < nr_threads; i++)
		rand_order[i] = i;
	for (i = 0; i < nr_threads; i++) {
		int index = rand() % nr_threads;
		int tmp = rand_order[i];
		rand_order[i] = rand_order[index];
		rand_order[index] = tmp;
	}

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

	test_percpu_benchmark();

end:
	rseq_mempool_percpu_free(percpudata);
	rseq_mempool_destroy(mempool);
	exit(exit_status());
}
