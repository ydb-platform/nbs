// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2020-2022 Mathieu Desnoyers <mathieu.desnoyers@efficios.com>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <assert.h>
#include <linux/version.h>
#include <linux/membarrier.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syscall.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <signal.h>
#include <errno.h>
#include <stddef.h>
#include <stdbool.h>
#include <rseq/mempool.h>

#if LINUX_VERSION_CODE < KERNEL_VERSION(5,10,0)
enum {
	MEMBARRIER_CMD_PRIVATE_EXPEDITED_RSEQ			= (1 << 7),
	MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_RSEQ		= (1 << 8),
};

enum {
	MEMBARRIER_CMD_FLAG_CPU		= (1 << 0),
};
#endif

#define NR_INJECT	9
static int loop_cnt[NR_INJECT + 1];

static int loop_cnt_1 asm("asm_loop_cnt_1") __attribute__((used));
static int loop_cnt_2 asm("asm_loop_cnt_2") __attribute__((used));
static int loop_cnt_3 asm("asm_loop_cnt_3") __attribute__((used));
static int loop_cnt_4 asm("asm_loop_cnt_4") __attribute__((used));
static int loop_cnt_5 asm("asm_loop_cnt_5") __attribute__((used));
static int loop_cnt_6 asm("asm_loop_cnt_6") __attribute__((used));

static int opt_modulo, verbose;

static int opt_yield, opt_signal, opt_sleep,
		opt_disable_rseq, opt_threads = 200,
		opt_disable_mod = 0, opt_test = 's';

static long long opt_reps = 5000;

static __thread __attribute__((tls_model("initial-exec")))
unsigned int signals_delivered;

static inline pid_t rseq_gettid(void)
{
	return syscall(__NR_gettid);
}

#ifndef BENCHMARK

static __thread __attribute__((tls_model("initial-exec"), unused))
int yield_mod_cnt, nr_abort;

#define printf_verbose(fmt, ...)			\
	do {						\
		if (verbose)				\
			printf(fmt, ## __VA_ARGS__);	\
	} while (0)

#ifdef __i386__

#define INJECT_ASM_REG	"eax"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG

/*
 * Use ip-relative addressing to get the loop counter.
 */
#define __RSEQ_INJECT_ASM(n, ref_ip, ref_label) \
	"movl " __rseq_str(ref_ip) ", %%" INJECT_ASM_REG "\n\t" \
	"leal ( asm_loop_cnt_" #n " - " __rseq_str(ref_label) "b)(%%" INJECT_ASM_REG "), %%" INJECT_ASM_REG "\n\t" \
	"movl (%%" INJECT_ASM_REG "), %%" INJECT_ASM_REG "\n\t" \
	"test %%" INJECT_ASM_REG ",%%" INJECT_ASM_REG "\n\t" \
	"jz 333f\n\t" \
	"222:\n\t" \
	"dec %%" INJECT_ASM_REG "\n\t" \
	"jnz 222b\n\t" \
	"333:\n\t"

#define RSEQ_INJECT_ASM(n) \
	__RSEQ_INJECT_ASM(n, %[ref_ip], RSEQ_ASM_REF_LABEL)

#elif defined(__x86_64__)

#define INJECT_ASM_REG_P	"rax"
#define INJECT_ASM_REG		"eax"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG_P \
	, INJECT_ASM_REG

#define RSEQ_INJECT_ASM(n) \
	"lea asm_loop_cnt_" #n "(%%rip), %%" INJECT_ASM_REG_P "\n\t" \
	"mov (%%" INJECT_ASM_REG_P "), %%" INJECT_ASM_REG "\n\t" \
	"test %%" INJECT_ASM_REG ",%%" INJECT_ASM_REG "\n\t" \
	"jz 333f\n\t" \
	"222:\n\t" \
	"dec %%" INJECT_ASM_REG "\n\t" \
	"jnz 222b\n\t" \
	"333:\n\t"

#elif defined(__s390__)

#define RSEQ_INJECT_INPUT \
	, [loop_cnt_1]"m"(loop_cnt[1]) \
	, [loop_cnt_2]"m"(loop_cnt[2]) \
	, [loop_cnt_3]"m"(loop_cnt[3]) \
	, [loop_cnt_4]"m"(loop_cnt[4]) \
	, [loop_cnt_5]"m"(loop_cnt[5]) \
	, [loop_cnt_6]"m"(loop_cnt[6])

#define INJECT_ASM_REG	"r0"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG

#define RSEQ_INJECT_ASM(n) \
	"l %%" INJECT_ASM_REG ", %[loop_cnt_" #n "]\n\t" \
	"ltr %%" INJECT_ASM_REG ", %%" INJECT_ASM_REG "\n\t" \
	"je 333f\n\t" \
	"222:\n\t" \
	"ahi %%" INJECT_ASM_REG ", -1\n\t" \
	"jnz 222b\n\t" \
	"333:\n\t"

#elif defined(__ARMEL__)

#define RSEQ_INJECT_INPUT \
	, [loop_cnt_1]"m"(loop_cnt[1]) \
	, [loop_cnt_2]"m"(loop_cnt[2]) \
	, [loop_cnt_3]"m"(loop_cnt[3]) \
	, [loop_cnt_4]"m"(loop_cnt[4]) \
	, [loop_cnt_5]"m"(loop_cnt[5]) \
	, [loop_cnt_6]"m"(loop_cnt[6])

#define INJECT_ASM_REG	"r4"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG

#define RSEQ_INJECT_ASM(n) \
	"ldr " INJECT_ASM_REG ", %[loop_cnt_" #n "]\n\t" \
	"cmp " INJECT_ASM_REG ", #0\n\t" \
	"beq 333f\n\t" \
	"222:\n\t" \
	"subs " INJECT_ASM_REG ", #1\n\t" \
	"bne 222b\n\t" \
	"333:\n\t"

#elif defined(__AARCH64EL__)

#define RSEQ_INJECT_INPUT \
	, [loop_cnt_1] "Qo" (loop_cnt[1]) \
	, [loop_cnt_2] "Qo" (loop_cnt[2]) \
	, [loop_cnt_3] "Qo" (loop_cnt[3]) \
	, [loop_cnt_4] "Qo" (loop_cnt[4]) \
	, [loop_cnt_5] "Qo" (loop_cnt[5]) \
	, [loop_cnt_6] "Qo" (loop_cnt[6])

#define INJECT_ASM_REG	RSEQ_ASM_TMP_REG32

#define RSEQ_INJECT_ASM(n) \
	"	ldr	" INJECT_ASM_REG ", %[loop_cnt_" #n "]\n"	\
	"	cbz	" INJECT_ASM_REG ", 333f\n"			\
	"222:\n"							\
	"	sub	" INJECT_ASM_REG ", " INJECT_ASM_REG ", #1\n"	\
	"	cbnz	" INJECT_ASM_REG ", 222b\n"			\
	"333:\n"

#elif defined(__PPC__)

#define RSEQ_INJECT_INPUT \
	, [loop_cnt_1]"m"(loop_cnt[1]) \
	, [loop_cnt_2]"m"(loop_cnt[2]) \
	, [loop_cnt_3]"m"(loop_cnt[3]) \
	, [loop_cnt_4]"m"(loop_cnt[4]) \
	, [loop_cnt_5]"m"(loop_cnt[5]) \
	, [loop_cnt_6]"m"(loop_cnt[6])

#define INJECT_ASM_REG	"r18"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG

#define RSEQ_INJECT_ASM(n) \
	"lwz %%" INJECT_ASM_REG ", %[loop_cnt_" #n "]\n\t" \
	"cmpwi %%" INJECT_ASM_REG ", 0\n\t" \
	"beq 333f\n\t" \
	"222:\n\t" \
	"subic. %%" INJECT_ASM_REG ", %%" INJECT_ASM_REG ", 1\n\t" \
	"bne 222b\n\t" \
	"333:\n\t"

#elif defined(__mips__)

#define RSEQ_INJECT_INPUT \
	, [loop_cnt_1]"m"(loop_cnt[1]) \
	, [loop_cnt_2]"m"(loop_cnt[2]) \
	, [loop_cnt_3]"m"(loop_cnt[3]) \
	, [loop_cnt_4]"m"(loop_cnt[4]) \
	, [loop_cnt_5]"m"(loop_cnt[5]) \
	, [loop_cnt_6]"m"(loop_cnt[6])

#define INJECT_ASM_REG	"$5"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG

#define RSEQ_INJECT_ASM(n) \
	"lw " INJECT_ASM_REG ", %[loop_cnt_" #n "]\n\t" \
	"beqz " INJECT_ASM_REG ", 333f\n\t" \
	"222:\n\t" \
	"addiu " INJECT_ASM_REG ", -1\n\t" \
	"bnez " INJECT_ASM_REG ", 222b\n\t" \
	"333:\n\t"

#elif defined(__riscv)

#define RSEQ_INJECT_INPUT \
	, [loop_cnt_1]"m"(loop_cnt[1]) \
	, [loop_cnt_2]"m"(loop_cnt[2]) \
	, [loop_cnt_3]"m"(loop_cnt[3]) \
	, [loop_cnt_4]"m"(loop_cnt[4]) \
	, [loop_cnt_5]"m"(loop_cnt[5]) \
	, [loop_cnt_6]"m"(loop_cnt[6])

#define INJECT_ASM_REG  "t1"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG

#define RSEQ_INJECT_ASM(n)                                      \
	"lw " INJECT_ASM_REG ", %[loop_cnt_" #n "]\n\t"         \
	"beqz " INJECT_ASM_REG ", 333f\n\t"                     \
	"222:\n\t"                                              \
	"addi  " INJECT_ASM_REG "," INJECT_ASM_REG ", -1\n\t"   \
	"bnez " INJECT_ASM_REG ", 222b\n\t"                     \
	"333:\n\t"

#elif defined(__loongarch__)

#define RSEQ_INJECT_INPUT \
	, [loop_cnt_1]"m"(loop_cnt[1]) \
	, [loop_cnt_2]"m"(loop_cnt[2]) \
	, [loop_cnt_3]"m"(loop_cnt[3]) \
	, [loop_cnt_4]"m"(loop_cnt[4]) \
	, [loop_cnt_5]"m"(loop_cnt[5]) \
	, [loop_cnt_6]"m"(loop_cnt[6])

#define INJECT_ASM_REG  "$r12"

#define RSEQ_INJECT_CLOBBER \
	, INJECT_ASM_REG

#define RSEQ_INJECT_ASM(n)                                      \
	"ld.w " INJECT_ASM_REG ", %[loop_cnt_" #n "]\n\t"         \
	"beqz " INJECT_ASM_REG ", 333f\n\t"                     \
	"222:\n\t"                                              \
	"addi.w  " INJECT_ASM_REG "," INJECT_ASM_REG ", -1\n\t"   \
	"bnez " INJECT_ASM_REG ", 222b\n\t"                     \
	"333:\n\t"

#else
#error unsupported target
#endif

#define RSEQ_INJECT_FAILED \
	nr_abort++;

#define RSEQ_INJECT_C(n) \
{ \
	int loc_i, loc_nr_loops = loop_cnt[n]; \
	\
	for (loc_i = 0; loc_i < loc_nr_loops; loc_i++) { \
		rseq_barrier(); \
	} \
	if (loc_nr_loops == -1 && opt_modulo) { \
		if (yield_mod_cnt == opt_modulo - 1) { \
			if (opt_sleep > 0) \
				poll(NULL, 0, opt_sleep); \
			if (opt_yield) \
				sched_yield(); \
			if (opt_signal) \
				raise(SIGUSR1); \
			yield_mod_cnt = 0; \
		} else { \
			yield_mod_cnt++; \
		} \
	} \
}

#else

#define printf_verbose(fmt, ...)

#endif /* BENCHMARK */

#include <rseq/rseq.h>

static enum rseq_mo opt_mo = RSEQ_MO_RELAXED;

static int sys_membarrier(int cmd, int flags, int cpu_id)
{
	return syscall(__NR_membarrier, cmd, flags, cpu_id);
}

#ifdef rseq_arch_has_load_add_load_load_add_store
#define TEST_MEMBARRIER
#endif

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
# ifdef TEST_MEMBARRIER
/*
 * Membarrier does not currently support targeting a mm_cid, so
 * issue the barrier on all cpus.
 */
static
int rseq_membarrier_expedited(__attribute__ ((unused)) int cpu)
{
	return sys_membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED_RSEQ,
			      0, 0);
}
# endif /* TEST_MEMBARRIER */
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
# ifdef TEST_MEMBARRIER
static
int rseq_membarrier_expedited(int cpu)
{
	return sys_membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED_RSEQ,
			      MEMBARRIER_CMD_FLAG_CPU, cpu);
}
# endif /* TEST_MEMBARRIER */
#endif

struct percpu_lock {
	intptr_t v;
};

struct spinlock_test_data {
	struct percpu_lock lock;
	intptr_t count;
};

struct spinlock_thread_test_data {
	struct spinlock_test_data __rseq_percpu *data;
	long long reps;
	int reg;
};

struct inc_test_data {
	intptr_t count;
};

struct inc_thread_test_data {
	struct inc_test_data __rseq_percpu *data;
	long long reps;
	int reg;
};

struct percpu_list_node {
	intptr_t data;
	struct percpu_list_node *next;
};

struct percpu_list {
	struct percpu_list_node *head;
};

#define BUFFER_ITEM_PER_CPU	100

struct percpu_buffer_node {
	intptr_t data;
};

struct percpu_buffer {
	intptr_t offset;
	intptr_t buflen;
	struct percpu_buffer_node **array;
};

#define MEMCPY_BUFFER_ITEM_PER_CPU	100

struct percpu_memcpy_buffer_node {
	intptr_t data1;
	uint64_t data2;
};

struct percpu_memcpy_buffer {
	intptr_t offset;
	intptr_t buflen;
	struct percpu_memcpy_buffer_node *array;
};

/* A simple percpu spinlock. Grabs lock on current cpu. */
static int rseq_this_cpu_lock(struct percpu_lock __rseq_percpu *lock)
{
	int cpu;

	for (;;) {
		int ret;

		cpu = get_current_cpu_id();
		if (cpu < 0) {
			fprintf(stderr, "pid: %d: tid: %d, cpu: %d: cid: %d\n",
				getpid(), (int) rseq_gettid(), rseq_current_cpu_raw(), cpu);
			abort();
		}
		ret = rseq_load_cbne_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
					 &rseq_percpu_ptr(lock, cpu)->v,
					 0, 1, cpu);
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

static void rseq_percpu_unlock(struct percpu_lock __rseq_percpu *lock, int cpu)
{
	assert(rseq_percpu_ptr(lock, cpu)->v == 1);
	/*
	 * Release lock, with release semantic. Matches
	 * rseq_smp_acquire__after_ctrl_dep().
	 */
	rseq_smp_store_release(&rseq_percpu_ptr(lock, cpu)->v, 0);
}

static void *test_percpu_spinlock_thread(void *arg)
{
	struct spinlock_thread_test_data *thread_data = (struct spinlock_thread_test_data *) arg;
	struct spinlock_test_data __rseq_percpu *data = thread_data->data;
	long long i, reps;

	reps = thread_data->reps;
	for (i = 0; i < reps; i++) {
		int cpu = rseq_this_cpu_lock(&data->lock);
		rseq_percpu_ptr(data, cpu)->count++;
		rseq_percpu_unlock(&data->lock, cpu);
#ifndef BENCHMARK
		if (i != 0 && !(i % (reps / 10)))
			printf_verbose("tid %d: count %lld\n",
				       (int) rseq_gettid(), i);
#endif
	}
	printf_verbose("tid %d: number of rseq abort: %d, signals delivered: %u\n",
		       (int) rseq_gettid(), nr_abort, signals_delivered);
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
	const int num_threads = opt_threads;
	int i, ret, max_nr_cpus;
	uint64_t sum;
	pthread_t test_threads[num_threads];
	struct spinlock_test_data __rseq_percpu *data;
	struct spinlock_thread_test_data thread_data[num_threads];
	struct rseq_mempool *mempool;
	struct rseq_mempool_attr *attr;

	attr = rseq_mempool_attr_create();
	if (!attr) {
		perror("rseq_mempool_attr_create");
		abort();
	}
	ret = rseq_mempool_attr_set_percpu(attr, RSEQ_MEMPOOL_STRIDE, 0);
	if (ret) {
		perror("rseq_mempool_attr_set_percpu");
		abort();
	}
	mempool = rseq_mempool_create("spinlock_test_data",
			sizeof(struct spinlock_test_data), attr);
	if (!mempool) {
		perror("rseq_mempool_create");
		abort();
	}
	rseq_mempool_attr_destroy(attr);
	max_nr_cpus = rseq_mempool_get_max_nr_cpus(mempool);
	data = (struct spinlock_test_data __rseq_percpu *)rseq_mempool_percpu_zmalloc(mempool);
	if (!data) {
		perror("rseq_mempool_percpu_zmalloc");
		abort();
	}

	for (i = 0; i < num_threads; i++) {
		thread_data[i].reps = opt_reps;
		if (opt_disable_mod <= 0 || (i % opt_disable_mod))
			thread_data[i].reg = 1;
		else
			thread_data[i].reg = 0;
		thread_data[i].data = data;
		ret = pthread_create(&test_threads[i], NULL,
				     test_percpu_spinlock_thread,
				     &thread_data[i]);
		if (ret) {
			errno = ret;
			perror("pthread_create");
			abort();
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(test_threads[i], NULL);
		if (ret) {
			errno = ret;
			perror("pthread_join");
			abort();
		}
	}

	sum = 0;
	for (i = 0; i < max_nr_cpus; i++)
		sum += rseq_percpu_ptr(data, i)->count;

	assert(sum == (uint64_t)opt_reps * num_threads);
	rseq_mempool_percpu_free(data);
	ret = rseq_mempool_destroy(mempool);
	if (ret) {
		perror("rseq_mempool_destroy");
		abort();
	}
}

static void *test_percpu_inc_thread(void *arg)
{
	struct inc_thread_test_data *thread_data = (struct inc_thread_test_data *) arg;
	struct inc_test_data __rseq_percpu *data = thread_data->data;
	long long i, reps;

	reps = thread_data->reps;
	for (i = 0; i < reps; i++) {
		int ret;

		do {
			int cpu;

			cpu = get_current_cpu_id();
			ret = rseq_load_add_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
					&rseq_percpu_ptr(data, cpu)->count, 1, cpu);
		} while (rseq_unlikely(ret));
#ifndef BENCHMARK
		if (i != 0 && !(i % (reps / 10)))
			printf_verbose("tid %d: count %lld\n",
				       (int) rseq_gettid(), i);
#endif
	}
	printf_verbose("tid %d: number of rseq abort: %d, signals delivered: %u\n",
		       (int) rseq_gettid(), nr_abort, signals_delivered);
	return NULL;
}

static void test_percpu_inc(void)
{
	const int num_threads = opt_threads;
	int i, ret, max_nr_cpus;
	uint64_t sum;
	pthread_t test_threads[num_threads];
	struct inc_test_data __rseq_percpu *data;
	struct inc_thread_test_data thread_data[num_threads];
	struct rseq_mempool *mempool;
	struct rseq_mempool_attr *attr;

	attr = rseq_mempool_attr_create();
	if (!attr) {
		perror("rseq_mempool_attr_create");
		abort();
	}
	ret = rseq_mempool_attr_set_percpu(attr, RSEQ_MEMPOOL_STRIDE, 0);
	if (ret) {
		perror("rseq_mempool_attr_set_percpu");
		abort();
	}
	mempool = rseq_mempool_create("inc_test_data",
			sizeof(struct inc_test_data), attr);
	if (!mempool) {
		perror("rseq_mempool_create");
		abort();
	}
	rseq_mempool_attr_destroy(attr);
	max_nr_cpus = rseq_mempool_get_max_nr_cpus(mempool);
	data = (struct inc_test_data __rseq_percpu *)rseq_mempool_percpu_zmalloc(mempool);
	if (!data) {
		perror("rseq_mempool_percpu_zmalloc");
		abort();
	}

	for (i = 0; i < num_threads; i++) {
		thread_data[i].reps = opt_reps;
		if (opt_disable_mod <= 0 || (i % opt_disable_mod))
			thread_data[i].reg = 1;
		else
			thread_data[i].reg = 0;
		thread_data[i].data = data;
		ret = pthread_create(&test_threads[i], NULL,
				     test_percpu_inc_thread,
				     &thread_data[i]);
		if (ret) {
			errno = ret;
			perror("pthread_create");
			abort();
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(test_threads[i], NULL);
		if (ret) {
			errno = ret;
			perror("pthread_join");
			abort();
		}
	}

	sum = 0;
	for (i = 0; i < max_nr_cpus; i++)
		sum += rseq_percpu_ptr(data, i)->count;

	assert(sum == (uint64_t)opt_reps * num_threads);
	rseq_mempool_percpu_free(data);
	ret = rseq_mempool_destroy(mempool);
	if (ret) {
		perror("rseq_mempool_destroy");
		abort();
	}
}

static void this_cpu_list_push(struct percpu_list __rseq_percpu *list,
			struct percpu_list_node *node,
			int *_cpu)
{
	int cpu;

	for (;;) {
		intptr_t *targetptr, newval, expect;
		struct percpu_list *cpulist;
		int ret;

		cpu = get_current_cpu_id();
		cpulist = rseq_percpu_ptr(list, cpu);
		/* Load list->c[cpu].head with single-copy atomicity. */
		expect = (intptr_t)RSEQ_READ_ONCE(cpulist->head);
		newval = (intptr_t)node;
		targetptr = (intptr_t *)&cpulist->head;
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
static struct percpu_list_node *this_cpu_list_pop(struct percpu_list __rseq_percpu *list,
					   int *_cpu)
{
	struct percpu_list_node *node = NULL;
	int cpu;

	for (;;) {
		struct percpu_list_node *head;
		intptr_t *targetptr, expectnot, *load;
		struct percpu_list *cpulist;
		long offset;
		int ret;

		cpu = get_current_cpu_id();
		cpulist = rseq_percpu_ptr(list, cpu);
		targetptr = (intptr_t *)&cpulist->head;
		expectnot = (intptr_t)NULL;
		offset = offsetof(struct percpu_list_node, next);
		load = (intptr_t *)&head;
		ret = rseq_load_cbeq_store_add_load_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
						 targetptr, expectnot,
						 offset, load, cpu);
		if (rseq_likely(!ret)) {
			node = head;
			break;
		}
		if (ret > 0)
			break;
		/* Retry if rseq aborts. */
	}
	if (_cpu)
		*_cpu = cpu;
	return node;
}

/*
 * __percpu_list_pop is not safe against concurrent accesses. Should
 * only be used on lists that are not concurrently modified.
 */
static struct percpu_list_node *__percpu_list_pop(struct percpu_list __rseq_percpu *list, int cpu)
{
	struct percpu_list *cpulist = rseq_percpu_ptr(list, cpu);
	struct percpu_list_node *node;

	node = cpulist->head;
	if (!node)
		return NULL;
	cpulist->head = node->next;
	return node;
}

static void *test_percpu_list_thread(void *arg)
{
	long long i, reps;
	struct percpu_list __rseq_percpu *list = (struct percpu_list __rseq_percpu *)arg;

	reps = opt_reps;
	for (i = 0; i < reps; i++) {
		struct percpu_list_node *node;

		node = this_cpu_list_pop(list, NULL);
		if (opt_yield)
			sched_yield();  /* encourage shuffling */
		if (node)
			this_cpu_list_push(list, node, NULL);
	}

	printf_verbose("tid %d: number of rseq abort: %d, signals delivered: %u\n",
		       (int) rseq_gettid(), nr_abort, signals_delivered);

	return NULL;
}

/* Simultaneous modification to a per-cpu linked list from many threads.  */
static void test_percpu_list(void)
{
	const int num_threads = opt_threads;
	int i, j, ret, max_nr_cpus;
	uint64_t sum = 0, expected_sum = 0;
	struct percpu_list __rseq_percpu *list;
	pthread_t test_threads[num_threads];
	struct rseq_mempool *mempool;
	struct rseq_mempool_attr *attr;

	attr = rseq_mempool_attr_create();
	if (!attr) {
		perror("rseq_mempool_attr_create");
		abort();
	}
	ret = rseq_mempool_attr_set_percpu(attr, RSEQ_MEMPOOL_STRIDE, 0);
	if (ret) {
		perror("rseq_mempool_attr_set_percpu");
		abort();
	}
	mempool = rseq_mempool_create("percpu_list",
			sizeof(struct percpu_list), attr);
	if (!mempool) {
		perror("rseq_mempool_create");
		abort();
	}
	rseq_mempool_attr_destroy(attr);
	max_nr_cpus = rseq_mempool_get_max_nr_cpus(mempool);
	list = (struct percpu_list __rseq_percpu *)rseq_mempool_percpu_zmalloc(mempool);
	if (!list) {
		perror("rseq_mempool_percpu_zmalloc");
		abort();
	}

	/* Generate list entries for every possible cpus. */
	for (i = 0; i < max_nr_cpus; i++) {
		for (j = 1; j <= 100; j++) {
			struct percpu_list *cpulist = rseq_percpu_ptr(list, i);
			struct percpu_list_node *node;

			expected_sum += j;

			node = (struct percpu_list_node *) malloc(sizeof(*node));
			assert(node);
			node->data = j;
			node->next = cpulist->head;
			cpulist->head = node;
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_create(&test_threads[i], NULL,
				     test_percpu_list_thread, list);
		if (ret) {
			errno = ret;
			perror("pthread_create");
			abort();
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(test_threads[i], NULL);
		if (ret) {
			errno = ret;
			perror("pthread_join");
			abort();
		}
	}

	for (i = 0; i < max_nr_cpus; i++) {
		struct percpu_list_node *node;

		while ((node = __percpu_list_pop(list, i))) {
			sum += node->data;
			free(node);
		}
	}

	/*
	 * All entries should now be accounted for.
	 */
	assert(sum == expected_sum);
	rseq_mempool_percpu_free(list);
	ret = rseq_mempool_destroy(mempool);
	if (ret) {
		perror("rseq_mempool_destroy");
		abort();
	}
}

static bool this_cpu_buffer_push(struct percpu_buffer __rseq_percpu *buffer,
			  struct percpu_buffer_node *node,
			  int *_cpu)
{
	bool result = false;
	int cpu;

	for (;;) {
		struct percpu_buffer *cpubuffer;
		intptr_t *targetptr_spec, newval_spec;
		intptr_t *targetptr_final, newval_final;
		intptr_t offset;
		int ret;

		cpu = get_current_cpu_id();
		cpubuffer = rseq_percpu_ptr(buffer, cpu);
		offset = RSEQ_READ_ONCE(cpubuffer->offset);
		if (offset == cpubuffer->buflen)
			break;
		newval_spec = (intptr_t)node;
		targetptr_spec = (intptr_t *)&cpubuffer->array[offset];
		newval_final = offset + 1;
		targetptr_final = &cpubuffer->offset;
		ret = rseq_load_cbne_store_store__ptr(opt_mo, RSEQ_PERCPU,
			targetptr_final, offset, targetptr_spec,
			newval_spec, newval_final, cpu);
		if (rseq_likely(!ret)) {
			result = true;
			break;
		}
		/* Retry if comparison fails or rseq aborts. */
	}
	if (_cpu)
		*_cpu = cpu;
	return result;
}

static struct percpu_buffer_node *this_cpu_buffer_pop(struct percpu_buffer __rseq_percpu *buffer,
					       int *_cpu)
{
	struct percpu_buffer_node *head;
	int cpu;

	for (;;) {
		struct percpu_buffer *cpubuffer;
		intptr_t *targetptr, newval;
		intptr_t offset;
		int ret;

		cpu = get_current_cpu_id();
		cpubuffer = rseq_percpu_ptr(buffer, cpu);
		/* Load offset with single-copy atomicity. */
		offset = RSEQ_READ_ONCE(cpubuffer->offset);
		if (offset == 0) {
			head = NULL;
			break;
		}
		head = RSEQ_READ_ONCE(cpubuffer->array[offset - 1]);
		newval = offset - 1;
		targetptr = (intptr_t *)&cpubuffer->offset;
		ret = rseq_load_cbne_load_cbne_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
			targetptr, offset,
			(intptr_t *)&cpubuffer->array[offset - 1],
			(intptr_t)head, newval, cpu);
		if (rseq_likely(!ret))
			break;
		/* Retry if comparison fails or rseq aborts. */
	}
	if (_cpu)
		*_cpu = cpu;
	return head;
}

/*
 * __percpu_buffer_pop is not safe against concurrent accesses. Should
 * only be used on buffers that are not concurrently modified.
 */
static struct percpu_buffer_node *__percpu_buffer_pop(struct percpu_buffer __rseq_percpu *buffer,
					       int cpu)
{
	struct percpu_buffer *cpubuffer;
	struct percpu_buffer_node *head;
	intptr_t offset;

	cpubuffer = rseq_percpu_ptr(buffer, cpu);
	offset = cpubuffer->offset;
	if (offset == 0)
		return NULL;
	head = cpubuffer->array[offset - 1];
	cpubuffer->offset = offset - 1;
	return head;
}

static void *test_percpu_buffer_thread(void *arg)
{
	long long i, reps;
	struct percpu_buffer __rseq_percpu *buffer = (struct percpu_buffer __rseq_percpu *)arg;

	reps = opt_reps;
	for (i = 0; i < reps; i++) {
		struct percpu_buffer_node *node;

		node = this_cpu_buffer_pop(buffer, NULL);
		if (opt_yield)
			sched_yield();  /* encourage shuffling */
		if (node) {
			if (!this_cpu_buffer_push(buffer, node, NULL)) {
				/* Should increase buffer size. */
				abort();
			}
		}
	}

	printf_verbose("tid %d: number of rseq abort: %d, signals delivered: %u\n",
		       (int) rseq_gettid(), nr_abort, signals_delivered);

	return NULL;
}

/* Simultaneous modification to a per-cpu buffer from many threads.  */
static void test_percpu_buffer(void)
{
	const int num_threads = opt_threads;
	int i, j, ret, max_nr_cpus;
	uint64_t sum = 0, expected_sum = 0;
	struct percpu_buffer __rseq_percpu *buffer;
	pthread_t test_threads[num_threads];
	struct rseq_mempool *mempool;
	struct rseq_mempool_attr *attr;

	attr = rseq_mempool_attr_create();
	if (!attr) {
		perror("rseq_mempool_attr_create");
		abort();
	}
	ret = rseq_mempool_attr_set_percpu(attr, RSEQ_MEMPOOL_STRIDE, 0);
	if (ret) {
		perror("rseq_mempool_attr_set_percpu");
		abort();
	}
	mempool = rseq_mempool_create("percpu_buffer",
			sizeof(struct percpu_buffer), attr);
	if (!mempool) {
		perror("rseq_mempool_create");
		abort();
	}
	rseq_mempool_attr_destroy(attr);
	max_nr_cpus = rseq_mempool_get_max_nr_cpus(mempool);
	buffer = (struct percpu_buffer __rseq_percpu *)rseq_mempool_percpu_zmalloc(mempool);
	if (!buffer) {
		perror("rseq_mempool_percpu_zmalloc");
		abort();
	}

	/* Generate list entries for every possible cpu. */
	for (i = 0; i < max_nr_cpus; i++) {
		struct percpu_buffer *cpubuffer;

		cpubuffer = rseq_percpu_ptr(buffer, i);
		/* Worse-case is every item in same CPU. */
		cpubuffer->array =
			(struct percpu_buffer_node **)
			malloc(sizeof(*cpubuffer->array) * max_nr_cpus *
			       BUFFER_ITEM_PER_CPU);
		assert(cpubuffer->array);
		cpubuffer->buflen = max_nr_cpus * BUFFER_ITEM_PER_CPU;
		for (j = 1; j <= BUFFER_ITEM_PER_CPU; j++) {
			struct percpu_buffer_node *node;

			expected_sum += j;

			/*
			 * We could theoretically put the word-sized
			 * "data" directly in the buffer. However, we
			 * want to model objects that would not fit
			 * within a single word, so allocate an object
			 * for each node.
			 */
			node = (struct percpu_buffer_node *) malloc(sizeof(*node));
			assert(node);
			node->data = j;
			cpubuffer->array[j - 1] = node;
			cpubuffer->offset++;
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_create(&test_threads[i], NULL,
				     test_percpu_buffer_thread, buffer);
		if (ret) {
			errno = ret;
			perror("pthread_create");
			abort();
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(test_threads[i], NULL);
		if (ret) {
			errno = ret;
			perror("pthread_join");
			abort();
		}
	}

	for (i = 0; i < max_nr_cpus; i++) {
		struct percpu_buffer *cpubuffer;
		struct percpu_buffer_node *node;

		cpubuffer = rseq_percpu_ptr(buffer, i);
		while ((node = __percpu_buffer_pop(buffer, i))) {
			sum += node->data;
			free(node);
		}
		free(cpubuffer->array);
	}

	/*
	 * All entries should now be accounted for.
	 */
	assert(sum == expected_sum);
	rseq_mempool_percpu_free(buffer);
	ret = rseq_mempool_destroy(mempool);
	if (ret) {
		perror("rseq_mempool_destroy");
		abort();
	}
}

static bool this_cpu_memcpy_buffer_push(struct percpu_memcpy_buffer __rseq_percpu *buffer,
				 struct percpu_memcpy_buffer_node item,
				 int *_cpu)
{
	bool result = false;
	int cpu;

	for (;;) {
		struct percpu_memcpy_buffer *cpubuffer;
		intptr_t *targetptr_final, newval_final, offset;
		char *destptr, *srcptr;
		size_t copylen;
		int ret;

		cpu = get_current_cpu_id();
		cpubuffer = rseq_percpu_ptr(buffer, cpu);
		/* Load offset with single-copy atomicity. */
		offset = RSEQ_READ_ONCE(cpubuffer->offset);
		if (offset == cpubuffer->buflen)
			break;
		destptr = (char *)&cpubuffer->array[offset];
		srcptr = (char *)&item;
		/* copylen must be <= 4kB. */
		copylen = sizeof(item);
		newval_final = offset + 1;
		targetptr_final = &cpubuffer->offset;
		ret = rseq_load_cbne_memcpy_store__ptr(
			opt_mo, RSEQ_PERCPU,
			targetptr_final, offset,
			destptr, srcptr, copylen,
			newval_final, cpu);
		if (rseq_likely(!ret)) {
			result = true;
			break;
		}
		/* Retry if comparison fails or rseq aborts. */
	}
	if (_cpu)
		*_cpu = cpu;
	return result;
}

static bool this_cpu_memcpy_buffer_pop(struct percpu_memcpy_buffer __rseq_percpu *buffer,
				struct percpu_memcpy_buffer_node *item,
				int *_cpu)
{
	bool result = false;
	int cpu;

	for (;;) {
		struct percpu_memcpy_buffer *cpubuffer;
		intptr_t *targetptr_final, newval_final, offset;
		char *destptr, *srcptr;
		size_t copylen;
		int ret;

		cpu = get_current_cpu_id();
		cpubuffer = rseq_percpu_ptr(buffer, cpu);
		/* Load offset with single-copy atomicity. */
		offset = RSEQ_READ_ONCE(cpubuffer->offset);
		if (offset == 0)
			break;
		destptr = (char *)item;
		srcptr = (char *)&cpubuffer->array[offset - 1];
		/* copylen must be <= 4kB. */
		copylen = sizeof(*item);
		newval_final = offset - 1;
		targetptr_final = &cpubuffer->offset;
		ret = rseq_load_cbne_memcpy_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
			targetptr_final, offset, destptr, srcptr, copylen,
			newval_final, cpu);
		if (rseq_likely(!ret)) {
			result = true;
			break;
		}
		/* Retry if comparison fails or rseq aborts. */
	}
	if (_cpu)
		*_cpu = cpu;
	return result;
}

/*
 * __percpu_memcpy_buffer_pop is not safe against concurrent accesses. Should
 * only be used on buffers that are not concurrently modified.
 */
static bool __percpu_memcpy_buffer_pop(struct percpu_memcpy_buffer __rseq_percpu *buffer,
				struct percpu_memcpy_buffer_node *item,
				int cpu)
{
	struct percpu_memcpy_buffer *cpubuffer;
	intptr_t offset;

	cpubuffer = rseq_percpu_ptr(buffer, cpu);
	offset = cpubuffer->offset;
	if (offset == 0)
		return false;
	memcpy(item, &cpubuffer->array[offset - 1], sizeof(*item));
	cpubuffer->offset = offset - 1;
	return true;
}

static void *test_percpu_memcpy_buffer_thread(void *arg)
{
	long long i, reps;
	struct percpu_memcpy_buffer __rseq_percpu *buffer = (struct percpu_memcpy_buffer __rseq_percpu *)arg;

	reps = opt_reps;
	for (i = 0; i < reps; i++) {
		struct percpu_memcpy_buffer_node item;
		bool result;

		result = this_cpu_memcpy_buffer_pop(buffer, &item, NULL);
		if (opt_yield)
			sched_yield();  /* encourage shuffling */
		if (result) {
			if (!this_cpu_memcpy_buffer_push(buffer, item, NULL)) {
				/* Should increase buffer size. */
				abort();
			}
		}
	}

	printf_verbose("tid %d: number of rseq abort: %d, signals delivered: %u\n",
		       (int) rseq_gettid(), nr_abort, signals_delivered);

	return NULL;
}

/* Simultaneous modification to a per-cpu buffer from many threads.  */
static void test_percpu_memcpy_buffer(void)
{
	const int num_threads = opt_threads;
	int i, j, ret, max_nr_cpus;
	uint64_t sum = 0, expected_sum = 0;
	struct percpu_memcpy_buffer *buffer;
	pthread_t test_threads[num_threads];
	struct rseq_mempool *mempool;
	struct rseq_mempool_attr *attr;

	attr = rseq_mempool_attr_create();
	if (!attr) {
		perror("rseq_mempool_attr_create");
		abort();
	}
	ret = rseq_mempool_attr_set_percpu(attr, RSEQ_MEMPOOL_STRIDE, 0);
	if (ret) {
		perror("rseq_mempool_attr_set_percpu");
		abort();
	}
	mempool = rseq_mempool_create("percpu_memcpy_buffer",
			sizeof(struct percpu_memcpy_buffer), attr);
	if (!mempool) {
		perror("rseq_mempool_create");
		abort();
	}
	rseq_mempool_attr_destroy(attr);
	max_nr_cpus = rseq_mempool_get_max_nr_cpus(mempool);
	buffer = (struct percpu_memcpy_buffer __rseq_percpu *)rseq_mempool_percpu_zmalloc(mempool);
	if (!buffer) {
		perror("rseq_mempool_percpu_zmalloc");
		abort();
	}

	/* Generate list entries for every possible cpu. */
	for (i = 0; i < max_nr_cpus; i++) {
		struct percpu_memcpy_buffer *cpubuffer;

		cpubuffer = rseq_percpu_ptr(buffer, i);
		/* Worse-case is every item in same CPU. */
		cpubuffer->array =
			(struct percpu_memcpy_buffer_node *)
			malloc(sizeof(*cpubuffer->array) * max_nr_cpus *
			       MEMCPY_BUFFER_ITEM_PER_CPU);
		assert(cpubuffer->array);
		cpubuffer->buflen = max_nr_cpus * MEMCPY_BUFFER_ITEM_PER_CPU;
		for (j = 1; j <= MEMCPY_BUFFER_ITEM_PER_CPU; j++) {
			expected_sum += 2 * j + 1;

			/*
			 * We could theoretically put the word-sized
			 * "data" directly in the buffer. However, we
			 * want to model objects that would not fit
			 * within a single word, so allocate an object
			 * for each node.
			 */
			cpubuffer->array[j - 1].data1 = j;
			cpubuffer->array[j - 1].data2 = j + 1;
			cpubuffer->offset++;
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_create(&test_threads[i], NULL,
				     test_percpu_memcpy_buffer_thread,
				     buffer);
		if (ret) {
			errno = ret;
			perror("pthread_create");
			abort();
		}
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(test_threads[i], NULL);
		if (ret) {
			errno = ret;
			perror("pthread_join");
			abort();
		}
	}

	for (i = 0; i < max_nr_cpus; i++) {
		struct percpu_memcpy_buffer_node item;
		struct percpu_memcpy_buffer *cpubuffer;

		cpubuffer = rseq_percpu_ptr(buffer, i);
		while (__percpu_memcpy_buffer_pop(buffer, &item, i)) {
			sum += item.data1;
			sum += item.data2;
		}
		free(cpubuffer->array);
	}

	/*
	 * All entries should now be accounted for.
	 */
	assert(sum == expected_sum);
	rseq_mempool_percpu_free(buffer);
	ret = rseq_mempool_destroy(mempool);
	if (ret) {
		perror("rseq_mempool_destroy");
		abort();
	}
}

static void test_signal_interrupt_handler(__attribute__ ((unused)) int signo)
{
	signals_delivered++;
}

static int set_signal_handler(void)
{
	int ret = 0;
	struct sigaction sa;
	sigset_t sigset;

	ret = sigemptyset(&sigset);
	if (ret < 0) {
		perror("sigemptyset");
		return ret;
	}

	sa.sa_handler = test_signal_interrupt_handler;
	sa.sa_mask = sigset;
	sa.sa_flags = 0;
	ret = sigaction(SIGUSR1, &sa, NULL);
	if (ret < 0) {
		perror("sigaction");
		return ret;
	}

	printf_verbose("Signal handler set for SIGUSR1\n");

	return ret;
}

static
bool membarrier_private_expedited_rseq_available(void)
{
	int status = sys_membarrier(MEMBARRIER_CMD_QUERY, 0, 0);

	if (status < 0) {
		perror("membarrier");
		return false;
	}
	if (!(status & MEMBARRIER_CMD_PRIVATE_EXPEDITED_RSEQ))
		return false;
	return true;
}

/* Test MEMBARRIER_CMD_PRIVATE_RESTART_RSEQ_ON_CPU membarrier command. */
#ifdef TEST_MEMBARRIER
struct test_membarrier_thread_args {
	struct rseq_mempool *mempool;
	struct percpu_list __rseq_percpu *percpu_list_ptr;
	int stop;
	int max_nr_cpus;
};

/* Worker threads modify data in their "active" percpu lists. */
static
void *test_membarrier_worker_thread(void *arg)
{
	struct test_membarrier_thread_args *args =
		(struct test_membarrier_thread_args *)arg;
	const long long iters = opt_reps;
	long long i;

	/* Wait for initialization. */
	while (!rseq_smp_load_acquire(&args->percpu_list_ptr)) { }

	for (i = 0; i < iters; ++i) {
		int ret;

		do {
			int cpu = get_current_cpu_id();

			ret = rseq_load_add_load_load_add_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU,
				(intptr_t *) &args->percpu_list_ptr,
				(RSEQ_MEMPOOL_STRIDE * cpu) + offsetof(struct percpu_list, head),
				1, cpu);
		} while (rseq_unlikely(ret));
	}

	return NULL;
}

static
struct percpu_list __rseq_percpu *test_membarrier_alloc_percpu_list(struct rseq_mempool *mempool)
{
	struct percpu_list __rseq_percpu *list;
	int i, max_nr_cpus;

	max_nr_cpus = rseq_mempool_get_max_nr_cpus(mempool);
	list = (struct percpu_list __rseq_percpu *)rseq_mempool_percpu_zmalloc(mempool);
	if (!list) {
		perror("rseq_mempool_percpu_zmalloc");
		return NULL;
	}
	for (i = 0; i < max_nr_cpus; i++) {
		struct percpu_list *cpulist = rseq_percpu_ptr(list, i);
		struct percpu_list_node *node;

		node = (struct percpu_list_node *) malloc(sizeof(*node));
		assert(node);
		node->data = 0;
		node->next = NULL;
		cpulist->head = node;
	}
	return list;
}

static
void test_membarrier_free_percpu_list(struct test_membarrier_thread_args *args,
		struct percpu_list __rseq_percpu *list)
{
	int i;

	for (i = 0; i < args->max_nr_cpus; i++)
		free(rseq_percpu_ptr(list, i)->head);
	rseq_mempool_percpu_free(list);
}

static
long long test_membarrier_count_percpu_list(struct test_membarrier_thread_args *args,
		struct percpu_list __rseq_percpu *list)
{
	long long total_count = 0;
	int i;

	for (i = 0; i < args->max_nr_cpus; i++)
		total_count += rseq_percpu_ptr(list, i)->head->data;
	return total_count;
}

/*
 * The manager thread swaps per-cpu lists that worker threads see,
 * and validates that there are no unexpected modifications.
 */
static
void *test_membarrier_manager_thread(void *arg)
{
	struct test_membarrier_thread_args *args =
		(struct test_membarrier_thread_args *)arg;
	struct percpu_list __rseq_percpu *list_a, __rseq_percpu *list_b;
	intptr_t expect_a = 0, expect_b = 0;
	int cpu_a = 0, cpu_b = 0;
	struct rseq_mempool *mempool;
	int ret;
	long long total_count = 0;
	struct rseq_mempool_attr *attr;

	attr = rseq_mempool_attr_create();
	if (!attr) {
		perror("rseq_mempool_attr_create");
		abort();
	}
	ret = rseq_mempool_attr_set_percpu(attr, RSEQ_MEMPOOL_STRIDE, 0);
	if (ret) {
		perror("rseq_mempool_attr_set_percpu");
		abort();
	}
	mempool = rseq_mempool_create("percpu_list",
			sizeof(struct percpu_list), attr);
	if (!mempool) {
		perror("rseq_mempool_create");
		abort();
	}
	rseq_mempool_attr_destroy(attr);
	args->max_nr_cpus = rseq_mempool_get_max_nr_cpus(mempool);
	args->mempool = mempool;

	/* Init lists. */
	list_a = test_membarrier_alloc_percpu_list(mempool);
	assert(list_a);
	list_b = test_membarrier_alloc_percpu_list(mempool);
	assert(list_b);

	/* Initialize lists before publishing them. */
	rseq_smp_wmb();

	RSEQ_WRITE_ONCE(args->percpu_list_ptr, list_a);

	while (!RSEQ_READ_ONCE(args->stop)) {
		/* list_a is "active". */
		cpu_a = rand() % args->max_nr_cpus;
		/*
		 * As list_b is "inactive", we should never see changes
		 * to list_b.
		 */
		if (expect_b != RSEQ_READ_ONCE(rseq_percpu_ptr(list_b, cpu_b)->head->data)) {
			fprintf(stderr, "Membarrier test failed\n");
			abort();
		}

		/* Make list_b "active". */
		RSEQ_WRITE_ONCE(args->percpu_list_ptr, list_b);
		if (rseq_membarrier_expedited(cpu_a) &&
				errno != ENXIO /* missing CPU */) {
			perror("sys_membarrier");
			abort();
		}
		/*
		 * Cpu A should now only modify list_b, so the values
		 * in list_a should be stable.
		 */
		expect_a = RSEQ_READ_ONCE(rseq_percpu_ptr(list_a, cpu_a)->head->data);

		cpu_b = rand() % args->max_nr_cpus;
		/*
		 * As list_a is "inactive", we should never see changes
		 * to list_a.
		 */
		if (expect_a != RSEQ_READ_ONCE(rseq_percpu_ptr(list_a, cpu_a)->head->data)) {
			fprintf(stderr, "Membarrier test failed\n");
			abort();
		}

		/* Make list_a "active". */
		RSEQ_WRITE_ONCE(args->percpu_list_ptr, list_a);
		if (rseq_membarrier_expedited(cpu_b) &&
				errno != ENXIO /* missing CPU */) {
			perror("sys_membarrier");
			abort();
		}
		/* Remember a value from list_b. */
		expect_b = RSEQ_READ_ONCE(rseq_percpu_ptr(list_b, cpu_b)->head->data);
	}

	total_count += test_membarrier_count_percpu_list(args, list_a);
	total_count += test_membarrier_count_percpu_list(args, list_b);

	/* Validate that we observe the right number of increments. */
	if (total_count != opt_threads * opt_reps) {
		fprintf(stderr, "Error: Observed %lld increments, expected %lld\n",
			total_count, opt_threads * opt_reps);
		abort();
	}
	test_membarrier_free_percpu_list(args, list_a);
	test_membarrier_free_percpu_list(args, list_b);

	ret = rseq_mempool_destroy(mempool);
	if (ret) {
		perror("rseq_mempool_destroy");
		abort();
	}

	return NULL;
}

static
void test_membarrier(void)
{
	const int num_threads = opt_threads;
	struct test_membarrier_thread_args thread_args;
	pthread_t worker_threads[num_threads];
	pthread_t manager_thread;
	int i, ret;

	if (!membarrier_private_expedited_rseq_available()) {
		fprintf(stderr, "Membarrier private expedited rseq not available. "
				"Skipping membarrier test.\n");
		return;
	}
	if (sys_membarrier(MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_RSEQ, 0, 0)) {
		perror("sys_membarrier");
		abort();
	}

	thread_args.percpu_list_ptr = NULL;
	thread_args.stop = 0;
	ret = pthread_create(&manager_thread, NULL,
			test_membarrier_manager_thread, &thread_args);
	if (ret) {
		errno = ret;
		perror("pthread_create");
		abort();
	}

	for (i = 0; i < num_threads; i++) {
		ret = pthread_create(&worker_threads[i], NULL,
				test_membarrier_worker_thread, &thread_args);
		if (ret) {
			errno = ret;
			perror("pthread_create");
			abort();
		}
	}


	for (i = 0; i < num_threads; i++) {
		ret = pthread_join(worker_threads[i], NULL);
		if (ret) {
			errno = ret;
			perror("pthread_join");
			abort();
		}
	}

	RSEQ_WRITE_ONCE(thread_args.stop, 1);
	ret = pthread_join(manager_thread, NULL);
	if (ret) {
		errno = ret;
		perror("pthread_join");
		abort();
	}
}
#else /* TEST_MEMBARRIER */
static
void test_membarrier(void)
{
	if (!membarrier_private_expedited_rseq_available()) {
		fprintf(stderr, "Membarrier private expedited rseq not available. "
				"Skipping membarrier test.\n");
		return;
	}
	fprintf(stderr, "rseq_load_add_load_load_add_store__ptr is not implemented on this architecture. "
			"Skipping membarrier test.\n");
}
#endif

static void show_usage(char **argv)
{
	printf("Usage : %s <OPTIONS>\n",
		argv[0]);
	printf("OPTIONS:\n");
	printf("	[-1 loops] Number of loops for delay injection 1\n");
	printf("	[-2 loops] Number of loops for delay injection 2\n");
	printf("	[-3 loops] Number of loops for delay injection 3\n");
	printf("	[-4 loops] Number of loops for delay injection 4\n");
	printf("	[-5 loops] Number of loops for delay injection 5\n");
	printf("	[-6 loops] Number of loops for delay injection 6\n");
	printf("	[-7 loops] Number of loops for delay injection 7 (-1 to enable -m)\n");
	printf("	[-8 loops] Number of loops for delay injection 8 (-1 to enable -m)\n");
	printf("	[-9 loops] Number of loops for delay injection 9 (-1 to enable -m)\n");
	printf("	[-m N] Yield/sleep/kill every modulo N (default 0: disabled) (>= 0)\n");
	printf("	[-y] Yield\n");
	printf("	[-k] Kill thread with signal\n");
	printf("	[-s S] S: =0: disabled (default), >0: sleep time (ms)\n");
	printf("	[-t N] Number of threads (default 200)\n");
	printf("	[-r N] Number of repetitions per thread (default 5000)\n");
	printf("	[-d] Disable rseq system call (no initialization)\n");
	printf("	[-D M] Disable rseq for each M threads\n");
	printf("	[-T test] Choose test: (s)pinlock, (l)ist, (b)uffer, (m)emcpy, (i)ncrement, membarrie(r)\n");
	printf("	[-M] Push into buffer and memcpy buffer with memory barriers.\n");
	printf("	[-c] Check if the rseq syscall is available.\n");
	printf("	[-v] Verbose output.\n");
	printf("	[-h] Show this help.\n");
	printf("\n");
}

int main(int argc, char **argv)
{
	int i;

	for (i = 1; i < argc; i++) {
		if (argv[i][0] != '-')
			continue;
		switch (argv[i][1]) {
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			if (argc < i + 2) {
				show_usage(argv);
				goto error;
			}
			loop_cnt[argv[i][1] - '0'] = atol(argv[i + 1]);
			i++;
			break;
		case 'm':
			if (argc < i + 2) {
				show_usage(argv);
				goto error;
			}
			opt_modulo = atol(argv[i + 1]);
			if (opt_modulo < 0) {
				show_usage(argv);
				goto error;
			}
			i++;
			break;
		case 's':
			if (argc < i + 2) {
				show_usage(argv);
				goto error;
			}
			opt_sleep = atol(argv[i + 1]);
			if (opt_sleep < 0) {
				show_usage(argv);
				goto error;
			}
			i++;
			break;
		case 'y':
			opt_yield = 1;
			break;
		case 'k':
			opt_signal = 1;
			break;
		case 'd':
			opt_disable_rseq = 1;
			break;
		case 'D':
			if (argc < i + 2) {
				show_usage(argv);
				goto error;
			}
			opt_disable_mod = atol(argv[i + 1]);
			if (opt_disable_mod < 0) {
				show_usage(argv);
				goto error;
			}
			i++;
			break;
		case 't':
			if (argc < i + 2) {
				show_usage(argv);
				goto error;
			}
			opt_threads = atol(argv[i + 1]);
			if (opt_threads < 0) {
				show_usage(argv);
				goto error;
			}
			i++;
			break;
		case 'r':
			if (argc < i + 2) {
				show_usage(argv);
				goto error;
			}
			opt_reps = atoll(argv[i + 1]);
			if (opt_reps < 0) {
				show_usage(argv);
				goto error;
			}
			i++;
			break;
		case 'h':
			show_usage(argv);
			goto end;
		case 'T':
			if (argc < i + 2) {
				show_usage(argv);
				goto error;
			}
			opt_test = *argv[i + 1];
			switch (opt_test) {
			case 's':
			case 'l':
			case 'i':
			case 'b':
			case 'm':
			case 'r':
				break;
			default:
				show_usage(argv);
				goto error;
			}
			i++;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'M':
			opt_mo = RSEQ_MO_RELEASE;
			break;
		case 'c':
			if (rseq_available(RSEQ_AVAILABLE_QUERY_KERNEL)) {
				printf_verbose("The rseq syscall is available.\n");
				goto end;
			} else {
				printf_verbose("The rseq syscall is unavailable.\n");
				goto no_rseq;
			}
		default:
			show_usage(argv);
			goto error;
		}
	}

	if (rseq_init() != RSEQ_INIT_OK) {
		printf_verbose("librseq initialization failed\n");
		goto no_rseq;
	}

	loop_cnt_1 = loop_cnt[1];
	loop_cnt_2 = loop_cnt[2];
	loop_cnt_3 = loop_cnt[3];
	loop_cnt_4 = loop_cnt[4];
	loop_cnt_5 = loop_cnt[5];
	loop_cnt_6 = loop_cnt[6];

	if (set_signal_handler())
		goto error;

	if (!opt_disable_rseq && !rseq_validate_cpu_id()) {
		printf_verbose("The rseq cpu id getter is unavailable\n");
		goto no_rseq;
	}
	switch (opt_test) {
	case 's':
		printf_verbose("spinlock\n");
		test_percpu_spinlock();
		break;
	case 'l':
		printf_verbose("linked list\n");
		test_percpu_list();
		break;
	case 'b':
		printf_verbose("buffer\n");
		test_percpu_buffer();
		break;
	case 'm':
		printf_verbose("memcpy buffer\n");
		test_percpu_memcpy_buffer();
		break;
	case 'i':
		printf_verbose("counter increment\n");
		test_percpu_inc();
		break;
	case 'r':
		printf_verbose("membarrier\n");
		test_membarrier();
		break;
	}
end:
	return 0;

error:
	return -1;

no_rseq:
	return 2;
}
