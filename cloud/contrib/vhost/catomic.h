/*
 * More traditional interfaces to atomic operations.
 * Modelled after QEMU qemu/atomic.h.
 * References:
 *    https://en.wikipedia.org/wiki/Memory_ordering
 *    https://en.cppreference.com/w/c/atomic
 *    https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html
 *    QEMU docs/devel/atomics.rst
 *
 * Note: definitions in this file use GCC intrinsics and macros rather than C11
 * stdatomic.h.  The reason is that the former operate on regular scalar types
 * while the latter wants them to be _Atomic-qualified, which also
 * (counter-intuitively) changes the behavior of the regular C accesses (i.e.
 * those not done via catomic_xxx functions).
 */

#pragma once

/* Compiler barrier */
#define barrier()   __atomic_signal_fence(__ATOMIC_ACQ_REL)

/*
 * Reportedly __atomic_thread_fence does not include a compiler barrier, so add
 * one here.
 */
#define smp_mb()                        \
    ({ barrier(); __atomic_thread_fence(__ATOMIC_SEQ_CST); })
#define smp_mb_release()                \
    ({ barrier(); __atomic_thread_fence(__ATOMIC_RELEASE); })
#define smp_mb_acquire()                \
    ({ barrier(); __atomic_thread_fence(__ATOMIC_ACQUIRE); })
/*
 * FIXME: reportedly current compilers promote consume order to acquire and
 * slow this down unnecessarily.  This seems not to be the case on x86_64; need
 * to recheck if we ever build for another arch.
 */
#ifndef __x86_64__
#error Verify smp_read_barrier_depends incurs no extra costs
#endif
#define smp_read_barrier_depends()      \
    ({ barrier(); __atomic_thread_fence(__ATOMIC_CONSUME); })

#define smp_wmb()   smp_mb_release()
#define smp_rmb()   smp_mb_acquire()

#define catomic_read(ptr)       __atomic_load_n(ptr, __ATOMIC_RELAXED)
#define catomic_set(ptr, val)   __atomic_store_n(ptr, val, __ATOMIC_RELAXED)

#define catomic_load_acquire(ptr)        \
    __atomic_load_n(ptr, __ATOMIC_ACQUIRE)
#define catomic_store_release(ptr, val)  \
    __atomic_store_n(ptr, val, __ATOMIC_RELEASE)

/*
 * FIXME: catomic_rcu_read potentially has the same issue with consume order as
 * smp_read_barrier_depends, see above.
 */
#ifndef __x86_64__
#error Verify catomic_rcu_read incurs no extra costs
#endif
#define catomic_rcu_read(ptr)      __atomic_load_n(ptr, __ATOMIC_CONSUME)
#define catomic_rcu_set(ptr, val)  __atomic_store_n(ptr, val, __ATOMIC_RELEASE)

/* All the remaining operations are fully sequentially consistent */
#define catomic_xchg(ptr, val)           \
    __atomic_exchange_n(ptr, val, __ATOMIC_SEQ_CST)
#define catomic_cmpxchg(ptr, old, new)    ({                                \
    __auto_type _old = (old);                                               \
    (void) __atomic_compare_exchange_n(ptr, &_old, new, false,              \
                                       __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST); \
    _old; })

#define catomic_fetch_add(ptr, n) __atomic_fetch_add(ptr, n, __ATOMIC_SEQ_CST)
#define catomic_fetch_sub(ptr, n) __atomic_fetch_sub(ptr, n, __ATOMIC_SEQ_CST)
#define catomic_fetch_and(ptr, n) __atomic_fetch_and(ptr, n, __ATOMIC_SEQ_CST)
#define catomic_fetch_or(ptr, n)  __atomic_fetch_or(ptr, n, __ATOMIC_SEQ_CST)
#define catomic_fetch_xor(ptr, n) __atomic_fetch_xor(ptr, n, __ATOMIC_SEQ_CST)

#define catomic_fetch_inc(ptr) catomic_fetch_add(ptr, 1)
#define catomic_fetch_dec(ptr) catomic_fetch_sub(ptr, 1)

#define catomic_add(ptr, n) ((void) catomic_fetch_add(ptr, n))
#define catomic_sub(ptr, n) ((void) catomic_fetch_sub(ptr, n))
#define catomic_and(ptr, n) ((void) catomic_fetch_and(ptr, n))
#define catomic_or(ptr, n)  ((void) catomic_fetch_or(ptr, n))
#define catomic_xor(ptr, n) ((void) catomic_fetch_xor(ptr, n))
#define catomic_inc(ptr)    ((void) catomic_fetch_inc(ptr))
#define catomic_dec(ptr)    ((void) catomic_fetch_dec(ptr))
