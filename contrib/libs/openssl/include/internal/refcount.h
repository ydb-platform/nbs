#include <contrib/libs/openssl/redef.h>
/*
 * Copyright 2016-2019 The OpenSSL Project Authors. All Rights Reserved.
 *
 * Licensed under the OpenSSL license (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 */
#ifndef OSSL_INTERNAL_REFCOUNT_H
# define OSSL_INTERNAL_REFCOUNT_H

/* Used to checking reference counts, most while doing perl5 stuff :-) */
# if defined(OPENSSL_NO_STDIO)
#  if defined(REF_PRINT)
#   error "REF_PRINT requires stdio"
#  endif
# endif

# if defined(__SANITIZE_THREAD__)
#  define OSSL_TSAN_BUILD
# elif defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#   define OSSL_TSAN_BUILD
#  endif
# endif

# if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L \
     && !defined(__STDC_NO_ATOMICS__)
#  include <stdatomic.h>
#  define HAVE_C11_ATOMICS
# endif

# if defined(HAVE_C11_ATOMICS) && defined(ATOMIC_INT_LOCK_FREE) \
     && ATOMIC_INT_LOCK_FREE > 0

#  define HAVE_ATOMICS 1

typedef _Atomic int CRYPTO_REF_COUNT;

static inline int CRYPTO_UP_REF(_Atomic int *val, int *ret, void *lock)
{
    *ret = atomic_fetch_add_explicit(val, 1, memory_order_relaxed) + 1;
    return 1;
}

/*
 * The decrement must release prior accesses to the object. When the count
 * reaches zero, an acquire fence pairs with earlier releases before the
 * object is destroyed. TSAN does not understand this fence, so use acq_rel
 * for the decrement in TSAN builds.
 */
static inline int CRYPTO_DOWN_REF(_Atomic int *val, int *ret, void *lock)
{
#  ifdef OSSL_TSAN_BUILD
    *ret = atomic_fetch_sub_explicit(val, 1, memory_order_acq_rel) - 1;
#  else
    *ret = atomic_fetch_sub_explicit(val, 1, memory_order_release) - 1;
    if (*ret == 0)
        atomic_thread_fence(memory_order_acquire);
#  endif
    return 1;
}

# elif defined(__GNUC__) && defined(__ATOMIC_RELAXED) && __GCC_ATOMIC_INT_LOCK_FREE > 0

#  define HAVE_ATOMICS 1

typedef int CRYPTO_REF_COUNT;

static __inline__ int CRYPTO_UP_REF(int *val, int *ret, void * /*lock*/)
{
    *ret = __atomic_fetch_add(val, 1, __ATOMIC_RELAXED) + 1;
    return 1;
}

static __inline__ int CRYPTO_DOWN_REF(int *val, int *ret, void * /*lock*/)
{
#  ifdef OSSL_TSAN_BUILD
    *ret = __atomic_fetch_sub(val, 1, __ATOMIC_ACQ_REL) - 1;
#  else
    *ret = __atomic_fetch_sub(val, 1, __ATOMIC_RELEASE) - 1;
    if (*ret == 0)
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
#  endif
    return 1;
}

# elif defined(_MSC_VER) && _MSC_VER>=1200

#  define HAVE_ATOMICS 1

typedef volatile int CRYPTO_REF_COUNT;

#  if (defined(_M_ARM) && _M_ARM>=7 && !defined(_WIN32_WCE)) || defined(_M_ARM64)
#   include <intrin.h>
#   if defined(_M_ARM64) && !defined(_ARM_BARRIER_ISH)
#    define _ARM_BARRIER_ISH _ARM64_BARRIER_ISH
#   endif

static __inline int CRYPTO_UP_REF(volatile int *val, int *ret, void *lock)
{
    *ret = _InterlockedExchangeAdd_nf(val, 1) + 1;
    return 1;
}

static __inline int CRYPTO_DOWN_REF(volatile int *val, int *ret, void *lock)
{
    *ret = _InterlockedExchangeAdd_nf(val, -1) - 1;
    if (*ret == 0)
        __dmb(_ARM_BARRIER_ISH);
    return 1;
}
#  else
#   if !defined(_WIN32_WCE)
#    pragma intrinsic(_InterlockedExchangeAdd)
#   else
#    if _WIN32_WCE >= 0x600
      extern long __cdecl _InterlockedExchangeAdd(long volatile*, long);
#    else
      /* under Windows CE we still have old-style Interlocked* functions */
      extern long __cdecl InterlockedExchangeAdd(long volatile*, long);
#     define _InterlockedExchangeAdd InterlockedExchangeAdd
#    endif
#   endif

static __inline int CRYPTO_UP_REF(volatile int *val, int *ret, void *lock)
{
    *ret = _InterlockedExchangeAdd(val, 1) + 1;
    return 1;
}

static __inline int CRYPTO_DOWN_REF(volatile int *val, int *ret, void *lock)
{
    *ret = _InterlockedExchangeAdd(val, -1) - 1;
    return 1;
}
#  endif

# else

typedef int CRYPTO_REF_COUNT;

# define CRYPTO_UP_REF(val, ret, lock) CRYPTO_atomic_add(val, 1, ret, lock)
# define CRYPTO_DOWN_REF(val, ret, lock) CRYPTO_atomic_add(val, -1, ret, lock)

# endif

# if !defined(NDEBUG) && !defined(OPENSSL_NO_STDIO)
#  define REF_ASSERT_ISNT(test) \
    (void)((test) ? (OPENSSL_die("refcount error", __FILE__, __LINE__), 1) : 0)
# else
#  define REF_ASSERT_ISNT(i)
# endif

# ifdef REF_PRINT
#  define REF_PRINT_COUNT(a, b) \
        fprintf(stderr, "%p:%4d:%s\n", b, b->references, a)
# else
#  define REF_PRINT_COUNT(a, b)
# endif

#endif
