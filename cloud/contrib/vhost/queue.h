/*
 * Lists and queues.
 *
 * Relevant OSes provide BSD-originated sys/queue.h, so just use it here, with
 * a few extensions.
 */

#pragma once

#include <sys/queue.h>

/*
 * Atomic extensions (based on QEMU qemu/queue.h) necessary for bottom halves
 * implementation
 */
#include "catomic.h"

#define SLIST_INSERT_HEAD_ATOMIC(head, elm, field)      ({                        \
    typeof(elm) old_slh_first;                                                    \
    do {                                                                          \
        old_slh_first = (elm)->field.sle_next = catomic_read(&(head)->slh_first); \
    } while (catomic_cmpxchg(&(head)->slh_first, old_slh_first, (elm)) !=         \
             old_slh_first);                                                      \
    old_slh_first;      })

#define SLIST_MOVE_ATOMIC(dest, src) do {                            \
    (dest)->slh_first = catomic_xchg(&(src)->slh_first, NULL);       \
} while (/*CONSTCOND*/0)

#define SLIST_FIRST_RCU(head)       catomic_rcu_read(&(head)->slh_first)
