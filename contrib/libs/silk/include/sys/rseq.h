/* Stub sys/rseq.h for ya build system.
 * The ya sysroot doesn't provide glibc's __rseq_offset, so we alias it
 * to librseq's rseq_offset which is populated by rseq_init(). */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern ptrdiff_t rseq_offset;

#ifdef __cplusplus
}
#endif

#define __rseq_offset rseq_offset

struct rseq {
    uint32_t cpu_id_start;
    uint32_t cpu_id;
    uint64_t rseq_cs;
    uint32_t flags;
    uint32_t node_id;
    uint32_t mm_cid;
} __attribute__((aligned(32)));
