/* Copyright (C) 2022-26 Advanced Micro Devices, Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _THRESHOLD_H_
#define _THRESHOLD_H_

#include "zen_cpu_info.h"
#include "almem_defs.h"

/*
 * Threshold computation macros
 *
 * These macros define the formulas for computing size thresholds where the
 * library switches between optimization strategies. Used by threshold.c
 * (library runtime) and detect_cpu_info.c (build-time test generation) to
 * keep thresholds in sync.
 *
 * Terminology:
 *   NT moves  - non-temporal load+store (memcpy, memmove)
 *   NT stores - non-temporal store-only  (memset)
 */

/*
 * Pre-Zen5 non-temporal moves threshold = 3/4 of L3 cache.
 * Above this size, memcpy/memmove switch to NT moves.
 */
#define COMPUTE_NT_MOV_THRESHOLD(l3_bytes) (((l3_bytes) >> 2) * 3)

/*
 * Zen5 non-temporal threshold = L3 cache.
 * Above this size, both NT moves (memcpy/memmove) and
 * NT stores (memset) are used.
 */
#define COMPUTE_NT_THRESHOLD_ZEN5(l3_bytes) (l3_bytes)

/*
 * rep-movs (ERMS) start threshold.
 * Below this size, memcpy/memmove use vector loads+stores; at/above it the
 * ERMS variants switch to rep-movsb. The crossover is micro-arch specific:
 *   Zen5/Zen6 - rep-movsb only wins for larger copies (tuned to 26KB).
 *   Pre-Zen5  - rep-movsb wins earlier (2KB).
 */
#define REPMOV_START_THRESHOLD_ZEN5 (26 * 1024)
#define REPMOV_START_THRESHOLD_DEFAULT (2 * 1024)

/*
 * Sentinel that disables the rep-movsb tier: set as the start threshold when
 * ERMS is unavailable so the runtime size check (size >= threshold) is never
 * satisfied for any valid copy length.
 */
#define REPMOV_DISABLED (UINT64_MAX)

/*
 * Zen5 aligned vector move threshold = L1D/2 + 2KB.
 * Below this size, memcpy/memmove use aligned vector loads+stores.
 * Above, they switch to rep-movs (ERMS).
 */
#define COMPUTE_ALIGNED_VEC_MOV_TH(l1d_bytes) (((l1d_bytes) >> 1) + 2048)

/*
 * Zen5 aligned vector store threshold = L1D.
 * Below this size, memset uses temporal aligned vector stores.
 * Above, it switches to rep-stores (ERMS).
 */
#define COMPUTE_ALIGNED_VEC_STORE_TH(l1d_bytes) (l1d_bytes)

#ifdef __cplusplus
extern "C" {
#endif

/* Instruction-specific thresholds. Declared hidden (matching their
 * HIDDEN_SYMBOL definitions in threshold.c) so intra-library references use
 * direct RIP-relative addressing instead of a GOT indirection. */
extern HIDDEN_SYMBOL uint64_t __repmov_start_threshold;
extern HIDDEN_SYMBOL uint64_t __repmov_stop_threshold;
extern HIDDEN_SYMBOL uint64_t __repstore_start_threshold;
extern HIDDEN_SYMBOL uint64_t __repstore_stop_threshold;
extern HIDDEN_SYMBOL uint64_t __nt_start_threshold;
extern HIDDEN_SYMBOL uint64_t __nt_stop_threshold;

#ifdef __cplusplus
}
#endif

#endif
