/* Copyright (C) 2022-25 Advanced Micro Devices, Inc. All rights reserved.
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
#ifndef _ZEN_CPU_H
#define _ZEN_CPU_H
#include <stdbool.h>
#include <stdint.h>
#include "almem_defs.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
unsigned int eax;
unsigned int ebx;
unsigned int ecx;
unsigned int edx;
} cpuid_registers;

typedef struct{
uint64_t l1d_per_core;
uint64_t l2_per_core;
uint64_t l3_per_ccx;
uint64_t l3_per_ccd;
} cache_info;

typedef struct {
uint64_t repmov_start_threshold;
uint64_t repmov_stop_threshold;
uint64_t repstore_start_threshold;
uint64_t repstore_stop_threshold;
uint64_t nt_start_threshold;
uint64_t nt_stop_threshold;
} thresholds;

//x64 Register size in Bytes
#define WORD_SZ     2
#define DWORD_SZ    4   //Double Word
#define QWORD_SZ    8   //Quad Word
#define XMM_SZ      16
#define YMM_SZ      32
#define ZMM_SZ      64

/* ZEN CPU feature masks */
#define     AVX2_MASK       (1 <<  5)
#define     AVX512_MASK     (1 << 16)
#define     ERMS_MASK       (1 <<  9)
#define     FSRM_MASK       (1 <<  4)
#define     RDSEED_MASK     (1 << 18)
#define     RDPID_MASK      (1 << 22)
#define     VPCLMULQDQ_MASK (1 << 10)
#define     MOVDIRI_MASK    (1 << 27)
/* CPUID leaf 7, subleaf 0 (EAX=7, ECX=0): EDX bit 23 */
#define     AVX512_FP16_MASK    (1 << 23)

#define ZEN1_L3 8*1024*1024     //8MB per CCX
#define ZEN2_L3 16*1024*1024    //16MB per CCX
#define ZEN3_L3 32*1024*1024    //32MB per CCX
#define ZEN4_L3 32*1024*1024    //32MB per CCX
#define ZEN5_L3 32*1024*1024    //32MB per CCX
#define ZEN6_L3 64*1024*1024    //64MB per CCX

#define ZEN3_L2 512*1024        //512KB per CORE
#define ZEN4_L2 1*1024*1024     //1MB per CORE
#define ZEN5_L2 1*1024*1024     //1MB per CORE
#define ZEN6_L2 1*1024*1024     //1MB per CORE

#define ENABLED 1

typedef enum { ZEN1, ZEN2, ZEN3, ZEN4, ZEN5, ZEN6 } microarch;

typedef struct {
    unsigned int threads_per_core;
    unsigned int threads_per_ccx;
    unsigned int threads_per_ccd;
} thread_info;

typedef struct {
bool fsrm;
bool erms;
bool avx2;
bool avx512;
bool movdiri;
bool vpclmul;
bool rdpid;
bool rdseed;
bool avx512_fp16;
} cpu_features;

typedef struct {
bool avx2;
bool avx512;
bool erms;
} cpu_operation;

typedef struct {
    cpu_features zen_cpu_features;
    cache_info zen_cache_info;
    thread_info zen_thread_info;
    thresholds zen_thresholds;
} cpu_info;

typedef enum {
    u_align = 0,
    w_align,
    d_align,
    q_align,
    x_align,
    y_align,
    z_align,
    n_align //aligned for non-temporal
}alignment;


typedef struct{
    cpu_operation user_operation;
    thresholds user_threshold;
    alignment src_aln;
    alignment dst_aln;
} user_cfg;

typedef enum{
    UNK_CFG = 1,
    SYS_CFG,
    USR_CFG,
    CHK_CFG
} config;

HIDDEN_SYMBOL void parse_env_operation_cfg(void);
HIDDEN_SYMBOL void parse_env_threshold_cfg(void);

#ifdef __cplusplus
}
#endif

#endif
