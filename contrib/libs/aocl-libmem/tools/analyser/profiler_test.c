/*
 * Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
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

/*
 * Comprehensive Profiler Correctness Test Suite
 *
 * Supports custom alignment for src/dst buffers via --align-src=SIZE / --align-dst=SIZE.
 * If alignment is not specified for a buffer, it is left unaligned (offset by 3 bytes).
 *
 * Supported functions (18 total):
 *   Distribution: memset memcpy mempcpy memmove memcmp memchr strncpy strncmp strncat strnlen
 *   Count-only:   strcpy strcmp strcat strlen strchr strrchr strstr strspn
 */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BUFFER_SIZE (2 * 1024 * 1024)
#define UNALIGNED_OFFSET 3
#define DEFAULT_OP_SIZE 4096

/*
 * Clamp snprintf() return value for safe use with write().
 * snprintf() may return >= buff_sz (output was truncated) or < 0 (error).
 * In either case the raw return value must NOT be used as the byte count
 * for write(), which would read past the buffer.
 */
#define SLEN(n, buff_sz) ((n) > 0 ? ((size_t) (n) < (buff_sz) ? (size_t) (n) : (buff_sz) - 1) : 0)

static char g_src_buffer[BUFFER_SIZE] __attribute__((aligned(64)));
static char g_dst_buffer[BUFFER_SIZE] __attribute__((aligned(64)));
static volatile void *g_result_ptr = NULL;
static volatile int g_result_int = 0;
static volatile size_t g_result_size = 0;
static volatile uint64_t g_call_counter = 0;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Parse alignment spec: "64B", "1KB", "4K", "2MB" etc. Returns bytes or 0 on error. */
static size_t parse_alignment_size(const char *spec)
{
    char *endptr;
    long v;
    if (!spec || !*spec)
        return 0;
    v = strtol(spec, &endptr, 10);
    if (v <= 0 || endptr == spec)
        return 0;
    if (*endptr == '\0' || ((*endptr == 'B' || *endptr == 'b') && !*(endptr + 1)))
    {
        /* bytes */
    }
    else if ((*endptr == 'K' || *endptr == 'k') &&
             (!*(endptr + 1) || ((*(endptr + 1) == 'B' || *(endptr + 1) == 'b') && !*(endptr + 2))))
    {
        v *= 1024;
    }
    else if ((*endptr == 'M' || *endptr == 'm') &&
             (!*(endptr + 1) || ((*(endptr + 1) == 'B' || *(endptr + 1) == 'b') && !*(endptr + 2))))
    {
        v *= 1024L * 1024L;
    }
    else
        return 0;
    if (v < 1 || (v & (v - 1)) != 0)
        return 0;
    if ((size_t) v < sizeof(void *))
        v = (long) sizeof(void *);
    return (size_t) v;
}

static char *alloc_aligned_buffer(size_t alignment, size_t size)
{
    void *p = NULL;
    if (posix_memalign(&p, alignment, size) != 0)
    {
        p = NULL;
    }
    return (char *) p;
}

static char *alloc_unaligned_buffer(size_t size, void **base_out)
{
    void *b = malloc(size + UNALIGNED_OFFSET + 64);
    if (!b)
    {
        *base_out = NULL;
        return NULL;
    }
    *base_out = b;
    return (char *) b + UNALIGNED_OFFSET;
}

static const char *alignment_str(size_t a, char *buf, size_t bs)
{
    if (a == 0)
        snprintf(buf, bs, "unaligned");
    else if (a >= 1024 * 1024 && a % (1024 * 1024) == 0)
        snprintf(buf, bs, "%zuMB", a / (1024 * 1024));
    else if (a >= 1024 && a % 1024 == 0)
        snprintf(buf, bs, "%zuKB", a / 1024);
    else
        snprintf(buf, bs, "%zuB", a);
    return buf;
}

/* Generic function dispatcher */
static void call_function(const char *fn, char *dst, char *src, size_t sz, int n)
{
    int i;
    if (!strcmp(fn, "memset"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = memset(dst, 0, sz);
    }
    else if (!strcmp(fn, "memcpy"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = memcpy(dst, src, sz);
    }
    else if (!strcmp(fn, "mempcpy"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = mempcpy(dst, src, sz);
    }
    else if (!strcmp(fn, "memmove"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = memmove(dst, src, sz);
    }
    else if (!strcmp(fn, "memcmp"))
    {
        for (i = 0; i < n; i++)
            g_result_int = memcmp(src, dst, sz);
    }
    else if (!strcmp(fn, "memchr"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = memchr(src, 'A', sz);
    }
    else if (!strcmp(fn, "strncpy"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = strncpy(dst, src, sz);
    }
    else if (!strcmp(fn, "strncmp"))
    {
        for (i = 0; i < n; i++)
            g_result_int = strncmp(src, dst, sz);
    }
    else if (!strcmp(fn, "strncat"))
    {
        for (i = 0; i < n; i++)
        {
            dst[0] = '\0';
            g_result_ptr = strncat(dst, src, sz > 10 ? 10 : sz);
        }
    }
    else if (!strcmp(fn, "strnlen"))
    {
        for (i = 0; i < n; i++)
            g_result_size = strnlen(src, sz);
    }
    else if (!strcmp(fn, "strcpy"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = strcpy(dst, src);
    }
    else if (!strcmp(fn, "strcmp"))
    {
        for (i = 0; i < n; i++)
            g_result_int = strcmp(src, dst);
    }
    else if (!strcmp(fn, "strcat"))
    {
        for (i = 0; i < n; i++)
        {
            dst[0] = '\0';
            g_result_ptr = strcat(dst, src);
        }
    }
    else if (!strcmp(fn, "strlen"))
    {
        for (i = 0; i < n; i++)
            g_result_size = strlen(src);
    }
    else if (!strcmp(fn, "strchr"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = strchr(src, 'A');
    }
    else if (!strcmp(fn, "strstr"))
    {
        for (i = 0; i < n; i++)
            g_result_ptr = strstr(src, src + (sz > 64 ? 64 : sz / 2));
    }
    else if (!strcmp(fn, "strspn"))
    {
        for (i = 0; i < n; i++)
            g_result_size = strspn(src, dst);
    }
}

typedef struct {
    const char *func_name;
    int calls;
    size_t op_size;
    char *src;
    char *dst;
    pthread_barrier_t *barrier;
} thread_args_t;

static void *thread_worker(void *arg)
{
    thread_args_t *a = (thread_args_t *) arg;
    pthread_barrier_wait(a->barrier);
    call_function(a->func_name, a->dst, a->src, a->op_size, a->calls);
    pthread_mutex_lock(&g_mutex);
    g_call_counter += a->calls;
    pthread_mutex_unlock(&g_mutex);
    return NULL;
}

static void init_buffers(char *src, char *dst, size_t sz, const char *fn)
{
    size_t i;
    /* Clamp sz to prevent out-of-bounds access */
    if (sz >= BUFFER_SIZE)
        sz = BUFFER_SIZE - 1;
    for (i = 0; i < sz + 512 && i < BUFFER_SIZE; i++)
    {
        src[i] = (char) ((i + 1) & 0x7F);
        dst[i] = 0;
    }
    if (sz > 0)
        src[sz - 1] = '\0';
    dst[0] = '\0';
    if (!strcmp(fn, "strstr") && sz > 104)
    {
        src[100] = 't';
        src[101] = 'e';
        src[102] = 's';
        src[103] = 't';
        src[104] = '\0';
    }
}

static void run_calls(const char *fn, char *dst, char *src, size_t sz, int calls, int threads)
{
    if (threads <= 1)
    {
        call_function(fn, dst, src, sz, calls);
    }
    else
    {
        int i;
        pthread_t *th = malloc(threads * sizeof(pthread_t));
        thread_args_t *wa = malloc(threads * sizeof(thread_args_t));
        pthread_barrier_t bar;
        char msg[128];
        int len;
        len = snprintf(msg, sizeof(msg), "NUM_THREADS=%d\n", threads);
        write(2, msg, SLEN(len, sizeof(msg)));
        len = snprintf(msg, sizeof(msg), "CALLS_PER_THREAD=%d\n", calls);
        write(2, msg, SLEN(len, sizeof(msg)));
        pthread_barrier_init(&bar, NULL, threads);
        g_call_counter = 0;
        for (i = 0; i < threads; i++)
        {
            wa[i].func_name = fn;
            wa[i].calls = calls;
            wa[i].op_size = sz;
            wa[i].src = src;
            wa[i].dst = dst;
            wa[i].barrier = &bar;
            pthread_create(&th[i], NULL, thread_worker, &wa[i]);
        }
        for (i = 0; i < threads; i++)
            pthread_join(th[i], NULL);
        pthread_barrier_destroy(&bar);
        free(th);
        free(wa);
    }
}

/* New: test with custom alignment for src/dst */
static void test_with_custom_alignment(const char *fn, int calls, size_t a_src, size_t a_dst, size_t sz, int threads)
{
    char msg[256];
    int len;
    char as[32], ad[32];
    void *sb = NULL, *db = NULL;
    char *sp = NULL, *dp = NULL;

    alignment_str(a_src, as, sizeof(as));
    alignment_str(a_dst, ad, sizeof(ad));

    if (a_src > 0)
        sp = alloc_aligned_buffer(a_src, sz + 1024);
    else
        sp = alloc_unaligned_buffer(sz + 1024, &sb);
    if (!sp)
        return;

    if (a_dst > 0)
        dp = alloc_aligned_buffer(a_dst, sz + 1024);
    else
        dp = alloc_unaligned_buffer(sz + 1024, &db);
    if (!dp)
    {
        if (a_src > 0)
            free(sp);
        else
            free(sb);
        return;
    }

    init_buffers(sp, dp, sz, fn);

    len = snprintf(msg, sizeof(msg), "EXPECTED_FUNCTION=%s\n", fn);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "EXPECTED_CALLS=%d\n", calls * threads);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "TEST_TYPE=custom_alignment\n");
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "ALIGN_SRC=%s\n", as);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "ALIGN_DST=%s\n", ad);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "ALIGN_SRC_BYTES=%zu\n", a_src);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "ALIGN_DST_BYTES=%zu\n", a_dst);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "OP_SIZE=%zu\n", sz);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "SRC_ADDR=%p\n", (void *) sp);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "DST_ADDR=%p\n", (void *) dp);
    write(2, msg, SLEN(len, sizeof(msg)));

    if (a_src > 0 && ((uintptr_t) sp % a_src) != 0)
    {
        len = snprintf(msg, sizeof(msg), "WARNING: src %p NOT aligned to %zu!\n", (void *) sp, a_src);
        write(2, msg, SLEN(len, sizeof(msg)));
    }
    if (a_dst > 0 && ((uintptr_t) dp % a_dst) != 0)
    {
        len = snprintf(msg, sizeof(msg), "WARNING: dst %p NOT aligned to %zu!\n", (void *) dp, a_dst);
        write(2, msg, SLEN(len, sizeof(msg)));
    }

    run_calls(fn, dp, sp, sz, calls, threads);
    usleep(100000);

    if (a_src > 0)
        free(sp);
    else
        free(sb);
    if (a_dst > 0)
        free(dp);
    else
        free(db);
}

/* Legacy: alignment test (64-byte aligned vs 3-byte offset) */
static void test_alignment(const char *fn, int ac, int uc)
{
    char msg[256];
    int len;
    char *su = g_src_buffer + UNALIGNED_OFFSET;
    char *du = g_dst_buffer + UNALIGNED_OFFSET;

    init_buffers(g_src_buffer, g_dst_buffer, DEFAULT_OP_SIZE, fn);

    len = snprintf(msg, sizeof(msg), "EXPECTED_FUNCTION=%s\n", fn);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "EXPECTED_CALLS=%d\n", ac + uc);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "TEST_TYPE=alignment\n");
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "EXPECTED_ALIGNED=%d\n", ac);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "EXPECTED_UNALIGNED=%d\n", uc);
    write(2, msg, SLEN(len, sizeof(msg)));

    call_function(fn, g_dst_buffer, g_src_buffer, DEFAULT_OP_SIZE, ac);

    init_buffers(su, du, DEFAULT_OP_SIZE, fn);
    call_function(fn, du, su, DEFAULT_OP_SIZE, uc);
    usleep(100000);
}

/* Legacy: basic call count / multithreaded test */
static void test_function(const char *fn, int calls, int threads)
{
    char msg[256];
    int len;

    init_buffers(g_src_buffer, g_dst_buffer, DEFAULT_OP_SIZE, fn);

    len = snprintf(msg, sizeof(msg), "EXPECTED_FUNCTION=%s\n", fn);
    write(2, msg, SLEN(len, sizeof(msg)));
    len = snprintf(msg, sizeof(msg), "EXPECTED_CALLS=%d\n", calls * threads);
    write(2, msg, SLEN(len, sizeof(msg)));
    if (threads > 1)
    {
        len = snprintf(msg, sizeof(msg), "TEST_TYPE=multithreaded\n");
        write(2, msg, SLEN(len, sizeof(msg)));
    }

    run_calls(fn, g_dst_buffer, g_src_buffer, DEFAULT_OP_SIZE, calls, threads);
    usleep(100000);
}

static void print_usage(void)
{
    const char u[] = "Usage: profiler_test <func> <calls> [options]\n"
                     "\n"
                     "Options:\n"
                     "  --align-src=SIZE  Align source buffer (e.g. 64B, 1KB, 4KB)\n"
                     "  --align-dst=SIZE  Align dest buffer (e.g. 64B, 1KB, 4KB)\n"
                     "  --threads=N       Number of threads (default: 1)\n"
                     "  --size=N          Operation size in bytes (default: 4096)\n"
                     "\n"
                     "If --align-src/--align-dst not specified, buffer is unaligned.\n"
                     "Alignment must be a power of 2. Suffixes: B, K/KB, M/MB.\n"
                     "\n"
                     "Legacy modes (backward compatible):\n"
                     "  <func> <calls> [threads]\n"
                     "  <func> <aligned_calls> 1 <unaligned_calls>\n"
                     "\n"
                     "Examples:\n"
                     "  profiler_test memcpy 1000 --align-src=4KB --align-dst=4KB\n"
                     "  profiler_test memcpy 1000 --align-src=64B\n"
                     "  profiler_test memset 1000 --align-dst=1KB\n"
                     "  profiler_test memcpy 500 --align-src=4KB --threads=4\n"
                     "  profiler_test memcpy 1000 --align-src=64B --size=8192\n"
                     "\n"
                     "Functions: memset memcpy mempcpy memmove memcmp memchr\n"
                     "           strncpy strncmp strncat strnlen\n"
                     "           strcpy strcmp strcat strlen strchr strstr strspn\n";
    write(2, u, sizeof(u) - 1);
}

int main(int argc, char **argv)
{
    const char *func_name = NULL;
    int num_calls = 0, num_threads = 1, has_new_opts = 0;
    size_t align_src = 0, align_dst = 0, op_size = DEFAULT_OP_SIZE;
    int pos_count = 0;
    char *pos_args[4] = {NULL, NULL, NULL, NULL};
    int i;

    if (argc < 3)
    {
        print_usage();
        return 1;
    }

    /* Parse arguments */
    for (i = 1; i < argc; i++)
    {
        if (strncmp(argv[i], "--align-src=", 12) == 0)
        {
            align_src = parse_alignment_size(argv[i] + 12);
            if (!align_src)
            {
                fprintf(stderr, "ERROR: Invalid --align-src='%s' (must be power-of-2, e.g. 64B, 4KB)\n", argv[i] + 12);
                return 1;
            }
            has_new_opts = 1;
        }
        else if (strncmp(argv[i], "--align-dst=", 12) == 0)
        {
            align_dst = parse_alignment_size(argv[i] + 12);
            if (!align_dst)
            {
                fprintf(stderr, "ERROR: Invalid --align-dst='%s' (must be power-of-2, e.g. 64B, 4KB)\n", argv[i] + 12);
                return 1;
            }
            has_new_opts = 1;
        }
        else if (strncmp(argv[i], "--threads=", 10) == 0)
        {
            num_threads = atoi(argv[i] + 10);
            if (num_threads < 1)
                num_threads = 1;
            has_new_opts = 1;
        }
        else if (strncmp(argv[i], "--size=", 7) == 0)
        {
            op_size = (size_t) atol(argv[i] + 7);
            if (!op_size)
                op_size = DEFAULT_OP_SIZE;
            if (op_size > BUFFER_SIZE - 1024)
            {
                fprintf(stderr, "ERROR: --size=%zu exceeds maximum (%d). Clamping.\n", op_size, BUFFER_SIZE - 1024);
                op_size = BUFFER_SIZE - 1024;
            }
            has_new_opts = 1;
        }
        else if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h"))
        {
            print_usage();
            return 0;
        }
        else
        {
            if (pos_count < 4)
                pos_args[pos_count] = argv[i];
            pos_count++;
        }
    }

    if (pos_count < 2)
    {
        print_usage();
        return 1;
    }
    func_name = pos_args[0];
    num_calls = atoi(pos_args[1]);

    if (has_new_opts)
    {
        /* New-style: use custom alignment mode */
        test_with_custom_alignment(func_name, num_calls, align_src, align_dst, op_size, num_threads);
    }
    else if (pos_count >= 4)
    {
        /* Legacy alignment mode: <func> <aligned_calls> 1 <unaligned_calls> */
        int unaligned_calls = atoi(pos_args[3]);
        test_alignment(func_name, num_calls, unaligned_calls);
    }
    else
    {
        /* Legacy basic mode: <func> <calls> [threads] */
        if (pos_count >= 3)
            num_threads = atoi(pos_args[2]);
        test_function(func_name, num_calls, num_threads);
    }

    return 0;
}
