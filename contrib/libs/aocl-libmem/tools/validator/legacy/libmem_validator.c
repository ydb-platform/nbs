/* Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.
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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <math.h>
#include <sys/mman.h>
#include <unistd.h>

#define CACHE_LINE_SZ           64
#define BOUNDARY_BYTES          8
#define PAGE_SZ                 4096
#define YMM_SZ                  32
#define ZMM_SZ                  64
#define NULL_TERM_CHAR         '\0'
#define NULL_STRING            ""
#define SINGLE_CHAR_STRING     ((uint8_t *)"A")
#define MIN_PRINTABLE_ASCII     32
#define MAX_PRINTABLE_ASCII     126
#define NULL_BYTE               1
#define LOWER_CHARS             26
#define SIZE_MIN                0

#define PAGE_CNT(X)             (X + NULL_BYTE + CACHE_LINE_SZ)/PAGE_SZ + \
                                !!((X + NULL_BYTE + CACHE_LINE_SZ) % PAGE_SZ)

#ifdef AVX512_FEATURE_ENABLED
    #define VEC_SZ ZMM_SZ
#else
    #define VEC_SZ YMM_SZ
#endif

#define NO_VECS(size)           (size / VEC_SZ + !!(size % VEC_SZ))

extern void *mempcpy(void *dest, const void *src, size_t n);

typedef struct
{
    const char *func_name;
    void  (*func)(size_t, uint32_t , uint32_t);
} libmem_func;

//uncomment the line below for verbosity of passed logs
//#define ALM_VERBOSE

#ifdef ALM_VERBOSE
#define ALM_VERBOSE_LOG(fmt, ...)    do {   \
                printf(fmt, ##__VA_ARGS__); \
                                } while(0);
#else
#define ALM_VERBOSE_LOG(fmt, ...) do {      \
                             } while (0)

#endif

#define OVERLAP_BUFFER        0
#define NON_OVERLAP_BUFFER    1
#define DEFAULT               2
#define NON_OVERLAP_BUFFER_EXTRA 3

#define implicit_func_decl_push_ignore \
    _Pragma("GCC diagnostic push") \
    _Pragma("GCC diagnostic ignored \"-Wimplicit-function-declaration\"") \

#define implicit_func_decl_pop \
    _Pragma("GCC diagnostic pop") \

/**
 * @brief Converts a memory page to inaccessible state for Page-cross testing
 *
 * This function uses mprotect() to set the specified page to PROT_NONE,
 * making it inaccessible for read/write operations. This creates a trap
 * that will cause a segmentation fault if any function attempts to access
 * memory beyond the intended boundary, helping detect buffer overruns
 * and other memory access violations in validator functions.
 *
 * @param page_buff Pointer to the page-aligned memory buffer
 * @param page_cnt Number of pages from the start of buffer to the target page
 * @return 0 on success, -1 on failure (with error message printed)
 */
static inline int convert_page_to_inaccessible(void *page_buff, uint32_t page_cnt)
{
    if (mprotect((uint8_t *)page_buff + page_cnt * PAGE_SZ, PAGE_SZ, PROT_NONE) != 0)
    {
        printf("ERROR:[MPROTECT] Failed to protect page at %p\n", (uint8_t *)page_buff + page_cnt * PAGE_SZ);
        return -1;
    }
    return 0;
}

/**
 * @brief Reverts a memory page back to accessible state before cleanup
 *
 * This function uses mprotect() to restore the specified page to PROT_READ|PROT_WRITE,
 * making it accessible again for normal memory operations. This is essential
 * before freeing memory that was previously protected, as attempting to free
 * protected memory can cause segmentation faults due to heap management
 * operations requiring access to the memory being freed.
 *
 * @param page_buff Pointer to the page-aligned memory buffer
 * @param page_cnt Number of pages from the start of buffer to the target page
 */
static inline void revert_page_to_accessible(void *page_buff, uint32_t page_cnt)
{
    if (mprotect((uint8_t *)page_buff + page_cnt * PAGE_SZ, PAGE_SZ, PROT_READ|PROT_WRITE) != 0)
    {
        printf("ERROR:[MPROTECT] Failed to restore page permissions at %p\n", (uint8_t *)page_buff + page_cnt * PAGE_SZ);
    }
}

/**
 * @brief Determines if a string is a good candidate for page-cross boundary testing
 *
 * This function calculates whether a string with given size and alignment
 * fits within a page after accounting for vector operations, making it
 * a good candidate for page-cross testing (does not cross last boundary vector).
 *
 * @param size Size of the string in bytes
 * @param alignment Alignment offset of the string
 * @return 1 if string is a page-cross candidate, 0 otherwise
 */
static inline int is_page_cross_candidate(size_t size, uint32_t alignment)
{
    size_t no_vecs = NO_VECS(size);
    size_t offset = (PAGE_SZ - no_vecs * VEC_SZ + alignment);
    return (offset + size) <= PAGE_SZ;
}

/**
 * @brief Allocates and protects a single page buffer for page-cross testing
 *
 * @param page_buff Pointer to store the allocated buffer address
 * @param page_cnt Number of pages to allocate (will add 1 extra for trap)
 * @param cleanup_buffer Buffer to free on error
 * @return 0 on success, function exits on failure
 */
static inline int alloc_page_cross_buffer(void **page_buff, uint32_t page_cnt, void *cleanup_buffer)
{
    if (posix_memalign(page_buff, PAGE_SZ, (page_cnt + 1) * PAGE_SZ) != 0)
    {
        printf("[ERROR] Failed to allocate memory for page-cross testing\n");
        free(cleanup_buffer);
        exit(-1);
    }
    if (convert_page_to_inaccessible(*page_buff, page_cnt) != 0)
    {
        free(*page_buff);
        free(cleanup_buffer);
        exit(-1);
    }
    return 0;
}

/**
 * @brief Cleans up page-cross buffer by restoring permissions and freeing memory
 *
 * This function restores the page permissions to accessible state and then
 * frees the page buffer. This consolidates the cleanup pattern used across
 * all page-cross tests.
 *
 * @param page_buff Pointer to the page buffer to clean up
 * @param page_cnt Number of pages from start to the protected page
 */
static inline void cleanup_page_cross_buffer(void *page_buff, uint32_t page_cnt)
{
    if (page_buff)
    {
        revert_page_to_accessible(page_buff, page_cnt);
        free(page_buff);
    }
}

/**
 * @brief Cleans up dual page buffers by restoring permissions and freeing memory
 *
 * @param page_buff1 First page buffer to clean up (can be NULL)
 * @param page_buff2 Second page buffer to clean up (can be NULL)
 * @param page_cnt Number of pages from start to the protected page
 */
static inline void cleanup_dual_page_cross_buffers(void *page_buff1, void *page_buff2, uint32_t page_cnt)
{
    cleanup_page_cross_buffer(page_buff1, page_cnt);
    cleanup_page_cross_buffer(page_buff2, page_cnt);
}

/**
 * @brief Calculate page-cross string address for different offset patterns
 *
 * @param page_buff Base page buffer address
 * @param page_cnt Page count
 * @param size String size
 * @param alignment String alignment
 * @param use_vector_calc Whether to use vector-based calculation (for strcmp/strncmp)
 * @return Calculated string address
 */
static inline uint8_t* calc_page_cross_address(void *page_buff, uint32_t page_cnt, size_t size,
                                             uint32_t alignment, int use_vector_calc)
{
    if (use_vector_calc)
    {
        // For strcmp/strncmp - vector-based calculation
        return (uint8_t *)page_buff + page_cnt * PAGE_SZ - (VEC_SZ * NO_VECS(size)) + alignment;
    }
    else
    {
        // For most string functions - basic calculation
        return (uint8_t *)page_buff + page_cnt * PAGE_SZ - (size + NULL_BYTE + alignment);
    }
}

/**
 * @brief Allocate and setup dual page buffers for page-cross testing
 *
 * @param str1_addr Pointer to store str1 address (can be NULL if not candidate)
 * @param str2_addr Pointer to store str2 address (can be NULL if not candidate)
 * @param str1_align Alignment for str1
 * @param str2_align Alignment for str2
 * @param size String size
 * @param cleanup_buf Buffer to cleanup on error
 * @param use_vector_calc Whether to use vector-based address calculation
 * @param page_buff1 Output: allocated page buffer 1 (or NULL)
 * @param page_buff2 Output: allocated page buffer 2 (or NULL)
 * @param page_cnt Output: calculated page count
 * @return 1 if any buffers were allocated, 0 otherwise
 */
static inline int setup_dual_page_cross_buffers(uint8_t **str1_addr, uint8_t **str2_addr,
                                               uint32_t str1_align, uint32_t str2_align,
                                               size_t size, void *cleanup_buf, int use_vector_calc,
                                               void **page_buff1, void **page_buff2, uint32_t *page_cnt)
{
    int str1_is_candidate = is_page_cross_candidate(size, str1_align);
    int str2_is_candidate = is_page_cross_candidate(size, str2_align);

    *page_buff1 = NULL;
    *page_buff2 = NULL;
    *page_cnt = PAGE_CNT(size);

    if (!(str1_is_candidate || str2_is_candidate))
        return 0;

    // Allocate str1 buffer if candidate
    if (str1_is_candidate)
    {
        alloc_page_cross_buffer(page_buff1, *page_cnt, cleanup_buf);
        *str1_addr = calc_page_cross_address(*page_buff1, *page_cnt, size, str1_align, use_vector_calc);
    }

    // Allocate str2 buffer if candidate
    if (str2_is_candidate)
    {
        if (posix_memalign(page_buff2, PAGE_SZ, (*page_cnt + 1) * PAGE_SZ) != 0)
        {
            printf("[ERROR] Failed to allocate memory for str2 page-cross\n");
            if (*page_buff1)
                cleanup_page_cross_buffer(*page_buff1, *page_cnt);
            free(cleanup_buf);
            exit(-1);
        }
        if (convert_page_to_inaccessible(*page_buff2, *page_cnt) != 0)
        {
            if (*page_buff1)
                cleanup_page_cross_buffer(*page_buff1, *page_cnt);
            free(*page_buff2);
            free(cleanup_buf);
            exit(-1);
        }
        *str2_addr = calc_page_cross_address(*page_buff2, *page_cnt, size, str2_align, use_vector_calc);
    }

    return 1;
}

/**
 * @brief Setup single page buffer for page-cross testing
 *
 * @param str_addr Pointer to store string address
 * @param str_align String alignment
 * @param size String size
 * @param cleanup_buf Buffer to cleanup on error
 * @param use_vector_calc Whether to use vector-based address calculation
 * @param page_buff Output: allocated page buffer (or NULL)
 * @param page_cnt Output: calculated page count
 * @return 1 if buffer was allocated, 0 otherwise
 */
static inline int setup_single_page_cross_buffer(uint8_t **str_addr, uint32_t str_align,
                                                size_t size, void *cleanup_buf, int use_vector_calc,
                                                void **page_buff, uint32_t *page_cnt)
{
    if (!is_page_cross_candidate(size, str_align))
    {
        *page_buff = NULL;
        return 0;
    }

    *page_cnt = PAGE_CNT(size);
    alloc_page_cross_buffer(page_buff, *page_cnt, cleanup_buf);
    *str_addr = calc_page_cross_address(*page_buff, *page_cnt, size, str_align, use_vector_calc);

    return 1;
}

typedef uint8_t alloc_mode;

//Function for verifying string functions
//Passing size = -1 for strcmp (Until Null is found)
int string_cmp(const char *str1, const char *str2, size_t size)
{
    size_t i;
    for (i = 0; (str1[i] != NULL_TERM_CHAR) && (str2[i] != NULL_TERM_CHAR) && (str2[i] == str1[i]) && (i < (size -1)); i++);
    return (unsigned char)str1[i] - (unsigned char)str2[i];
}

char *test_memchr( const char *src, int ch, size_t len)
{
    size_t n;
    for(n=0;n<len;n++)
        if(src[n] == ch)
            return (void*)&src[n];
    return NULL;
}

uint8_t *test_memcpy(uint8_t *dst, const uint8_t *src, size_t n)
{
    void *val = dst;
    while(n--)
        *dst++ = *src++;
    return val;
}

int test_memcmp(const uint8_t *s1, const uint8_t *s2, size_t n)
{
    for(size_t i = 0; i < n; i++)
        if(s1[i] != s2[i])
            return (int)s1[i] - (int)s2[i];
    return 0;
}

char *test_strcat_common(char *dst, const char *src, size_t n) {
    char *ret = dst;
    // Find the end of the destination string
    while (*dst != NULL_TERM_CHAR)
        dst++;
    // Concatenate src to the end of dst
    if (n == 0)
    {
        // Behave like strcat
        while ((*dst++ = *src++) != NULL_TERM_CHAR);
    }
    else
    {
        // Behave like strncat
        while (n-- && *src != NULL_TERM_CHAR)
        {
            *dst++ = *src++;
        }
        *dst = NULL_TERM_CHAR; // Null-terminate the result
    }
    return ret;
}

char *test_strncat(char *dst, const char *src, size_t n) {
    char *ret = dst;
    // Find the end of the destination string
    while (*dst != NULL_TERM_CHAR)
        dst++;
    // Concatenate src to the end of dst (strncat behavior)
    while (n-- && *src != NULL_TERM_CHAR)
    {
        *dst++ = *src++;
    }
    *dst = NULL_TERM_CHAR; // Null-terminate the result
    return ret;
}

char *test_strcpy(char *dst, const char *src)
{
    char* temp = dst;
    while((*dst++ = *src++) != '\0'){
    }
    return temp;

}
int test_strcmp(const char* str1, const char* str2) {
    while (*str1 && (*str1 == *str2)) {
        str1++;
        str2++;
    }
    return *(unsigned char*)str1 - *(unsigned char*)str2;
}

char *test_strstr(const char *str1, const char *str2)
{
    if (*str2 == NULL_TERM_CHAR) {
        return (char*)str1;
    }
    const char *p1 = str1;
    const char *p2 = str2;
    while (*p1 != NULL_TERM_CHAR) {
        if (*p1 == *p2) {
            const char *q1 = p1;
            const char *q2 = p2;
            while (*q1 != NULL_TERM_CHAR && *q2 != NULL_TERM_CHAR && *q1 == *q2) {
                ++q1;
                ++q2;
            }
            if (*q2 == NULL_TERM_CHAR) {
                return (char*)p1;
            }
        }
        ++p1;
    }
    return NULL;
}

size_t test_strspn(const char *str1, const char *str2) {
    size_t count = 0;
    while (*str1) {
        int found = 0;
        for (const char* p = str2; *p; ++p) {
            if (*p == *str1) {
                found = 1;
                break;
            }
        }
        if (!found) {
            break;
        }
        count++;
        str1++;
    }

    return count;
}

char *test_strchr(const char *s, int c) {
    while (*s != (char)c) {
        if (*s == '\0') {
            return NULL;
        }
        s++;
    }
    return (char *)s;
}

char *test_strrchr(const char *s, int c) {
    const char *last = NULL;

    do {
        if ((unsigned char)*s == (unsigned char)c) {
            last = s;
        }
    } while (*s++ != '\0');

    return (char *)last;
}

static uint8_t * alloc_buffer(uint8_t **head_buff, uint8_t **tail_buff,\
                                         size_t size, alloc_mode mode)
{
    void *buff_addr = NULL;
    int ret;
    switch (mode)
    {
        case OVERLAP_BUFFER:
            ret = posix_memalign(&buff_addr, CACHE_LINE_SZ, \
                            2 * (size + 2 * CACHE_LINE_SZ));
            if(ret != 0)
                return NULL;  // posix_memalign failed

            * head_buff = (uint8_t *)buff_addr;
            * tail_buff = (uint8_t *)(((uint64_t)buff_addr +\
             CACHE_LINE_SZ + rand()%(size + 1)) & ~(CACHE_LINE_SZ - 1));
            break;
        case NON_OVERLAP_BUFFER:
            ret = posix_memalign(&buff_addr, CACHE_LINE_SZ, \
                            2 * (size + 2 * CACHE_LINE_SZ));

            if(ret != 0)
                return NULL;  // posix_memalign failed

            * head_buff = (uint8_t *)buff_addr;
            * tail_buff = (uint8_t *)(((uint64_t)buff_addr +\
                     size + 2*CACHE_LINE_SZ) & ~(CACHE_LINE_SZ - 1));
            break;

        case NON_OVERLAP_BUFFER_EXTRA:
            ret = posix_memalign(&buff_addr, CACHE_LINE_SZ, \
                            3 * (size + 2 * CACHE_LINE_SZ));

            if(ret != 0)
                return NULL;  // posix_memalign failed

            * head_buff = (uint8_t *)buff_addr;
            * tail_buff = (uint8_t *)(((uint64_t)buff_addr +\
                     2*size + 2*CACHE_LINE_SZ) & ~(CACHE_LINE_SZ - 1));
            break;

        default:
            ret = posix_memalign(&buff_addr, CACHE_LINE_SZ, size + 2 * CACHE_LINE_SZ);

            if(ret != 0)
                return NULL;  // posix_memalign failed

            * tail_buff = (uint8_t *)buff_addr;
            * head_buff = NULL;
    }
    return (uint8_t *)buff_addr;
}

static inline void prepare_boundary(uint8_t *dst, size_t size)
{
    for (size_t index = 1; index <= BOUNDARY_BYTES; index++)
    {
        *(dst - index) = '#';
        *(dst + size + index - 1) = '#';
    }
}

static inline int boundary_check(uint8_t *dst, size_t size)
{
    for (size_t index = 1; index <= BOUNDARY_BYTES; index++)
    {
        if (*(dst - index) != '#')
        {
            printf("ERROR:[BOUNDARY] Out of bound Data corruption @pre_index:%ld for size: %ld ", index, size);
            return -1;
        }
        if (*(dst + size + index - 1) != '#')
        {
            printf("ERROR:[BOUNDARY] Out of bound Data corruption @post_index:%ld for size: %ld ", index, size);
            return -1;
        }
    }
    return 0;
}

void generate_random_string(uint8_t *buf, size_t length)
{
    for (size_t i = 0; i < length; i++) {
        buf[i] = MIN_PRINTABLE_ASCII + (rand() % (MAX_PRINTABLE_ASCII - MIN_PRINTABLE_ASCII));
    }

    // Null-terminate the string
    buf[length] = NULL_TERM_CHAR;

}

// Generate a printable character
char random_char()
{
    return ' ' + rand() % ('~' - ' ' + 1);
}

void string_setup(char *haystack, size_t size, char *needle, size_t needle_len)
{
    //Substrings of needle excluding the needle itself
    haystack[0] = NULL_TERM_CHAR;

    for (size_t i = 0; i < needle_len; i++) {
        test_strncat(haystack, needle, i);
    }

    uint32_t hay_index = (needle_len - 1)*(needle_len)/2;
    //Randomly copying chars from needle
    while (hay_index < size) {
        int index = rand() % needle_len;
        haystack[hay_index++] =  needle[index];
    }
    haystack[size] = NULL_TERM_CHAR;
}

static inline void init_buffer(char* src, size_t size)
{
    size_t index = 0;
    for (index = 0; index < size; index++)
    {
        *(src + index) = ((char) ((rand() % 92) +36));//[ASCII value 36 ($) -> 127 (DEL)]
    }
    //Random needle char at index, index<size/2, index> size/2
    index = rand()%size;
    *(src + index) = (int)'!';
    if (size != 1)
    {
        int pos = size/2;
        index = rand()%pos;
        *(src + index) = (int)'!';
        index = rand()%(size - pos +1) +pos;
        index = rand()%pos;
        *(src + index) = (int)'!';
    }
    *(src+ size -1) = NULL_TERM_CHAR;

}
static inline void memcpy_validator(size_t size, uint32_t dst_alnmnt,\
                                                 uint32_t src_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *dst_alnd_addr = NULL, *src_alnd_addr = NULL;
    size_t index;
    void *ret = NULL;

    //special case to handle size ZERO with NULL buff.
    if (size == 0)
    {
        ret = memcpy(dst_alnd_addr, src_alnd_addr, size);
        if (ret != NULL)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - %p"\
                        ", actual - %p\n", size, dst_alnd_addr, ret);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    dst_alnd_addr = buff_tail + dst_alnmnt;
    src_alnd_addr = buff_head + src_alnmnt;

    prepare_boundary(dst_alnd_addr, size);

    //intialize src memory
    for (index = 0; index < size; index++)
        *(src_alnd_addr +index) = 'a' + rand()%LOWER_CHARS;
    ret = memcpy(dst_alnd_addr, src_alnd_addr, size);

    //validation of dst memory
    for (index = 0; (index < size) && (*(dst_alnd_addr + index) == \
                            *(src_alnd_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] failed for size: %lu @index:%lu" \
                    "[src: %p(alignment = %u), dst:%p(alignment = %u)]\n", \
                 size, index, src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);
    else
        ALM_VERBOSE_LOG("Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != dst_alnd_addr)
        printf("ERROR:[RETURN] value mismatch: expected - %p, actual - %p\n", dst_alnd_addr, ret);

    if (boundary_check(dst_alnd_addr, size))
        printf("[src: %p(alignment = %u), dst:%p(alignment = %u)]\n",src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);


    free(buff);
}

static inline void mempcpy_validator(size_t size, uint32_t dst_alnmnt,\
                                                 uint32_t src_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *dst_alnd_addr = NULL, *src_alnd_addr = NULL;
    size_t index;
    void *ret = NULL;

    //special case to handle size ZERO with NULL buffer inputs.
    if (size == 0)
    {
        implicit_func_decl_push_ignore
        ret = mempcpy(buff, buff, size);
        implicit_func_decl_pop
        if (ret != buff)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - %p"\
                        ", actual - %p\n", size, buff, ret);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }

    dst_alnd_addr = buff_tail + dst_alnmnt;
    src_alnd_addr = buff_head + src_alnmnt;

    prepare_boundary(dst_alnd_addr, size);

    //intialize src memory
    for (index = 0; index < size; index++)
        *(src_alnd_addr +index) = 'a' + rand()%LOWER_CHARS;

    implicit_func_decl_push_ignore
    ret = mempcpy(dst_alnd_addr, src_alnd_addr, size);
    implicit_func_decl_pop

    //validation of dst memory
    for (index = 0; (index < size) && (*(dst_alnd_addr + index) == \
                            *(src_alnd_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] failed for size: %lu @index:%lu "\
                             "[src: %p(alignment = %u), dst:%p(alignment = %u)]\n",\
                 size, index, src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);
    else
        ALM_VERBOSE_LOG("Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != (dst_alnd_addr + size))
        printf("ERROR:[RETURN] value mismatch: expected - %p, actual - %p\n",\
                                                 dst_alnd_addr + size, ret);

    if (boundary_check(dst_alnd_addr, size))
        printf("[src: %p(alignment = %u), dst:%p(alignment = %u)]\n",src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);


    free(buff);
}


static inline void memmove_validator(size_t size, uint32_t dst_alnmnt, uint32_t src_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail, *validation_buff, *temp;
    uint8_t *dst_alnd_addr = NULL, *src_alnd_addr = NULL, *validation_addr = NULL;
    size_t index;
    void *ret = NULL;

    //special case to handle size ZERO with NULL buffer inputs.
    if (size == 0)
    {
        ret = memmove(buff, buff, size);
        if (ret != buff)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - %p"\
                        ", actual - %p\n", size, buff, ret);
        return;
    }

    // Overlapping Memory Validation
    buff = alloc_buffer(&buff_head, &buff_tail, size, OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }

    validation_buff = alloc_buffer(&temp, &validation_addr, size, DEFAULT);
    if (validation_buff == NULL)
    {
        printf("[ERROR] Failed to allocate validation buffer\n");
        free(buff);
        exit(-1);
    }

    //Forward Validation
    src_alnd_addr = buff_tail + src_alnmnt;
    dst_alnd_addr = buff_head + dst_alnmnt;

    //intialize src memory
    for (index = 0; index < size; index++)
        *(validation_addr + index) = *(src_alnd_addr + index) = 'a' + rand()%LOWER_CHARS;

    ret = memmove(dst_alnd_addr, src_alnd_addr, size);
    //validation of dst memory
    for (index = 0; (index < size) && (*(dst_alnd_addr + index) == \
                            *(validation_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] Forward failed for size: %lu @index:%lu "\
                             "[src: %p(alignment = %u), dst:%p(alignment = %u)]\n",\
                 size, index, src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);
    else
        ALM_VERBOSE_LOG("Forward Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != dst_alnd_addr)
        printf("ERROR:[RETURN] Forward value mismatch: expected - %p, actual - %p\n",\
                                                         dst_alnd_addr, ret);

    //Backward Validation
    src_alnd_addr = buff_head + src_alnmnt;
    dst_alnd_addr = buff_tail + dst_alnmnt;

    //intialize src memory
    for (index = 0; index < size; index++)
        *(validation_addr + index) = *(src_alnd_addr + index) = 'a' + rand()%LOWER_CHARS;

    ret = memmove(dst_alnd_addr, src_alnd_addr, size);
    //validation of dst memory
    for (index = 0; (index < size) && (*(dst_alnd_addr + index) == \
                            *(validation_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] Backward failed for size: %lu @index:%lu "\
                             "[src: %p(alignment = %u), dst:%p(alignment = %u)]\n",\
                 size, index, src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);
    else
        ALM_VERBOSE_LOG("Backward Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != dst_alnd_addr)
        printf("ERROR:[RETURN] Backward value mismatch: expected - %p, actual - %p\n",\
                                                         dst_alnd_addr, ret);

    free(buff);
    free(validation_buff);
    buff = NULL;
    // Non Over-lapping memory validation
    buff = alloc_buffer(&buff_head, &buff_tail, size + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    dst_alnd_addr = buff_tail + dst_alnmnt;
    src_alnd_addr = buff_head + src_alnmnt;

    prepare_boundary(dst_alnd_addr, size);

    //intialize src memory
    for (index = 0; index < size; index++)
        *(src_alnd_addr +index) = 'a' + rand()%LOWER_CHARS;
    ret = memmove(dst_alnd_addr, src_alnd_addr, size);

    //validation of dst memory
    for (index = 0; (index < size) && (*(dst_alnd_addr + index) == \
                            *(src_alnd_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] Non-Overlap failed for size: %lu"\
            " @index:%lu [src: %p(alignment = %u), dst:%p(alignment = %u)]\n",\
            size, index, src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);
    else
        ALM_VERBOSE_LOG("Non-Overlapping Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != dst_alnd_addr)
        printf("ERROR:[RETURN] Non-Overlap value mismatch: expected - %p,"\
                                        " actual - %p\n", dst_alnd_addr, ret);

    if (boundary_check(dst_alnd_addr, size))
        printf("[src: %p(alignment = %u), dst:%p(alignment = %u)]\n",src_alnd_addr, src_alnmnt, dst_alnd_addr, dst_alnmnt);


    free(buff);
}

static inline void memset_validator(size_t size, uint32_t dst_alnmnt,\
                                                    uint32_t src_alnmnt __attribute__((unused)))
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *dst_alnd_addr = NULL;
    int value = 0;
    size_t index;
    void *ret = NULL;
    //special case to handle size ZERO with NULL buff inputs.
    if (size == 0)
    {
        ret = memset(buff, value, size);
        if (ret != buff)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - %p"\
                        ", actual - %p\n", size, buff, ret);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + 2 * BOUNDARY_BYTES, DEFAULT);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    //Adding offset of CACHE_LINE_SZ to fit in Boundary bytes
    dst_alnd_addr = buff_tail + dst_alnmnt + CACHE_LINE_SZ;

    prepare_boundary(dst_alnd_addr, size);
    //choose value to set
    value = rand()%256;

    ret = memset(dst_alnd_addr, value, size);
    //validation of dst memory
    for (index = 0; (index < size) && (*(dst_alnd_addr + index) == value);\
                                                     index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] failed for size: %lu @index:%lu "\
                             "[dst:%p(alignment = %u)]\n",\
                 size, index, dst_alnd_addr, dst_alnmnt);
    else
        ALM_VERBOSE_LOG("Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != dst_alnd_addr)
        printf("ERROR:[RETURN] value mismatch: expected - %p, actual - %p\n",\
                                                         dst_alnd_addr, ret);

    if (boundary_check(dst_alnd_addr, size))
        printf("[dst:%p(alignment = %u)]\n",dst_alnd_addr, dst_alnmnt);


    free(buff);
}

static inline void memcmp_validator(size_t size, uint32_t mem2_alnmnt,\
                                                 uint32_t mem1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *mem2_alnd_addr = NULL, *mem1_alnd_addr = NULL;
    size_t index;
    int ret, exp_ret = 0, validation_passed = 1;

    //special case to handle size ZERO with NULL buff inputs.
    if (size == 0)
    {
        ret = memcmp(buff, buff, size);
        if (ret != 0)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - 0"\
                        ", actual - %d\n", size, ret);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }

    mem2_alnd_addr = buff_tail + mem2_alnmnt;
    mem1_alnd_addr = buff_head + mem1_alnmnt;

    //intialize mem1 and mem2 buffers
    for (index = 0; index < size; index++)
        *(mem2_alnd_addr + index) =  *(mem1_alnd_addr +index) = 'a' + rand()%LOWER_CHARS;

    ret = memcmp(mem2_alnd_addr , mem1_alnd_addr, size);

    if (ret != exp_ret)
    {
        printf("ERROR:[VALIDATION] failed for matching data of size: %lu,"\
                                    " return_value = %d\n", size, ret);
    }
    else
    {
        ALM_VERBOSE_LOG("Validation passed for matching memory of size: %lu\n", size);
    }

    // Validation of Byte by BYte mismtach
    for (index = 0; index < size; index++)
    {
       //set a byte of source different from destination
       while(*(mem1_alnd_addr + index) == *(mem2_alnd_addr + index))
            *(mem1_alnd_addr + index) = (char)rand();
       //set one more byte from the end
       *(mem1_alnd_addr + size - 1) = '$';
       ret = memcmp(mem1_alnd_addr , mem2_alnd_addr, size);
       exp_ret = (*(uint8_t *)(mem1_alnd_addr + index) - \
                        *(uint8_t *)(mem2_alnd_addr + index));
       if (ret != exp_ret)
       {
           printf("ERROR:[VALIDATION] Non-Matching failed for string of "\
                    "size: %lu(index = %lu), return_value [actual= %d,"\
                                " expected = %d]\n", size, index, ret, exp_ret);
           validation_passed = 0;
       }
       *(mem1_alnd_addr + index) = *(mem2_alnd_addr + index);
    }
    if (validation_passed)
         ALM_VERBOSE_LOG("Validation successfull for non-matching data of size: %lu\n",\
                                                                          size);

}

static inline void strcpy_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str2_alnd_addr = NULL, *str1_alnd_addr = NULL;
    size_t index;
    void *ret = NULL;

    //special case to handle size ZERO with NULL buff.
    if (size == 0)
    {
      char dst[]="?";// size 2 dst with ?\0
      strcpy(dst,(char*)NULL_STRING);
      if (*dst != NULL_TERM_CHAR)
         printf("ERROR:[Validation] for size(%lu): expected - %p"\
                        ", actual - %p\n", size, NULL_STRING, dst);
      return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str2_alnd_addr = buff_tail + str2_alnmnt;
    str1_alnd_addr = buff_head + str1_alnmnt;

    prepare_boundary(str2_alnd_addr, size);

    //intialize str1 memory
    for (index = 0; index < size; index++)
    {
        *(str1_alnd_addr + index) = 'a' + (char)(rand() % LOWER_CHARS);
    }
    // Appending Null character at the end of str1 string
    *(str1_alnd_addr + size - 1) = NULL_TERM_CHAR;
    ret = strcpy((char *)str2_alnd_addr, (char *)str1_alnd_addr);

    //validation of str2 memory
    for (index = 0; (index < size) && (*(str2_alnd_addr + index) == \
                            *(str1_alnd_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] failed for size: %lu @index:%lu " \
                    "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
              size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);
    else
        ALM_VERBOSE_LOG("Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != str2_alnd_addr)
        printf("ERROR:[RETURN] value mismatch: expected - %p, actual - %p\n", \
                                                             str2_alnd_addr, ret);

    //Multi-Null check
    size_t more_null_idx = rand() % (size);
    *(str1_alnd_addr + more_null_idx) = NULL_TERM_CHAR;

    ret = strcpy((char *)str2_alnd_addr, (char *)str1_alnd_addr);
    for (index = 0; (index <= more_null_idx) && (*(str2_alnd_addr + index) == \
                            *(str1_alnd_addr + index)); index ++);

    if (index != (more_null_idx + 1))
        printf("ERROR:[VALIDATION] Multi-NULL failed for size: %lu @index:%lu" \
                    "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
              size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);
    else
        ALM_VERBOSE_LOG("Multi-NULL check Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != str2_alnd_addr)
        printf("ERROR: [RETURN] Multi-NULL value mismatch: expected - %p, actual - %p\n", \
                                                             str2_alnd_addr, ret);

    if (boundary_check(str2_alnd_addr, size))
        printf("[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n",str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);

    // Robust page-cross check: Only for good candidates (does NOT cross last boundary vector)
    if (is_page_cross_candidate(size, str1_alnmnt)) {
        void *page_buff = NULL;
        uint32_t page_cnt = PAGE_CNT(size);

        alloc_page_cross_buffer(&page_buff, page_cnt, buff);

        str1_alnd_addr = (uint8_t *)page_buff + page_cnt * PAGE_SZ - (size + NULL_BYTE + str1_alnmnt);
        for (index = 0; index < size; index++) {
            *(str1_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
        }
        *(str1_alnd_addr + size - 1) = NULL_TERM_CHAR;
        ret = strcpy((char *)str2_alnd_addr, (char *)str1_alnd_addr);
        for (index = 0; (index < size) && (*(str2_alnd_addr + index) == \
                                *(str1_alnd_addr + index)); index ++);
        if (index != (size))
            printf("ERROR:[PAGE-CROSS] validation failed for size: %lu @index:%lu" \
                        "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
                  size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);
        else
            ALM_VERBOSE_LOG("Page-cross validation passed for size: %lu\n", size);
        //validation of return value
        if (ret != str2_alnd_addr)
            printf("ERROR:[PAGE-CROSS] Return value mismatch: expected - %p, actual - %p\n", \
                                                                 str2_alnd_addr, ret);

        cleanup_page_cross_buffer(page_buff, page_cnt);
    }
    free(buff);
}

static inline void strncpy_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str2_alnd_addr = NULL, *str1_alnd_addr = NULL;
    size_t index, str1_len;
    void *ret = NULL;

    //special case to handle size ZERO with NULL buff inputs.
    if (size == 0)
    {
        ret = strncpy((char*)str2_alnd_addr, (char*)str1_alnd_addr, size);
        if (ret != buff)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - %p"\
                        ", actual - %p\n", size, str2_alnd_addr, ret);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str2_alnd_addr = buff_tail + str2_alnmnt;
    str1_alnd_addr = buff_head + str1_alnmnt;

    prepare_boundary(str2_alnd_addr, size);

    for (index = 0; index <= size; index++)
    {
        *(str1_alnd_addr + index) = 'a' + (char)(rand() % LOWER_CHARS);
    }
    for(index = size; index <= size + BOUNDARY_BYTES; index++)
        *(str2_alnd_addr + index) = '#';

    //CASE 1:validation when NULL terminating char is beyond strlen
    ret = strncpy((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);
    //validation of str2 memory
    for (index = 0; (index < size) && (*(str2_alnd_addr + index) == \
                            *(str1_alnd_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] (strlen > n) failed for size: %lu @index:%lu" \
                    "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
              size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);
    else
        ALM_VERBOSE_LOG("[strlen > n] Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != str2_alnd_addr)
        printf("ERROR:[RETURN] (strlen > n) value mismatch: expected - %p, actual - %p\n",
                                                             str2_alnd_addr, ret);


    //Check if the str2 buffer was modified after 'n bytes' copy
    if (boundary_check(str2_alnd_addr, size))
        printf("[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n",str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);

    // CASE 2:validation when index of NULL char is equal to strlen
    // Appending Null character at the end of str1 string
    *(str1_alnd_addr + size - 1) = NULL_TERM_CHAR;

    //Passing the size including the NULL char(size+1)
    ret = strncpy((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);

    //validation of str2 memory
    for (index = 0; (index < size) && (*(str2_alnd_addr + index) == \
                            *(str1_alnd_addr + index)); index ++);

    if (index != size)
        printf("ERROR:[VALIDATION] (strlen = n) failed for size: %lu @index:%lu" \
                    "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
              size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);
    else
        ALM_VERBOSE_LOG("[strlen = n] Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != str2_alnd_addr)
        printf("ERROR:[RETURN] (strlen = n) Return value mismatch: expected - %p, actual - %p\n", \
                                                             str2_alnd_addr, ret);
    //Check if the str2 buffer was modified after 'n bytes' copy
    if (boundary_check(str2_alnd_addr, size))
        printf("[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n",str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);

    //CASE 3:validation when str1 length is less than that of 'n'(no.of bytes to be copied)
    //Generating random str1 buffer of size less than n

    //Multi-Null check
    size_t null_idx = rand() % (size);
    size_t more_null_idx = rand() % (size - null_idx);
    str1_len = null_idx;

    if (str1_len > 0)
        str1_len--;

    *(str1_alnd_addr + str1_len) = NULL_TERM_CHAR;
    *(str1_alnd_addr + str1_len + more_null_idx) = NULL_TERM_CHAR;

    ret = strncpy((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);
    //validation of str2 memory
    for (index = 0; (index <= str1_len) && (*(str2_alnd_addr + index) == \
                            *(str1_alnd_addr + index)); index ++);

    if (index != (str1_len + 1))
        printf("ERROR:[VALIDATION] (strlen < n) failed for size: %lu @index:%lu" \
                    "[str1: %p(alignment = %u), str2:%p(alignment = %u)] (strlen = %lu)\n", \
              size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt, str1_len);
    //Checking for NULL after str1_len in str2 buffer
    for (;index < size; index++)
    {
        if (str2_alnd_addr[index] != NULL_TERM_CHAR)
        {
            printf("ERROR:[VALIDATION] (strlen < n) NULL Validation failed at index:%lu" \
                        " for size: %ld(strlen = %ld)\n", index, size, str1_len);
            break;
        }
    }
    if (index == size)
        ALM_VERBOSE_LOG("[strlen < n] Data Validation passed for size: %lu\n", size);
    //validation of return value
    if (ret != str2_alnd_addr)
        printf("ERROR:[RETURN] (strlen < n) value mismatch: expected - %p, actual - %p\n", \
                                                             str2_alnd_addr, ret);

    //Check if the str2 buffer was modified after 'n bytes' copy
    if (boundary_check(str2_alnd_addr, size))
        printf("[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n",str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);

    // Robust page-cross check: Only for good candidates (does NOT cross last boundary vector)
    void *page_buff;
    uint32_t page_cnt;
    if (setup_single_page_cross_buffer(&str1_alnd_addr, str1_alnmnt, size, buff, 0, &page_buff, &page_cnt))
    {
        for (index = 0; index < size; index++) {
            *(str1_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
        }

        //case1: Null at the size
        *(str1_alnd_addr + size) = NULL_TERM_CHAR;
        ret = strncpy((char *)str2_alnd_addr, (char *)str1_alnd_addr, size + VEC_SZ); // intentionally copy more than size bytes

        //validation of return value
        if (ret != str2_alnd_addr)
            printf("ERROR:[PAGE-CROSS] (strlen > n) Return value mismatch: expected - %p, actual - %p\n", \
                                                                 str2_alnd_addr, ret);
        //Validation of str2 memory
        if (test_strcmp((char *)str2_alnd_addr, (char *)str1_alnd_addr)) //Validate till NULL

            printf("ERROR:[PAGE-CROSS] (strlen > n) validation failed for size: %lu @index:%lu" \
                        "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
                  size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);
        else
                ALM_VERBOSE_LOG("[PAGE-CROSS] (strlen > n) validation passed for size: %lu\n", size);

        //validation of NULL bytes in str2 after str1_len
        for (index = size + 1; index < size + VEC_SZ; index++)
        {
            if (str2_alnd_addr[index] != NULL_TERM_CHAR)
            {
                printf("ERROR:[PAGE-CROSS] (strlen > n) NULL Validation failed at index:%lu" \
                            " for size: %ld(strlen = %ld)\n", index, size, str1_len);
                break;
            }
        }
        if (index == size + VEC_SZ)
            ALM_VERBOSE_LOG("[PAGE-CROSS] (strlen > n) NULL Validation passed for size: %lu\n", size);

        //case2: Null exists before size, rand()%VEC_SZ bytes should be NULL
        null_idx = size - (rand() % VEC_SZ) % size;
        *(str1_alnd_addr + null_idx) = NULL_TERM_CHAR;
        ret = strncpy((char *)str2_alnd_addr, (char *)str1_alnd_addr, size + VEC_SZ); // intentionally copy more than size bytes

        //validation of return value
        if (ret != str2_alnd_addr)
            printf("ERROR:[PAGE-CROSS] (strlen < n) Return value mismatch: expected - %p, actual - %p\n", \
                                                                 str2_alnd_addr, ret);
        //Validation of str2 memory
        if (test_strcmp((char *)str2_alnd_addr, (char *)str1_alnd_addr)) //Validate till NULL

            printf("ERROR:[PAGE-CROSS] (strlen < n) validation failed for size: %lu @index:%lu" \
                        "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
                  size, index, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);
        else
                ALM_VERBOSE_LOG("[PAGE-CROSS] (strlen < n) validation passed for size: %lu\n", size);

        for (index = null_idx + 1; index < size + VEC_SZ; index++)
        {
            if (str2_alnd_addr[index] != NULL_TERM_CHAR)
            {
                printf("ERROR:[PAGE-CROSS] (strlen < n) NULL Validation failed at index:%lu" \
                            " for size: %ld(strlen = %ld)\n", index, size, str1_len);
                break;
            }
        }
        if (index == size + VEC_SZ)
            ALM_VERBOSE_LOG("[PAGE-CROSS] (strlen < n) NULL Validation passed for size: %lu\n", size);

        cleanup_page_cross_buffer(page_buff, page_cnt);
    }
    free(buff);
}

static inline void strcmp_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str2_alnd_addr = NULL, *str1_alnd_addr = NULL;
    size_t index;
    int ret = 0;
    int validation1_passed = 1, validation2_passed = 1;
    int exp_ret = 0;

    //special case to handle size ZERO with NULL string inputs.
    if (size == 0)
    {   
        char *src = NULL_STRING;
        char *dst = NULL_STRING;
        ret = strcmp(dst, src);
        if (ret != exp_ret)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - %d"\
                        ", actual - %d\n", size, exp_ret, ret);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + VEC_SZ, NON_OVERLAP_BUFFER); // + VEC_SZ for extra_size

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str2_alnd_addr = buff_tail + str2_alnmnt;
    str1_alnd_addr = buff_head + str1_alnmnt;

    //Case1: Equal strings
    //intialize str1 and str2 memory
    for (index = 0; index < (size -1); index++)
    {
        *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
    }

    //Appending Null character at the end of str1 and str2
    *(str1_alnd_addr + size - 1) = *(str2_alnd_addr + size - 1) = NULL_TERM_CHAR;
    ret = strcmp((char *)str2_alnd_addr, (char *)str1_alnd_addr);

    if (ret != 0)
    {
        printf("ERROR:[VALIDATION] failed for matching data of str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d\n",str1_alnmnt,str2_alnmnt, size, ret);
    }
    else
    {
        ALM_VERBOSE_LOG("Validation passed for matching memory of size: %lu\n", size);
    }

    //Case2:  Validation of Byte by Byte mismtach for MATCHING string
    for (index = 0; index < size; index++)
    {
       //set a byte of source different from destination @index
       //Case2(a): s1 < s2
           *(str1_alnd_addr + index) = '$';

       ret = strcmp((char *)str1_alnd_addr, (char *)str2_alnd_addr); // -ve value expected
       exp_ret = string_cmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, -1);

       if (ret != exp_ret)
       {
           printf("ERROR:[VALIDATION] (str1<str2) failed for Non-Matching @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d exp=%d\n", index, str1_alnmnt, str2_alnmnt, size, ret, exp_ret);
           validation1_passed = 0;
       }

       //Case2(b): s1 > s2
       ret = strcmp((char *)str2_alnd_addr, (char *)str1_alnd_addr); // +ve value expected
       exp_ret = (*(uint8_t *)(str2_alnd_addr + index) - \
                        *(uint8_t *)(str1_alnd_addr + index));
       if (ret != exp_ret)
       {
           printf("ERROR:[VALIDATION] (str1>str2) failed for Non-Matching @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d exp=%d\n", index, str1_alnmnt, str2_alnmnt, size, ret, exp_ret);
           validation2_passed = 0;
       }
       //copying back the mismatching byte
       *(str1_alnd_addr + index) = *(str2_alnd_addr + index);
    }

    if (validation1_passed && validation2_passed)
         ALM_VERBOSE_LOG("Validation successfull for non-matching data of size: %lu\n",\
                                                                          size);
    //Case3: Multi-NULL check
    //Can't have multiple NULL for size < 2
    if (size >= 2)
    {
        size_t more_null_idx = rand() % (size - 1);
        *(str1_alnd_addr + more_null_idx) = *(str2_alnd_addr + more_null_idx) = NULL_TERM_CHAR;

        //Set a Mismatching byte after NULL @more_null_idx + 1
        *(str1_alnd_addr + more_null_idx + 1) = '@';

        ret = strcmp((char *)str2_alnd_addr, (char *)str1_alnd_addr);
        exp_ret = string_cmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, -1);

        if (ret != exp_ret)
        {
            printf("ERROR:[VALIDATION] Multi-NULL failed for size: %lu @Mismatching index:%lu" \
                    "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
                size, more_null_idx + 1, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);

        }
        else
            ALM_VERBOSE_LOG("Multi-NULL check Validation passed for size: %lu\n", size);
    }

    //case4: strlen(str1), strlen(str2) > size and are matching
    size_t extra_size = (rand() % VEC_SZ) + size + 1; //extra_size between [size+1, size+VEC_SZ]
    for (index = 0; index < extra_size; index++)
    {
        *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'A'+ rand() % LOWER_CHARS);
    }
    //Appending Null Character at the end of str1 and str2
    *(str1_alnd_addr + extra_size - NULL_BYTE) = *(str2_alnd_addr + extra_size - NULL_BYTE) = NULL_TERM_CHAR;

    ret = strcmp((char *)str1_alnd_addr, (char *)str2_alnd_addr);
    exp_ret = string_cmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, -1);

    if (ret!=exp_ret)
    {
        printf("ERROR:[VALIDATION] (str1(%lu) & str2(%lu) > size) failed for str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d, exp=%d",strlen((char*)str1_alnd_addr),strlen((char*)str2_alnd_addr), str1_alnmnt, str2_alnmnt, size, ret, exp_ret);
    }

    //case5: strlen(str1) = size and strlen(str2) > size

    *(str1_alnd_addr + size - 1) = NULL_TERM_CHAR;

    ret = strcmp((char *)str1_alnd_addr, (char *)str2_alnd_addr);
    exp_ret = string_cmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, -1);
    if (ret != exp_ret)
    {
        printf("ERROR:[VALIDATION] (str1=%lu, str2(%lu) >size) failed for string @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d\n", size, strlen((char*)str2_alnd_addr), size-1,str1_alnmnt,str2_alnmnt, size, ret);
    }

    //case6:strlen(str2) = size and strlen(str1) > size
    test_strcpy((char *)str1_alnd_addr, (char *)str2_alnd_addr);
    *(str2_alnd_addr + size - 1) = NULL_TERM_CHAR;

    ret = strcmp((char *)str1_alnd_addr,(char *)str2_alnd_addr);
    exp_ret = string_cmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, -1);

    if (ret != exp_ret)
    {
        printf("ERROR:[VALIDATION] (str2=%lu, str1(%lu) >size) failed for string @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d\n", size, strlen((char*)str2_alnd_addr), size-1,str1_alnmnt,str2_alnmnt, size, ret);
    }

    // Smart page-cross check: Apply unless NEITHER string is a candidate
    void *page_buff1 = NULL, *page_buff2 = NULL;
    uint32_t page_cnt;

    if (setup_dual_page_cross_buffers(&str1_alnd_addr, &str2_alnd_addr,
                                     str1_alnmnt, str2_alnmnt, size, buff, 1,
                                     &page_buff1, &page_buff2, &page_cnt)) {

        for (index = 0; index < size; index++) {
            *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
        }
        *(str1_alnd_addr + size - 1) = *(str2_alnd_addr + size - 1) = NULL_TERM_CHAR;

        ret = strcmp((char *)str1_alnd_addr, (char *)str2_alnd_addr);
        if (ret != 0) {
            printf("ERROR:[PAGE-CROSS] failure for str1_aln:%u str2_aln:%u size: %lu\n", str1_alnmnt, str2_alnmnt, size);
        }

        // Cleanup page buffers
        cleanup_dual_page_cross_buffers(page_buff1, page_buff2, page_cnt);
    }
    free(buff);
}

static inline void strncmp_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head = SINGLE_CHAR_STRING, *buff_tail = (uint8_t*)NULL_STRING;
    uint8_t *str2_alnd_addr = NULL, *str1_alnd_addr = NULL;
    size_t index;
    int ret = 0;
    int validation1_passed = 1, validation2_passed = 1;
    int exp_ret = 0;

    //special case to handle size ZERO with NULL buff inputs.
    if (size == 0)
    {
        ret = strncmp((char*)buff_head, (char*)buff_tail, size);
        if (ret != exp_ret)
            printf("ERROR:[VALIDATION] failure for size(%lu): expected - %d"\
                        ", actual - %d\n", size, exp_ret, ret);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str2_alnd_addr = buff_tail + str2_alnmnt;
    str1_alnd_addr = buff_head + str1_alnmnt;

    //Case1: Equal strings with NULL
    //intialize str1 and str2 memory
    for (index = 0; index < size; index++)
    {
        *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
    }
    //Appending Null character at the end of str1 and str2
    *(str1_alnd_addr + size - 1) = *(str2_alnd_addr + size - 1) = NULL_TERM_CHAR;
    ret = strncmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);

    if (ret != 0)
    {
        printf("ERROR:[VALIDATION] Matching failure for str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d\n",str1_alnmnt,str2_alnmnt, size, ret);
    }

    //passing additional VEC_SZ bytes without NULL, but has mismatching byte after size
    //to check if function is not going beyond 'size' bytes
    *(str1_alnd_addr + size - 1) = ((char) 'a' + rand() % LOWER_CHARS);
    *(str2_alnd_addr + size - 1) = 'X'; //mismatching UPPER case letter
    ret = strncmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size + VEC_SZ); // intentionally compare more than size bytes
    exp_ret = (*(uint8_t *)(str1_alnd_addr + size - 1) - \
                        *(uint8_t *)(str2_alnd_addr + size - 1));
    if (ret != exp_ret)
    {
        printf("ERROR:[VALIDATION] Matching failure without NULL for str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d exp=%d \n",str1_alnmnt,str2_alnmnt, size, ret, exp_ret);
    }

    else
    {
        ALM_VERBOSE_LOG("Validation passed for matching memory of size: %lu\n", size);
    }

    //Case2:  Validation of Byte by Byte mismtach for MATCHING string
    for (index = 0; index < size; index++)
    {
       //set a byte of source different from destination @index
       //Case2(a): s1 < s2
           *(str1_alnd_addr + index) = '$';

       ret = strncmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size + VEC_SZ); // -ve value expected
       exp_ret = (*(uint8_t *)(str1_alnd_addr + index) - \
                        *(uint8_t *)(str2_alnd_addr + index));

       if (ret != exp_ret)
       {
           printf("ERROR:[VALIDATION] (str1<str2) failure for Non-Matching @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d exp=%d\n", index, str1_alnmnt, str2_alnmnt, size, ret, exp_ret);
           validation1_passed = 0;
       }

       //Case2(b): s1 > s2
       ret = strncmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size + VEC_SZ); // +ve value expected
       exp_ret = (*(uint8_t *)(str2_alnd_addr + index) - \
                        *(uint8_t *)(str1_alnd_addr + index));
       if (ret != exp_ret)
       {
           printf("ERROR:[VALIDATION] (str1>str2) failure for Non-Matching @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d exp=%d\n", index, str1_alnmnt, str2_alnmnt, size, ret, exp_ret);
           validation2_passed = 0;
       }
       //copying back the mismatching byte
       *(str1_alnd_addr + index) = *(str2_alnd_addr + index);
    }

    if (validation1_passed && validation2_passed)
         ALM_VERBOSE_LOG("Validation successfull for non-matching data of size: %lu\n",\
                                                                          size);
    //Case3: Multi-NULL check
    //Can't have multiple NULL for size < 2
    if (size >= 2)
    {
        size_t more_null_idx = rand() % (size - 1);
        *(str1_alnd_addr + more_null_idx) = *(str2_alnd_addr + more_null_idx) = NULL_TERM_CHAR;

        //Set a Mismatching byte after NULL @more_null_idx + 1
        *(str1_alnd_addr + more_null_idx + 1) = '@';

        ret = strncmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);
        exp_ret = string_cmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);

        if (ret != exp_ret)
        {
            printf("ERROR:[VALIDATION] Multi-NULL failed for size: %lu @Mismatching index:%lu" \
                    "[str1: %p(alignment = %u), str2:%p(alignment = %u)]\n", \
                size, more_null_idx + 1, str1_alnd_addr, str1_alnmnt, str2_alnd_addr, str2_alnmnt);

        }
        else
            ALM_VERBOSE_LOG("Multi-NULL check Validation passed for size: %lu\n", size);
    }

    //case4: strlen(str1), strlen(str2) > size and are matching

    for (index = 0; index < size; index++)
    {
        *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'A'+ rand() % LOWER_CHARS);
    }

    ret = strncmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size);
    exp_ret = string_cmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size);

    if (ret!=exp_ret)
    {
        printf("ERROR:[VALIDATION] (str1(%lu) & str2(%lu) > size) failure for str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d, exp=%d",strlen((char*)str1_alnd_addr),strlen((char*)str2_alnd_addr),str1_alnmnt,str2_alnmnt, size, ret, exp_ret);
    }

    //case5: strlen(str1) = size and strlen(str2) > size

    *(str1_alnd_addr + size -1) = NULL_TERM_CHAR;
    ret = strncmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);
    exp_ret = (*(uint8_t *)(str2_alnd_addr + size - 1) - \
                        *(uint8_t *)(str1_alnd_addr + size - 1));
    if (ret != exp_ret)
    {
        printf("ERROR:[VALIDATION] (str1=size, str2(%lu) > size) failure for string @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d\n", strlen((char*)str2_alnd_addr), size-1,str1_alnmnt,str2_alnmnt, size, ret);
    }

    //case6:strlen(str2) = size and strlen(str1) > size

    ret = strncmp((char *)str1_alnd_addr,(char *)str2_alnd_addr, size);
    exp_ret = (*(uint8_t *)(str1_alnd_addr + size - 1) - \
                        *(uint8_t *)(str2_alnd_addr + size - 1));

    if (ret != exp_ret)
    {
        printf("ERROR:[VALIDATION] (str2=size, str1(%lu) > size) failure for string @index:%lu str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d\n", strlen((char*)str2_alnd_addr), size-1,str1_alnmnt,str2_alnmnt, size, ret);
    }

    //Case7: strlen(str1), strlein(str2) < size
    if (size >=2)
    {
        //case7(a): strlen(str1) < strlen(str2)
        *(str1_alnd_addr + size - 1) = *(str2_alnd_addr + size - 1); //Removing Null from str1
        size_t s1_sz = rand() % (size / 2 ); //strlen(str1) between [0, size/2 -1]
        size_t s2_sz = rand() % (size / 2) + size / 2; //strlen(str2) between [size/2 + 1, size-1]

        *(str1_alnd_addr + s1_sz) = NULL_TERM_CHAR;
        *(str2_alnd_addr + s2_sz) = NULL_TERM_CHAR;

        ret = strncmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);
        exp_ret = string_cmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size);

        if (ret != exp_ret)
        {
           printf("ERROR:[VALIDATION] (str1(%lu) < str2(%lu)) failure for Non-Matching string str1_aln:%u str2_aln:%u ,"\
                                    " return_value = %d, exp_value =%d\n", s1_sz, s2_sz, str1_alnmnt, str2_alnmnt, ret, exp_ret);
        }

        //case7(b): strlen(str1) > strlen(str2)
        ret = strncmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size);
        exp_ret = string_cmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size);

        if (ret != exp_ret)
        {
           printf("ERROR:[VALIDATION] (str1_sz(%lu) > str2_sz(%lu)) failure for Non-Matching string str1_aln:%u str2_aln:%u size: %lu,"\
                                    " return_value = %d, exp_value=%d\n", s2_sz, s1_sz, str1_alnmnt, str2_alnmnt, size, ret,exp_ret);
        }
    }

    // Smart page-cross check: Apply unless NEITHER string is a candidate
    void *page_buff1 = NULL, *page_buff2 = NULL;
    uint32_t page_cnt;

    if (setup_dual_page_cross_buffers(&str1_alnd_addr, &str2_alnd_addr,
                                     str1_alnmnt, str2_alnmnt, size, buff, 0,
                                     &page_buff1, &page_buff2, &page_cnt)) {

        // Test case 1: Equal strings
        for (index = 0; index < size; index++) {
            *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
        }
        *(str1_alnd_addr + size - 1) = *(str2_alnd_addr + size - 1) = NULL_TERM_CHAR;

        ret = strncmp((char *)str2_alnd_addr, (char *)str1_alnd_addr, size + VEC_SZ); //Intentionally using size + VEC_SZ
        if (ret != 0) {
            printf("ERROR:[PAGE-CROSS] Equal strings failure for str1_aln:%u str2_aln:%u size: %lu\n", str1_alnmnt, str2_alnmnt, size);
        } else {
            ALM_VERBOSE_LOG("[PAGE-CROSS] Equal strings validation passed for size: %lu\n", size);
        }

        // Test case 2: Different strings (first string smaller)
        *(str1_alnd_addr + size - 1) = 'a';
        *(str2_alnd_addr + size - 1) = 'b';

        ret = strncmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size + VEC_SZ); //Intentionally using size + VEC_SZ
        exp_ret = (*(uint8_t *)(str1_alnd_addr + size - 1) - *(uint8_t *)(str2_alnd_addr + size - 1));
        if (ret != exp_ret) {
            printf("ERROR:[PAGE-CROSS] Different strings (str1<str2) failure for str1_aln:%u str2_aln:%u size: %lu, return_value = %d, expected = %d\n",
                   str1_alnmnt, str2_alnmnt, size, ret, exp_ret);
        } else {
            ALM_VERBOSE_LOG("[PAGE-CROSS] Different strings validation passed for size: %lu\n", size);
        }

        // Test case 3: Strings shorter than n (null terminator before n bytes)
        for (index = 0; index < size; index++) {
            *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'c' + rand() % LOWER_CHARS);
        }

        *(str1_alnd_addr + size - (rand() % VEC_SZ) % size) = NULL_TERM_CHAR; //Randomly place NULL within last vector
        *(str2_alnd_addr + size - (rand() % VEC_SZ) % size) = NULL_TERM_CHAR; //Randomly place NULL within last vector

        ret = strncmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size + VEC_SZ); //Intentionally using size + VEC_SZ
        if (ret != test_strcmp((char *)str1_alnd_addr, (char *)str2_alnd_addr)) {
            printf("ERROR:[PAGE-CROSS] Short strings failure for str1_aln:%u str2_aln:%u size: %lu\n", str1_alnmnt, str2_alnmnt, size);
        } else {
            ALM_VERBOSE_LOG("[PAGE-CROSS] Short strings validation passed for size: %lu\n", size);
        }

        //Test case 4: Strings longer than n (no null terminator before n bytes)
        for (index = 0; index < size; index++) {
            *(str1_alnd_addr + index) = *(str2_alnd_addr + index) = ((char) 'c' + rand() % LOWER_CHARS);
        }
        ret = strncmp((char *)str1_alnd_addr, (char *)str2_alnd_addr, size); //Exact size
        //Expecting 0 as both strings are equal till size
        if (ret != 0) {
            printf("ERROR:[PAGE-CROSS] Long strings failure for str1_aln:%u str2_aln:%u size: %lu\n", str1_alnmnt, str2_alnmnt, size);
        } else {
            ALM_VERBOSE_LOG("[PAGE-CROSS] Long strings validation passed for size: %lu\n", size);
        }

        // Cleanup page buffers
        cleanup_dual_page_cross_buffers(page_buff1, page_buff2, page_cnt);
    }
    free(buff);
}

static inline void strlen_validator(size_t size, uint32_t str2_alnmnt __attribute__((unused)),\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str_alnd_addr = NULL;
    size_t index;
    size_t ret = 0;

    buff = alloc_buffer(&buff_head, &buff_tail, size + 1, DEFAULT);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }

    str_alnd_addr = buff_tail + str1_alnmnt;

    for (index = 0; index < size; index++)
    {
        *(str_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
    }
    //Appending Null character at the end of str
    *(str_alnd_addr + size) = NULL_TERM_CHAR;

    //Adding Additional NULL char after size
    *(str_alnd_addr + size + rand() % 8 ) = NULL_TERM_CHAR;
    ret = strlen((char *)str_alnd_addr);

    if (ret != size)
    {
        printf("ERROR:[VALIDATION] failure for strlen of str1_aln:%u size: %lu,"\
                                    " return_value = %lu\n",str1_alnmnt, size, ret);
    }
    else
    {
        ALM_VERBOSE_LOG("Validation passed for strlen: %lu\n", size);
    }

    // Robust page-cross check: Only if string does NOT cross last boundary vector
    // This identifies good candidates for page-cross testing
    void *page_buff;
    uint32_t page_cnt;

    if (setup_single_page_cross_buffer(&str_alnd_addr, str1_alnmnt, size, buff, 0, &page_buff, &page_cnt)) {
        for (index = 0; index < size; index++) {
            *(str_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
        }
        *(str_alnd_addr + size) = NULL_TERM_CHAR;
        ret = strlen((char *)str_alnd_addr);
        if (ret != size) {
            printf("ERROR:[PAGE-CROSS] failure for str1_aln:%u size: %lu\n", str1_alnmnt, size);
        }

        cleanup_page_cross_buffer(page_buff, page_cnt);
    }
    free(buff);
}

static inline void strnlen_validator(size_t size, uint32_t str2_alnmnt __attribute__((unused)),\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str_alnd_addr = NULL;
    size_t index;
    size_t ret = 0;
    size_t maxlen;

    buff = alloc_buffer(&buff_head, &buff_tail, size + 1, DEFAULT);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }

    str_alnd_addr = buff_tail + str1_alnmnt;

    for (index = 0; index < size; index++)
    {
        *(str_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
    }

    //Appending Null character at the end of str
    *(str_alnd_addr + size) = NULL_TERM_CHAR;

    // Test case 1: maxlen > strlen (should return strlen)
    maxlen = size + 1 + (rand() % 100);
    ret = strnlen((char *) str_alnd_addr, maxlen);

    if (ret != size)
    {
        printf("ERROR:[VALIDATION] (maxlen > strlen) failure for str1_aln:%u size: %lu,"\
                                    " maxlen: %lu, return_value = %lu\n",str1_alnmnt, size, maxlen, ret);
    }
    else
    {
        ALM_VERBOSE_LOG("[maxlen > strlen] Validation passed for strnlen: size=%lu, maxlen=%lu\n", size, maxlen);
    }

    // Test case 2: maxlen < strlen (should return maxlen)
    if (size > 1)
    {
        maxlen = size / 2;
        ret = strnlen((char *)str_alnd_addr, maxlen);

        if (ret != maxlen)
        {
            printf("ERROR:[VALIDATION] (maxlen < strlen) failure for str1_aln:%u size: %lu,"\
                                        " maxlen: %lu, return_value = %lu\n",str1_alnmnt, size, maxlen, ret);
        }
        else
        {
            ALM_VERBOSE_LOG("[maxlen < strlen] Validation passed for strnlen: size=%lu, maxlen=%lu\n", size, maxlen);
        }
    }

    // Test case 3: maxlen == strlen (should return strlen)
    maxlen = size;
    ret = strnlen((char *)str_alnd_addr, maxlen);

    if (ret != size)
    {
        printf("ERROR:[VALIDATION] (maxlen == strlen) failure for str1_aln:%u size: %lu,"\
                                    " maxlen: %lu, return_value = %lu\n",str1_alnmnt, size, maxlen, ret);
    }
    else
    {
        ALM_VERBOSE_LOG("[maxlen == strlen] Validation passed for strnlen: size=%lu, maxlen=%lu\n", size, maxlen);
    }

    // Test case 4: maxlen == 0 (should return 0)
    maxlen = 0;
    ret = strnlen((char *)str_alnd_addr, maxlen);

    if (ret != 0)
    {
        printf("ERROR:[VALIDATION] (maxlen == 0) failure for str1_aln:%u size: %lu,"\
                                    " return_value = %lu (expected 0)\n",str1_alnmnt, size, ret);
    }
    else
    {
        ALM_VERBOSE_LOG("[maxlen == 0] Validation passed for strnlen: size=%lu\n", size);
    }

    // Robust page-cross check: Only if string does NOT cross last boundary vector
    // This identifies good candidates for page-cross testing
    void *page_buff;
    uint32_t page_cnt;

    if (setup_single_page_cross_buffer(&str_alnd_addr, str1_alnmnt, size, buff, 0, &page_buff, &page_cnt)) {
        for (index = 0; index < size; index++) {
            *(str_alnd_addr + index) = ((char) 'a' + rand() % LOWER_CHARS);
        }
        *(str_alnd_addr + size) = NULL_TERM_CHAR;

        // Page-cross test with maxlen > strlen
        maxlen = size + 1 + (rand() % 100);
        ret = strnlen((char *) str_alnd_addr, maxlen);
        if (ret != size) {
            printf("ERROR:[PAGE-CROSS] (maxlen > strlen) failure for str1_aln:%u size: %lu, maxlen: %lu\n", 
                   str1_alnmnt, size, maxlen);
        }
        // Page-cross test with maxlen < strlen
        if (size > 1) 
        {
            maxlen = size / 2;
            ret = strnlen((char *)str_alnd_addr, maxlen);
            if (ret != maxlen) 
            {
                printf("ERROR:[PAGE-CROSS] (maxlen < strlen) failure for str1_aln:%u size: %lu, maxlen: %lu\n",
                       str1_alnmnt, size, maxlen);
            }
        }

        cleanup_page_cross_buffer(page_buff, page_cnt);
    }
    free(buff);
}

static inline void memchr_validator(size_t size, uint32_t str2_alnmnt __attribute__((unused)),\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str_alnd_addr = NULL;
    char* res;
    int find = '#';
    buff = alloc_buffer(&buff_head, &buff_tail, size + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str_alnd_addr = buff_tail + str1_alnmnt;

    if (size == 0)
    {
        res = memchr((char *)str_alnd_addr, find, size);
        if (res != NULL)
            printf("ERROR:[RETURN] value mismatch for size(%lu): expected - NULL"\
                        ", actual - %p\n", size, res);
        return;
    }

    init_buffer((char*)str_alnd_addr, size);
    prepare_boundary(str_alnd_addr, size);

    find = '#'; //ASCII value 35
    res = memchr((char *)str_alnd_addr, find, size);

    if(res != NULL)
    {
        printf("ERROR:[BOUNDARY] Out of bound Data failure for memchr of str1_aln:%u size: %lu, find:%c\n"\
                                    " return_value =%s\n EXP:NULL\n STR:%s\n",str1_alnmnt, size, find, res, str_alnd_addr);
    }

    init_buffer((char*)str_alnd_addr, size);
    find = '!'; // ASCII value 33
    res = memchr((char *)str_alnd_addr, find, size);

    if (res != test_memchr((char *)str_alnd_addr, find, size))
    {
        printf("ERROR:[VALIDATION] failure for memchr of str1_aln:%u size: %lu, find:%c\n"\
                                    " return_value =%s\n STR:%s\n",str1_alnmnt, size, find, res, str_alnd_addr);
    }
    else
    {
        ALM_VERBOSE_LOG("Validation passed for memchr: %lu\n", size);
    }

    //Modifying the needle char
    find = ' '; //ASCII value 32 (SPACE)
    res = memchr((char *)str_alnd_addr, find, size);

    if (res != NULL)
    {
        printf("ERROR:[VALIDATION] failure for memchr of str1_aln:%u size: %lu, find:%c\n"\
                                    " return_value =%s\n EXP:NULL\n STR:%s\n",str1_alnmnt, size, find, res, str_alnd_addr);
    }
    else
    {
        ALM_VERBOSE_LOG("Validation passed for memchr: %lu\n", size);
    }
    free(buff);
}

static inline void strcat_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *str1_buff = NULL, *str2_buff = NULL, *temp_buff = NULL, *buff_head, *buff_tail;
    char *str2_alnd_addr = NULL, *str1_alnd_addr = NULL;
    size_t index;
    char *ret = NULL;

    if(size == 0) //src is null string with extra bytes
    {
        uint8_t src[] = {NULL_TERM_CHAR,'?','?'};
        uint8_t dst[] = {'!',NULL_TERM_CHAR,'!','!'};
        uint8_t dst_before[sizeof(dst)];
        test_memcpy(dst_before, dst,sizeof(dst));
        strcat((char*)dst, (char*)src);
        if (test_memcmp(dst_before,dst,sizeof(dst)) != 0)
        {
           printf("ERROR:[VALIDATION] failure for size(%lu): dst buffer modified \n", size);

        }

        return;
    }
    str1_buff = alloc_buffer(&buff_head, &buff_tail,2 * size + NULL_BYTE, DEFAULT);
    // String size big enough to accommodate str2
    if (str1_buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str1_alnd_addr = (char *)buff_tail + str1_alnmnt;
    str2_buff = alloc_buffer(&buff_head, &buff_tail, size + NULL_BYTE, DEFAULT);
    if (str2_buff == NULL)
    {
        free(str1_buff);
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str2_alnd_addr = (char *)buff_tail + str2_alnmnt;

    //intialize str1 & str2 memory
    for (index = 0; index < size; index++)
    {
        *(str1_alnd_addr + index) = 'a' + (char)(rand() % LOWER_CHARS);
        *(str2_alnd_addr + index) = 'a' + (char)(rand() % LOWER_CHARS);
    }
    //Appending Null character at the end of str1 string
    *(str1_alnd_addr + size - 1) = NULL_TERM_CHAR;
    *(str2_alnd_addr + (rand()%size)) = NULL_TERM_CHAR;
    //Source Corruption
    temp_buff = alloc_buffer(&buff_head, &buff_tail, 2 * size + NULL_BYTE, DEFAULT);

    if (temp_buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        free(str1_buff);
        free(str2_buff);
        exit(-1);
    }
    uint8_t *tmp_alnd_addr = NULL;
    tmp_alnd_addr = buff_tail + str1_alnmnt;

    test_strcpy((char *)tmp_alnd_addr, (char *)str1_alnd_addr);
    ret = strcat((char *)str1_alnd_addr, (char *)str2_alnd_addr);

    if(test_strcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str2_alnd_addr, SIZE_MIN),(char *)str1_alnd_addr) != 0)
    {
        printf("ERROR: [VALIDATION] failed\n str1:%s\n str2:%s\n str1+str2:%s\n",tmp_alnd_addr, str2_alnd_addr,str1_alnd_addr);
    }
    if (ret != str1_alnd_addr)
    {
        printf("ERROR:[VALIDATION] failure for size: %lu,"\
                " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
    }
    //Multi-Null check
    size_t more_null_idx = rand() % (size);
    *(tmp_alnd_addr + more_null_idx) = NULL_TERM_CHAR;
    test_strcpy((char *)str1_alnd_addr, (char *)tmp_alnd_addr);

    *(str2_alnd_addr + more_null_idx) = NULL_TERM_CHAR;
    ret = strcat((char *)str1_alnd_addr, (char *)str2_alnd_addr);

    if(test_strcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str2_alnd_addr, SIZE_MIN),(char *)str1_alnd_addr ) != 0)
    {
        printf("ERROR: [VALIDATION] MultiNull check failed\n str1:%s\n str2:%s\n str1+str2:%s\n",tmp_alnd_addr, str2_alnd_addr,str1_alnd_addr);
    }
    if (ret != str1_alnd_addr)
    {
        printf("ERROR:[VALIDATION] failure for size: %lu,"\
                " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
    }

    free(str2_buff);
    // Robust page-cross check: Only for good candidates (does NOT cross last boundary vector)
    if (is_page_cross_candidate(size, str2_alnmnt)) {
        void *str2_page_buff = NULL;
        uint32_t page_cnt = PAGE_CNT(2 * size);

        alloc_page_cross_buffer(&str2_page_buff, page_cnt, str1_buff);

        str2_alnd_addr = (char *)str2_page_buff + page_cnt* PAGE_SZ - (size + NULL_BYTE + str2_alnmnt);
        for (index = 0; index < size; index++) {
            *(str1_alnd_addr +index) = ((char) 'a' + rand() % LOWER_CHARS);
            *(str2_alnd_addr +index) = ((char) 'a' + rand() % LOWER_CHARS);
        }
        *(str1_alnd_addr + size -1) = *(str2_alnd_addr + (rand()%size)) = NULL_TERM_CHAR;
        test_strcpy((char *)tmp_alnd_addr, (char *)str1_alnd_addr);
        ret = strcat((char *)str1_alnd_addr, (char *)str2_alnd_addr);
        if(test_strcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str2_alnd_addr, SIZE_MIN), (char *)str1_alnd_addr) != 0) {
            printf("ERROR: [PAGE-CROSS] failed\n str1:%s\n str2:%s\n str1+str2:%s\n",tmp_alnd_addr, str2_alnd_addr,str1_alnd_addr);
        }
        if (ret != str1_alnd_addr) {
            printf("ERROR:[VALIDATION] failure for size: %lu,"\
                    " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
        }
        //Page_check with Multi-NULL
        more_null_idx = rand() % size;
        *(tmp_alnd_addr + more_null_idx) = NULL_TERM_CHAR;
        test_strcpy((char *)str1_alnd_addr, (char *)tmp_alnd_addr);
        *(str2_alnd_addr + more_null_idx) = NULL_TERM_CHAR;
        ret = strcat((char *)str1_alnd_addr, (char *)str2_alnd_addr);
        if(test_strcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str2_alnd_addr, SIZE_MIN),(char *)str1_alnd_addr) != 0) {
            printf("ERROR: [PAGE-CROSS] MultiNull check failed\n str1:%s\n str2:%s\n str1+str2:%s\n",tmp_alnd_addr, str2_alnd_addr,str1_alnd_addr);
        }
        if (ret != str1_alnd_addr) {
            printf("ERROR:[VALIDATION] failure for size: %lu,"\
                    " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
        }

        cleanup_page_cross_buffer(str2_page_buff, page_cnt);
    }
    free(temp_buff);
    free(str1_buff);
}

static inline void strncat_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *str1_buff = NULL, *str2_buff = NULL, *temp_buff = NULL, *buff_head, *buff_tail;
    char *str2_alnd_addr = NULL, *str1_alnd_addr = NULL, *str3_alnd_addr = NULL ;
    size_t index;
    char *ret = NULL;

    if(size == 0) //For Null, expected behaviour is SEGFAULT
    {
        return;
    }
    str1_buff = alloc_buffer(&buff_head, &buff_tail,2 * size + NULL_BYTE, DEFAULT);
    // String size big enough to accommodate str2
    if (str1_buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str1_alnd_addr = (char *)buff_tail + str1_alnmnt;
    str2_buff = alloc_buffer(&buff_head, &buff_tail, size + NULL_BYTE, DEFAULT);
    if (str2_buff == NULL)
    {
        free(str1_buff);
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    str2_alnd_addr = (char *)buff_tail + str2_alnmnt;

    //intialize str1 & str2 memory
    for (index = 0; index < size; index++)
    {
        *(str1_alnd_addr + index) = random_char();
        *(str2_alnd_addr + index) = random_char();
    }
    //Appending Null character at the end of str1 string
    *(str1_alnd_addr + size - 1) = NULL_TERM_CHAR;
    *(str2_alnd_addr + (rand() % size)) = NULL_TERM_CHAR;
    //Source Corruption
    temp_buff = alloc_buffer(&buff_head, &buff_tail, 2 * size + NULL_BYTE, DEFAULT);

    if (temp_buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        free(str1_buff);
        free(str2_buff);
        exit(-1);
    }
    char *tmp_alnd_addr = NULL;
    tmp_alnd_addr = (char *)buff_tail + str1_alnmnt;

    test_strcpy((char *)tmp_alnd_addr, (char *)str1_alnd_addr);
    ret = strncat((char *)str1_alnd_addr, (char *)str2_alnd_addr, size);

    if(memcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str2_alnd_addr, size),(char *)str1_alnd_addr, size) != 0)
    {
        printf("ERROR: [VALIDATION] failed for size:%zu\n str1:%s\n str2:%s\n str1+str2:%s\n", size, tmp_alnd_addr, str2_alnd_addr,str1_alnd_addr);
    }
    if (ret != str1_alnd_addr)
    {
        printf("ERROR:[VALIDATION] failure for size: %lu,"\
                " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
    }

    //Multi-Null check
    size_t more_null_idx = rand() % (size);
    *(tmp_alnd_addr + more_null_idx) = NULL_TERM_CHAR;
    test_strcpy((char *)str1_alnd_addr, (char *)tmp_alnd_addr);

    *(str2_alnd_addr + more_null_idx) = NULL_TERM_CHAR;
    ret = strncat((char *)str1_alnd_addr, (char *)str2_alnd_addr, size);

    if(memcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str2_alnd_addr, size),(char *)str1_alnd_addr, size) != 0)
    {
        printf("ERROR: [VALIDATION] MultiNull check failed for size:%zu\n str1:%s\n str2:%s\n str1+str2:%s\n", size, tmp_alnd_addr, str2_alnd_addr,str1_alnd_addr);
    }
    if (ret != str1_alnd_addr)
    {
        printf("ERROR:[VALIDATION] failure for size: %lu,"\
                " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
    }

    // Robust page-cross check: Only for good candidates (does NOT cross last boundary vector)
    if (is_page_cross_candidate(size, str2_alnmnt)) {
        void *str2_page_buff = NULL;
        uint32_t page_cnt = PAGE_CNT(2 * size);

        // Need to handle cleanup of temp_buff and str1_buff - use a compound cleanup approach
        if (posix_memalign(&str2_page_buff, PAGE_SZ, (page_cnt + 1) * PAGE_SZ) != 0) {
            printf("[ERROR] Failed to allocate memory for page-cross testing\n");
            free(temp_buff);
            free(str1_buff);
            free(str2_buff);
            exit(-1);
        }
        if (convert_page_to_inaccessible(str2_page_buff, page_cnt) != 0) {
            free(str2_page_buff);
            free(temp_buff);
            free(str1_buff);
            free(str2_buff);
            exit(-1);
        }

        str3_alnd_addr = (char *)str2_page_buff + page_cnt* PAGE_SZ - (size + NULL_BYTE + str2_alnmnt);
        test_strcpy(str1_alnd_addr, tmp_alnd_addr);
        test_strcpy(str3_alnd_addr, str2_alnd_addr);
        more_null_idx = rand()%size;
        *(str1_alnd_addr + size -1) = *(str3_alnd_addr + more_null_idx) =*(tmp_alnd_addr + size -1) = *(str2_alnd_addr + more_null_idx) = NULL_TERM_CHAR;
        test_strcpy((char *)tmp_alnd_addr, (char *)str1_alnd_addr);
        ret = strncat((char *)str1_alnd_addr, (char *)str3_alnd_addr, size);
        if (memcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str3_alnd_addr, size), (char *)str1_alnd_addr, size) != 0) {
            printf("ERROR: [PAGE-CROSS] failed for size:%zu\n str1:%s\n str2:%s\n str1+str2:%s\n", size, tmp_alnd_addr, str3_alnd_addr,str1_alnd_addr);
        }
        if (ret != str1_alnd_addr) {
            printf("ERROR:[VALIDATION] failure for size: %lu,"\
                    " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
        }
        //Page_check with Multi-NULL
        more_null_idx = rand() % size;
        *(str1_alnd_addr + size -1) = *(str3_alnd_addr + more_null_idx) = *(tmp_alnd_addr + size -1) = NULL_TERM_CHAR;
        test_strcpy((char *)str1_alnd_addr, (char *)tmp_alnd_addr);
        ret = strncat((char *)str1_alnd_addr, (char *)str3_alnd_addr, size);
        if(memcmp(test_strcat_common((char *)tmp_alnd_addr, (char *)str3_alnd_addr, size),(char *)str1_alnd_addr, size) != 0) {
            printf("ERROR: [PAGE-CROSS] MultiNull check failed for size:%zu\n str1:%s\n str2:%s\n str1+str2:%s\n", size, tmp_alnd_addr, str3_alnd_addr,str1_alnd_addr);
        }
        if (ret != str1_alnd_addr) {
            printf("ERROR:[VALIDATION] failure for size: %lu,"\
                    " return_value = %p, expected_value= %p\n", size, ret, str1_alnd_addr);
        }

        cleanup_page_cross_buffer(str2_page_buff, page_cnt);
    }
    free(temp_buff);
    free(str1_buff);
    free(str2_buff);
}

static inline void strstr_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *haystack = SINGLE_CHAR_STRING, *needle = (uint8_t*)NULL_STRING;
    char *res;
    size_t needle_len = 0;

    if (size == 0)
    {
        //Needle is Zero
        res = strstr((char *)haystack, (char *)needle);
        if (res != (char*)haystack)
            printf("ERROR:[RETURN] value mismatch for NEEDLE size(%lu): expected -%p "\
                        ", actual - %p\n", size, (char*)haystack, res);

        //Haystack is Zero
        res = strstr((char *)needle, (char *)haystack);
        if (res != NULL)
            printf("ERROR:[RETURN] value mismatch for HAYSTACK size(%lu): expected -NULL "\
                        ", actual - %p\n", size, res);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + NULL_BYTE, NON_OVERLAP_BUFFER);
    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    haystack = buff_tail + str1_alnmnt;
    //case1: Haystack = SUBSTRINGS(NEEDLE) without the NEEDLE
    needle_len = ceil(sqrt(size));
    needle = buff_head + str2_alnmnt;
    generate_random_string(needle, needle_len);

    string_setup((char *)haystack, size, (char *)needle, needle_len);


    res = strstr((char*)haystack, (char*) needle);
    if (res != test_strstr((char*)haystack, (char*)needle))
    {
        printf("ERROR:[VALIDATION:HAYSTACK = substrings(Needle) without needle]failure for HAYSTACK\
        of str1_aln:%u size:%lu,\nreturn_value(%p):%s\nNEEDLE(%p):%s\nHAYSTACK(%p):%s\n", \
        str1_alnmnt, size, res, res, needle, needle, haystack, haystack);
    }

    //case2: NEEDLE at the END of HAYSTACK
    //Adding Needle at the end of haysatck
    haystack[size - needle_len] = NULL_TERM_CHAR;
    test_strcat_common((char*)haystack, (char*)needle, SIZE_MIN);

    res = strstr((char*)haystack, (char*) needle);
    if (res != test_strstr((char*)haystack, (char*)needle))
    {
        printf("ERROR:[VALIDATION:HAYSTACK = NEEDLE@END]failure for HAYSTACK\
        of str1_aln:%u size:%lu,\nreturn_value(%p):%s\nNEEDLE(%p):%s\nHAYSTACK(%p):%s\n", \
        str1_alnmnt, size, res, res, needle, needle, haystack, haystack);
    }

    //case3: Multi-NEEDLE check
    // Append multiple needles to the haystack (haystack[size/2]=needle)
    for(size_t index = 0; index < needle_len ; index++)
        haystack[size /2 + index] = needle[index];

    res = strstr((char*)haystack, (char*) needle);
    if (res != test_strstr((char*)haystack, (char*)needle))
    {
        printf("ERROR:[VALIDATION:Multi-NEEDLE]failure for HAYSTACK of str1_aln:%u size:%lu,\
            \nreturn_value(%p):%s\nNEEDLE(%p):%s\nHAYSTACK(%p):%s\n",\
             str1_alnmnt, size, res, res, needle, needle, haystack, haystack);
    }

    //case4: NEEDLE bigger than HAYSTACK
    res = strstr((char*)needle, (char*) haystack);
    if (res != test_strstr((char*)needle, (char*)haystack))
    {
        printf("ERROR:[VALIDATION:HAYSTACK = NEEDLE > HAYSTACK]failure for HAYSTACK of \
        str1_aln:%u size:%lu \nreturn_value(%p):%s\nNEEDLE(%p):%s\nHAYSTACK(%p):%s\n", \
        str1_alnmnt, needle_len, res, res, haystack, haystack, needle, needle);
    }

    // Smart page-cross check: Apply to haystack and/or needle if they are candidates
    int haystack_is_candidate = is_page_cross_candidate(size, str1_alnmnt);
    int needle_is_candidate = is_page_cross_candidate(needle_len, str2_alnmnt);

    if (haystack_is_candidate || needle_is_candidate)
    {
        void *haystack_page_buff = NULL, *needle_page_buff = NULL;
        uint32_t haystack_page_cnt = 0, needle_page_cnt = 0;
        uint8_t *page_haystack = haystack, *page_needle = needle;

        // Setup haystack page buffer if it's a candidate
        if (haystack_is_candidate)
        {
            haystack_page_cnt = PAGE_CNT(size);
            alloc_page_cross_buffer(&haystack_page_buff, haystack_page_cnt, buff);
            page_haystack = (uint8_t *)haystack_page_buff + haystack_page_cnt * PAGE_SZ - (size + NULL_BYTE + str1_alnmnt);
        }

        // Setup needle page buffer if it's a candidate
        if (needle_is_candidate)
        {
            needle_page_cnt = PAGE_CNT(needle_len);
            if (posix_memalign(&needle_page_buff, PAGE_SZ, (needle_page_cnt + 1) * PAGE_SZ) != 0)
            {
                printf("[ERROR] Failed to allocate needle page buffer\n");
                if (haystack_page_buff)
                    cleanup_page_cross_buffer(haystack_page_buff, haystack_page_cnt);
                free(buff);
                exit(-1);
            }
            if (convert_page_to_inaccessible(needle_page_buff, needle_page_cnt) != 0)
            {
                free(needle_page_buff);
                if (haystack_page_buff)
                    cleanup_page_cross_buffer(haystack_page_buff, haystack_page_cnt);
                free(buff);
                exit(-1);
            }
            page_needle = (uint8_t *)needle_page_buff + needle_page_cnt * PAGE_SZ - (needle_len + NULL_BYTE + str2_alnmnt);
            // Copy original needle content to page buffer and ensure NULL termination
            test_strcpy((char*)page_needle, (char*)needle);
        }

        // Test case 1: Haystack = substrings(needle) without the needle
        string_setup((char *)page_haystack, size, (char *)page_needle, needle_len);
        res = strstr((char*)page_haystack, (char*)page_needle);
        if (res != test_strstr((char*)page_haystack, (char*)page_needle))
        {
            printf("ERROR:[PAGE-CROSS:HAYSTACK = substrings(Needle)] failure for \
            str1_aln:%u str2_aln:%u size:%lu,\nreturn_value(%p):%s\nNEEDLE(%p):%s\nHAYSTACK(%p):%s\n",\
            str1_alnmnt, str2_alnmnt, size, res, res, page_needle, page_needle, page_haystack, page_haystack);
        }

        //Test case 2: Adding needle at the end of haystack
        page_haystack[size - needle_len] = NULL_TERM_CHAR;
        strncat((char*)page_haystack, (char*)page_needle, needle_len);
        res = strstr((char*)page_haystack, (char*)page_needle);
        if (res != test_strstr((char*)page_haystack, (char*)page_needle))
        {
            printf("ERROR:[PAGE-CROSS:Needle at the end] failure for \
            str1_aln:%u str2_aln:%u size:%lu,\nreturn_value(%p):%s\nNEEDLE(%p):%s\nHAYSTACK(%p):%s\n",\
            str1_alnmnt, str2_alnmnt, size, res, res, page_needle, page_needle, page_haystack, page_haystack);
        }

        // Cleanup buffers
        if (haystack_page_buff)
            cleanup_page_cross_buffer(haystack_page_buff, haystack_page_cnt);
        if (needle_page_buff)
            cleanup_page_cross_buffer(needle_page_buff, needle_page_cnt);
    }
    free(buff);
}

static inline void strspn_validator(size_t size, uint32_t str2_alnmnt,\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *s = SINGLE_CHAR_STRING, *accept = (uint8_t*)NULL_STRING, *page_alnd_addr = NULL;
    size_t res, expected;
    size_t accept_len = 0;



    if (size == 0)
    {
        //accept is Zero
        res = strspn((char *)s, (char*)accept);
        if (res != 0)
            printf("ERROR:[RETURN] value mismatch for ACCEPT size(%lu): expected - 0 "\
                        ", actual - %lu\n", size, res);

        //S is Zero
        res = strspn((char*)accept, (char *)s);
        if (res != 0)
            printf("ERROR:[RETURN] value mismatch for S size(%lu): expected - 0 "\
                        ", actual - %lu\n", size, res);
        return;
    }

    buff = alloc_buffer(&buff_head, &buff_tail, size + NULL_BYTE, NON_OVERLAP_BUFFER);
    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }
    s = buff_tail + str1_alnmnt;
    //case 1 : Accept in S+
    accept_len = ceil(sqrt(size));
    accept = buff_head + str2_alnmnt;
    generate_random_string(accept, accept_len);
    string_setup((char*) s, size, (char *)accept, accept_len);
    res = strspn((char*)s, (char*)accept);
    expected = test_strspn((char*)s, (char*)accept);
    if (res != expected)
    {
        printf("ERROR:[VALIDATION: substrings of ACCEPT in S]failure for S\
        of str1_aln:%u size:%lu,\nreturn_value:%lu,\nACCEPT(%p):%s\nS(%p):%s\n", \
        str1_alnmnt, size, res, accept, accept, s, s);
    }
    //case 2 : Replacing S with char not in accept
    char original_char;
    for (size_t i = 0; i < size; i++) {
        original_char = s[size - 1 - i];
        s[size - 1 - i] = (char)127;
        res = strspn((char*)s, (char*)accept);
        expected = test_strspn((char*)s, (char*)accept);
        if (res != expected)
        {
            printf("ERROR:[VALIDATION: Failure at Index : %lu]failure for S\
            of str1_aln:%u size:%lu,\nreturn_value:%lu,\nACCEPT(%p):%s\nS(%p):%s\n", \
            size-1-i, str1_alnmnt, size, res, accept, accept, s, s);
        }
        // Restore the original character
        s[size - 1 - i] = original_char;
    }

    //case - 3 : Replacing S with Permutations of accept
    size_t index = accept_len;
    while (index < size) {
        if (accept_len > 1) {
            size_t i;
            for (i = 0; i < accept_len - 1; i++) {
                size_t j = i + rand() / (RAND_MAX / (accept_len - i) + 1);
                char t = accept[j];
                accept[j] = accept[i];
                accept[i] = t;
            }
            for (size_t j = 0; j < accept_len && index < size; j++, index++) {
                s[index] = accept[j];
            }
        }
    }
    res = strspn((char*)s, (char*)accept);
    expected = test_strspn((char*)s, (char*)accept);
    if (res != expected)
    {
        printf("ERROR:[VALIDATION: Generating S with permutations of ACCEPT]failure for S\
        of str1_aln:%u size:%lu,\nreturn_value:%lu\nACCEPT(%p):%s\nS(%p):%s\n", \
        str1_alnmnt, size, res, accept, accept, s, s);
    }

    //case 4 : PageCross_check for size <= 4KB
    if  (size <= PAGE_SZ)
    {
        void *page_buff = NULL;
        uint32_t page_cnt = (size + NULL_BYTE + CACHE_LINE_SZ)/PAGE_SZ + \
                        !!((size + NULL_BYTE + CACHE_LINE_SZ) % PAGE_SZ);
        posix_memalign(&page_buff, PAGE_SZ, page_cnt * PAGE_SZ);

        if (page_buff == NULL)
        {
            printf("[ERROR] Failed to allocate memory\n");
            free(buff);
            exit(-1);
        }
        //Accept in page_alnd_addr
        page_alnd_addr = (uint8_t *)page_buff + page_cnt * PAGE_SZ - (size + NULL_BYTE + str1_alnmnt);
        string_setup((char*) page_alnd_addr, size, (char *)accept, accept_len);
        res = strspn((char*)page_alnd_addr, (char*)accept);
        expected = test_strspn((char*)page_alnd_addr, (char*)accept);
        if (res != expected)
        {
            printf("ERROR:[VALIDATION: substrings of ACCEPT in page_alnd_addr]failure for page_alnd_addr\
            of str1_aln:%u size:%lu,\nreturn_value:%lu,\nACCEPT(%p):%s\nS(%p):%s\n", \
            str1_alnmnt, size, res, accept, accept, page_alnd_addr, page_alnd_addr);
        }
        //Replacing S with char not in accept
        char orig_char;
        for (size_t i = 0; i < size; i++) {
            orig_char = page_alnd_addr[size - 1 - i];
            page_alnd_addr[size - 1 - i] = (char)127;
            res = strspn((char*)page_alnd_addr, (char*)accept);
            expected = test_strspn((char*)page_alnd_addr, (char*)accept);
            if (res != expected)
            {
                printf("ERROR:[VALIDATION: Failure at Index : %lu]failure for page_alnd_addr\
                of str1_aln:%u size:%lu,\nreturn_value:%lu,\nACCEPT(%p):%s\nS(%p):%s\n", \
                size-1-i, str1_alnmnt, size, res, accept, accept, page_alnd_addr, page_alnd_addr);
            }
            // Restore the original character
            page_alnd_addr[size - 1 - i] = orig_char;
        }
        free(page_buff);
    }

    // case 5 : Accept near page boundary
    //  Place accept so it straddles a page boundary tests masked load
    //  handling in the accept loading path.
    if (size <= PAGE_SZ && accept_len >= 2)
    {
        void *page_buff_accept = NULL;
        uint32_t page_cnt_a = 2; // two pages
        posix_memalign(&page_buff_accept, PAGE_SZ, page_cnt_a * PAGE_SZ);

        if (page_buff_accept != NULL)
        {
            size_t offsets[] = {1, 2, 5, 9, 13, 15};
            size_t num_offsets = sizeof(offsets) / sizeof(offsets[0]);

            for (size_t oi = 0; oi < num_offsets; oi++)
            {
                size_t offset = offsets[oi];
                if (offset > accept_len)
                    continue;

                uint8_t *accept_pg = (uint8_t *)page_buff_accept + PAGE_SZ - offset;
                memcpy(accept_pg, accept, accept_len);
                accept_pg[accept_len] = '\0';

                string_setup((char *)s, size, (char *)accept_pg, accept_len);
                res = strspn((char *)s, (char *)accept_pg);
                expected = test_strspn((char *)s, (char *)accept_pg);
                if (res != expected)
                {
                    printf("ERROR:[ACCEPT PAGE-CROSS offset=%lu]failure for S\
            of str1_aln:%u size:%lu,\nreturn_value:%lu, expected:%lu,\nACCEPT(%p):%s\nS(%p):%s\n",
                           offset, str1_alnmnt, size, res, expected, accept_pg, accept_pg, s, s);
                }

                if (size > 1)
                {
                    size_t mid = size / 2;
                    char orig = s[mid];
                    s[mid] = (char)127;
                    res = strspn((char *)s, (char *)accept_pg);
                    expected = test_strspn((char *)s, (char *)accept_pg);
                    if (res != expected)
                    {
                        printf("ERROR:[ACCEPT PAGE-CROSS mismatch offset=%lu idx=%lu]failure\
            of str1_aln:%u size:%lu,\nreturn_value:%lu, expected:%lu,\nACCEPT(%p):%s\nS(%p):%s\n",
                               offset, mid, str1_alnmnt, size, res, expected, accept_pg, accept_pg, s, s);
                    }
                    s[mid] = orig;
                }
            }
            free(page_buff_accept);
        }
    }

    // case 6 : Both s AND accept near page boundaries
    if (size <= PAGE_SZ && accept_len >= 2)
    {
        void *page_buff_both = NULL;
        posix_memalign(&page_buff_both, PAGE_SZ, 4 * PAGE_SZ);

        if (page_buff_both != NULL)
        {
            size_t a_off = (accept_len < 9) ? accept_len : 9;
            uint8_t *accept_pg = (uint8_t *)page_buff_both + PAGE_SZ - a_off;
            memcpy(accept_pg, accept, accept_len);
            accept_pg[accept_len] = '\0';

            uint8_t *s_pg = (uint8_t *)page_buff_both + 3 * PAGE_SZ - (size + NULL_BYTE);
            string_setup((char *)s_pg, size, (char *)accept_pg, accept_len);

            res = strspn((char *)s_pg, (char *)accept_pg);
            expected = test_strspn((char *)s_pg, (char *)accept_pg);
            if (res != expected)
            {
                printf("ERROR:[BOTH PAGE-CROSS]failure\
            of str1_aln:%u size:%lu,\nreturn_value:%lu, expected:%lu,\nACCEPT(%p):%s\nS(%p):%s\n",
                       str1_alnmnt, size, res, expected, accept_pg, accept_pg, s_pg, s_pg);
            }
            free(page_buff_both);
        }
    }

    free(buff);
}

static inline void strchr_validator(size_t size, uint32_t str2_alnmnt __attribute__((unused)),\
                                                 uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str_alnd_addr = NULL;
    size_t index;
    char find, not_found;
    char* res;

    buff = alloc_buffer(&buff_head, &buff_tail, size + NULL_BYTE + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }

    str_alnd_addr = buff_tail + str1_alnmnt;
    prepare_boundary(str_alnd_addr, size);

    if (size == 0)
    {
        *str_alnd_addr = NULL_TERM_CHAR;
        if(strchr((char*)str_alnd_addr,'#') != NULL)
        {
            printf("ERROR:[VALIDATION] failure for size 0 ");
        }
        free(buff);
        return;

    }

    //Intialize_buffer with all possible chars
    for (index = 0; index < size; index++)
    {
        *(str_alnd_addr + index) = random_char();
    }
    //Appending Null character at the end of str
    *(str_alnd_addr + size) = NULL_TERM_CHAR;

    //index of the char to be found
    if (size == 1)
        find = *(str_alnd_addr);
    else
        find =  str_alnd_addr[rand() % (size - 1)];

    res = strchr((char*)str_alnd_addr,(int)find);

    if ((test_strchr((char*)str_alnd_addr,(int)find)) != (char*)res)
   {
        printf("ERROR:[VALIDATION (MATCH)] failure for str1_aln:%u size: %lu,"\
                " return_value = %s, EXP= %s\n",str1_alnmnt, size, res, \
                test_strchr((char*)str_alnd_addr,(int)find));
    }

    //Removing the char from string
    if (size == 1)
        find = *(str_alnd_addr);
    else
        find =  str_alnd_addr[rand() % (size - 1)];
    for (index = 0; index < size; index++)
    {
        if (*(str_alnd_addr + index) == find)
        {
            do
            {
                not_found = random_char();
            } while (find == not_found);
            *(str_alnd_addr + index) = not_found;
        }
    }

    res = strchr((char*)str_alnd_addr,(int)find);
    if(res != NULL)
    {
        printf("ERROR:[VALIDATION (NON-MATCH)] failure for str1_aln:%u size: %lu,"\
                " return_value = %s, EXP= NULL\n",str1_alnmnt, size, res);
    }
    //Match at END case
    if (size == 1)
        find = *(str_alnd_addr);
    else
        find = *(str_alnd_addr + size - NULL_BYTE);

    res = strchr((char*)str_alnd_addr,(int)find);

    if(test_strchr((char*)str_alnd_addr,(int)find) != (char*)res)
    {
            printf("ERROR:[VALIDATION (MATCH:END)] failure for str1_aln:%u size: %lu,"\
                    " return_value = %s, EXP= %s\n",str1_alnmnt, size, res, \
                    test_strchr((char*)str_alnd_addr,(int)find));
    }

    // Robust page-cross check: Only for good candidates
    if (is_page_cross_candidate(size, str1_alnmnt)) {
        void *page_buff = NULL;
        uint32_t page_cnt = PAGE_CNT(size);

        alloc_page_cross_buffer(&page_buff, page_cnt, buff);

        str_alnd_addr = (uint8_t *)page_buff + page_cnt * PAGE_SZ - (size + NULL_BYTE + str1_alnmnt);
        for (index = 0; index < size; index++) {
            *(str_alnd_addr + index) = random_char();
        }
        *(str_alnd_addr + size ) = NULL_TERM_CHAR;
        if (size == 1)
            find = *(str_alnd_addr);
        else
            find =  str_alnd_addr[rand() % (size - 1)];
        res = strchr((char*)str_alnd_addr,(int)find);
        if ((test_strchr((char*)str_alnd_addr,(int)find)) != (char*)res) {
            printf("ERROR:[PAGE-CROSS (MATCH)] failure for str1_aln:%u size: %lu,"\
                    " return_value = %s, EXP= %s\n",str1_alnmnt, size, res, \
                    test_strchr((char*)str_alnd_addr,(int)find));
        }
        if (size == 1)
            find = *(str_alnd_addr);
        else
            find =  str_alnd_addr[rand() % (size - 1)];
        for (index = 0; index < size; index++) {
            if (*(str_alnd_addr + index) == find) {
                do {
                    not_found = random_char();
                } while (find == not_found);
                *(str_alnd_addr + index) = not_found;
            }
        }
        res = strchr((char*)str_alnd_addr,(int)find);
        if(res != NULL) {
            printf("ERROR:[PAGE-CROSS (NON-MATCH)] failure for str1_aln:%u size: %lu,"\
                                        " return_value = %s, EXP= NULL\n",str1_alnmnt, size, res);
        }
        if (size == 1)
            find = *(str_alnd_addr);
        else
            find = *(str_alnd_addr + size - NULL_BYTE);
        res = strchr((char*)str_alnd_addr,(int)find);
        if(test_strchr((char*)str_alnd_addr,(int)find) != (char*)res) {
            printf("ERROR:[PAGE-CROSS (MATCH:END)] failure for str1_aln:%u size: %lu,"\
                        " return_value = %s, EXP= %s\n",str1_alnmnt, size, res, \
                        test_strchr((char*)str_alnd_addr,(int)find));
        }

        cleanup_page_cross_buffer(page_buff, page_cnt);
    }
    free(buff);
}

static inline void strrchr_validator(size_t size, uint32_t str2_alnmnt __attribute__((unused)),\
                                                  uint32_t str1_alnmnt)
{
    uint8_t *buff = NULL, *buff_head, *buff_tail;
    uint8_t *str_alnd_addr = NULL;
    size_t index;
    char find, not_found;
    char *res;

    buff = alloc_buffer(&buff_head, &buff_tail, size + NULL_BYTE + BOUNDARY_BYTES, NON_OVERLAP_BUFFER);

    if (buff == NULL)
    {
        printf("[ERROR] Failed to allocate memory\n");
        exit(-1);
    }

    str_alnd_addr = buff_tail + str1_alnmnt;
    prepare_boundary(str_alnd_addr, size);

    if (size == 0)
    {
        *str_alnd_addr = NULL_TERM_CHAR;
        if (strrchr((char *)str_alnd_addr, '#') != NULL)
        {
            printf("ERROR:[VALIDATION] failure for size 0 ");
        }
        free(buff);
        return;
    }

    for (index = 0; index < size; index++)
    {
        *(str_alnd_addr + index) = random_char();
    }
    *(str_alnd_addr + size) = NULL_TERM_CHAR;

    if (size == 1)
        find = *(str_alnd_addr);
    else
        find = str_alnd_addr[rand() % (size - 1)];

    res = strrchr((char *)str_alnd_addr, (int)find);
    if (test_strrchr((char *)str_alnd_addr, (int)find) != (char *)res)
    {
        printf("ERROR:[VALIDATION (MATCH)] failure for str1_aln:%u size: %lu,"\
                " return_value = %p, EXP= %p\n", str1_alnmnt, size, (void *)res,\
                (void *)test_strrchr((char *)str_alnd_addr, (int)find));
    }

    if (size > 2)
    {
        find = '!';
        for (index = 0; index < size; index++)
        {
            if (*(str_alnd_addr + index) == find)
            {
                do
                {
                    not_found = random_char();
                } while (not_found == find);
                *(str_alnd_addr + index) = not_found;
            }
        }
        *(str_alnd_addr + 0) = find;
        *(str_alnd_addr + size / 2) = find;
        res = strrchr((char *)str_alnd_addr, (int)find);
        if (res != (char *)(str_alnd_addr + size / 2))
        {
            printf("ERROR:[VALIDATION (LAST-MATCH)] failure for str1_aln:%u size: %lu,"\
                    " return_value = %p, EXP= %p\n", str1_alnmnt, size, res, str_alnd_addr + size / 2);
        }
    }

    res = strrchr((char *)str_alnd_addr, '\0');
    if (res != (char *)(str_alnd_addr + size))
    {
        printf("ERROR:[VALIDATION (NULL-CHAR)] failure for str1_aln:%u size: %lu,"\
                " return_value = %p, EXP= %p\n", str1_alnmnt, size, res, str_alnd_addr + size);
    }

    if (size == 1)
        find = *(str_alnd_addr);
    else
        find = str_alnd_addr[rand() % (size - 1)];
    for (index = 0; index < size; index++)
    {
        if (*(str_alnd_addr + index) == find)
        {
            do
            {
                not_found = random_char();
            } while (find == not_found);
            *(str_alnd_addr + index) = not_found;
        }
    }

    res = strrchr((char *)str_alnd_addr, (int)find);
    if (res != NULL)
    {
        printf("ERROR:[VALIDATION (NON-MATCH)] failure for str1_aln:%u size: %lu,"\
                " return_value = %s, EXP= NULL\n", str1_alnmnt, size, res);
    }

    if (size == 1)
        find = *(str_alnd_addr);
    else
        find = *(str_alnd_addr + size - NULL_BYTE);
    res = strrchr((char *)str_alnd_addr, (int)find);
    if (test_strrchr((char *)str_alnd_addr, (int)find) != (char *)res)
    {
        printf("ERROR:[VALIDATION (MATCH:END)] failure for str1_aln:%u size: %lu,"\
                " return_value = %p, EXP= %p\n", str1_alnmnt, size, (void *)res,\
                (void *)test_strrchr((char *)str_alnd_addr, (int)find));
    }

    if (is_page_cross_candidate(size, str1_alnmnt)) {
        void *page_buff = NULL;
        uint32_t page_cnt = PAGE_CNT(size);

        alloc_page_cross_buffer(&page_buff, page_cnt, buff);

        str_alnd_addr = (uint8_t *)page_buff + page_cnt * PAGE_SZ - (size + NULL_BYTE + str1_alnmnt);
        for (index = 0; index < size; index++) {
            *(str_alnd_addr + index) = random_char();
        }
        *(str_alnd_addr + size) = NULL_TERM_CHAR;

        if (size == 1)
            find = *(str_alnd_addr);
        else
            find = str_alnd_addr[rand() % (size - 1)];
        res = strrchr((char *)str_alnd_addr, (int)find);
        if (test_strrchr((char *)str_alnd_addr, (int)find) != (char *)res) {
            printf("ERROR:[PAGE-CROSS (MATCH)] failure for str1_aln:%u size: %lu,"\
                    " return_value = %p, EXP= %p\n", str1_alnmnt, size, (void *)res,\
                    (void *)test_strrchr((char *)str_alnd_addr, (int)find));
        }

        res = strrchr((char *)str_alnd_addr, '\0');
        if (res != (char *)(str_alnd_addr + size)) {
            printf("ERROR:[PAGE-CROSS (NULL-CHAR)] failure for str1_aln:%u size: %lu,"\
                    " return_value = %p, EXP= %p\n", str1_alnmnt, size, res, str_alnd_addr + size);
        }

        cleanup_page_cross_buffer(page_buff, page_cnt);
    }
    free(buff);
}

libmem_func supp_funcs[]=
{
    {"memcpy",  memcpy_validator},
    {"mempcpy", mempcpy_validator},
    {"memmove", memmove_validator},
    {"memset",  memset_validator},
    {"memcmp",  memcmp_validator},
    {"memchr",  memchr_validator},
    {"strcpy",  strcpy_validator},
    {"strncpy", strncpy_validator},
    {"strcmp",  strcmp_validator},
    {"strncmp", strncmp_validator},
    {"strlen",  strlen_validator},
    {"strnlen", strnlen_validator},
    {"strcat",  strcat_validator},
    {"strncat", strncat_validator},
    {"strstr",  strstr_validator},
    {"strspn",  strspn_validator},
    {"strchr",  strchr_validator},
    {"strrchr", strrchr_validator},
    {"none",    NULL}
};


int main(int argc, char **argv)
{
    srand((unsigned int)time(NULL));
    uint64_t size;
    char *ptr;
    unsigned int src_alignment = 0, dst_alignment = 0;
    libmem_func *lm_func_validator = &supp_funcs[0]; //default func is memcpy

    int al_check = 0;

    if (argc < 6)
    {
        if (argv[1] == NULL)
        {
            printf("ERROR: Function name not provided\n");
            exit(0);
        }
        if (argv[2] == NULL)
        {
            printf("ERROR: Size not provided\n");
            exit(0);
        }
    }

    for (uint16_t index = 0; index < (uint16_t)(sizeof(supp_funcs)/sizeof(supp_funcs[0])); index++)
    {
        if (!test_strcmp(supp_funcs[index].func_name, argv[1]))
        {
            lm_func_validator = &supp_funcs[index];
            break;
        }
    }

    // Size accepts "N" or a range "START:END[:STEP]" looped below.
    uint64_t range_start, range_end, range_step = 1;
    char *colon = strchr(argv[2], ':');
    if (colon == NULL)
    {
        range_start = strtoul(argv[2], &ptr, 10);
        range_end = range_start;
    }
    else
    {
        range_start = strtoul(argv[2], &ptr, 10);
        range_end = strtoul(colon + 1, &ptr, 10);
        char *colon2 = strchr(colon + 1, ':');
        if (colon2 != NULL)
            range_step = strtoul(colon2 + 1, &ptr, 10);
    }
    if (range_step == 0)
        range_step = 1;
    if (range_end < range_start)
        range_end = range_start;

    if (argv[3] != NULL)
        src_alignment = atoi(argv[3]) % VEC_SZ;

    if (argv[4] != NULL)
        dst_alignment = atoi(argv[4]) % VEC_SZ;

    if (argv[5] != NULL)
        al_check = atoi(argv[5]); //Check for alignment validation test.

#ifdef LIBMEM_VALIDATOR_DEBUG
    printf("[DEBUG] libmem_validator started\n");
    printf("[DEBUG] VEC_SZ = %d bytes\n", VEC_SZ);
    printf("[DEBUG] Function: %s\n", lm_func_validator->func_name);
    printf("[DEBUG] Size range: %lu..%lu (step %lu)\n", range_start, range_end, range_step);
    printf("[DEBUG] Alignment check mode: %s\n", al_check ? "All alignments" : "Single test");
#endif

    // Copy-pasteable rerun command; range_start is only an example size.
    // Substitute the size reported in the failing validation ERROR line.
    if (range_start != range_end)
    {
        const char *ld_preload = getenv("LD_PRELOAD");
        if (ld_preload == NULL)
            ld_preload = "";
        printf("[INFO] To debug a single size, rerun e.g.: "
               "LD_PRELOAD=%s %s %s %lu %u %u%s\n",
               ld_preload, argv[0], lm_func_validator->func_name, range_start,
               src_alignment, dst_alignment, al_check ? " 1" : "");
    }

    for (size = range_start; size <= range_end; size += range_step)
    {
        if (al_check == 0)
        {
            lm_func_validator->func(size, dst_alignment, src_alignment);
        }

        else
        {
            for(unsigned int aln_src  = 0; aln_src < VEC_SZ; aln_src++)
            {
                for(unsigned int aln_dst = 0; aln_dst < VEC_SZ; aln_dst++)
                {
#ifdef LIBMEM_VALIDATOR_DEBUG
                    printf("[DEBUG] Testing alignment - src: %d, dst: %d\n", aln_src, aln_dst);
#endif
                    lm_func_validator->func(size, aln_dst, aln_src);
                }
            }
        }
    }

    return 0;
}
