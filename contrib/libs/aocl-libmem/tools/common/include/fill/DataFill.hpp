/* Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
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

#ifndef LIBMEM_COMMON_DATA_FILL_HPP
#define LIBMEM_COMMON_DATA_FILL_HPP

/**
 * @file DataFill.hpp
 * @brief Free functions for buffer data initialization, shared by validator and benchmark.
 */

#include "config/Constants.hpp"
#include "fill/RandomPool.hpp"
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cmath>

namespace libmem {
namespace common {

enum class MatchPosition {
    BEGIN,
    MID,
    END,
    NO_MATCH
};

// Bulk fillers route through globalRandomPool() (xoshiro256**, 8 B/step).

inline void fillLowercaseLetters(uint8_t* buf, size_t size) {
    globalRandomPool().fill_lowercase(buf, size);
}

inline void fillWithByte(uint8_t* buf, size_t size, uint8_t value) {
    std::memset(buf, value, size);
}

/**
 * @param avoid_null If true, replace 0 with non-zero value (default: true)
 */
inline void fillFullByteRange(uint8_t* buf, size_t size, bool avoid_null = true) {
    if (avoid_null) {
        globalRandomPool().fill_full_range_non_zero(buf, size);
    } else {
        globalRandomPool().fill_bytes(buf, size);
    }
}

inline void fillNonNull(uint8_t* buf, size_t size) {
    globalRandomPool().fill_non_null(buf, size);
}

inline void shuffleChars(char* buf, int len) {
    RandomPool& rp = globalRandomPool();
    for (int i = len - 1; i > 0; --i) {
        uint32_t j = rp.uniform_u32(static_cast<uint32_t>(i + 1));
        char t = buf[i];
        buf[i] = buf[j];
        buf[j] = t;
    }
}

inline void nullTerminate(uint8_t* buf, size_t pos) {
    buf[pos] = '\0';
}

inline void copyData(uint8_t* dst, const uint8_t* src, size_t size) {
    std::memcpy(dst, src, size);
}

/**
 * Fill haystack and needle for strstr benchmarking at the given match position.
 * Needle length is sqrt(size). BEGIN/MID/END control where the needle appears;
 * NO_MATCH fills the haystack with characters that cannot form the needle.
 *
 * @param haystack Buffer for the haystack string (at least size bytes)
 * @param needle   Buffer for the needle string (at least sqrt(size)+1 bytes)
 * @param size     Total haystack size including null terminator
 * @param pos      Where to place the needle in the haystack
 */
inline void fillStrstrPos(uint8_t* haystack, uint8_t* needle, size_t size,
                           MatchPosition pos) {
    if (size == 0) return;
    if (size < 4) {
        fillWithByte(haystack, size, 'x');
        haystack[size - 1] = '\0';
        needle[0] = 'x';
        needle[1] = '\0';
        return;
    }

    size_t needle_len = static_cast<size_t>(
        std::ceil(std::sqrt(static_cast<double>(size))));
    if (needle_len < 2) needle_len = 2;
    if (needle_len >= size) needle_len = size / 2;

    for (size_t i = 0; i < needle_len; ++i)
        needle[i] = static_cast<uint8_t>(
            MIN_PRINTABLE_ASCII + (i % (MAX_PRINTABLE_ASCII - MIN_PRINTABLE_ASCII)));
    needle[needle_len] = '\0';

    switch (pos) {
    case MatchPosition::BEGIN: {
        std::memcpy(haystack, needle, needle_len);
        for (size_t i = needle_len; i < size - 1; ++i)
            haystack[i] = static_cast<uint8_t>('A' + (i % LOWER_CHARS));
        haystack[size - 1] = '\0';
        break;
    }
    case MatchPosition::MID: {
        size_t mid = (size - needle_len - 1) / 2;
        for (size_t i = 0; i < mid; ++i)
            haystack[i] = static_cast<uint8_t>('A' + (i % LOWER_CHARS));
        std::memcpy(haystack + mid, needle, needle_len);
        for (size_t i = mid + needle_len; i < size - 1; ++i)
            haystack[i] = static_cast<uint8_t>('A' + (i % LOWER_CHARS));
        haystack[size - 1] = '\0';
        break;
    }
    case MatchPosition::END: {
        size_t p = 0;
        for (size_t i = 1; i < needle_len && p + i + 1 < size - needle_len; ++i) {
            std::memcpy(haystack + p, needle, i);
            p += i;
            if (p < size - needle_len - 1) {
                haystack[p] = static_cast<uint8_t>('A' + (p % LOWER_CHARS));
                ++p;
            }
        }
        while (p + needle_len < size - 1) {
            haystack[p] = static_cast<uint8_t>('A' + (p % LOWER_CHARS));
            ++p;
        }
        std::memcpy(haystack + p, needle, needle_len);
        p += needle_len;
        haystack[p] = '\0';
        break;
    }
    case MatchPosition::NO_MATCH: {
        RandomPool& rp = globalRandomPool();
        for (size_t i = 0; i < size - 1; ++i)
            haystack[i] = static_cast<uint8_t>('A' + rp.uniform_u32(LOWER_CHARS));
        haystack[size - 1] = '\0';
        break;
    }
    }
}

/**
 * Fill buffer for strchr/memchr benchmarking with target at the given position.
 * Background is lowercase a-z (cannot contain the uppercase target 'X').
 *
 * @param buf       Buffer to fill
 * @param size      Buffer size
 * @param target    Target byte to search for (e.g. 'X')
 * @param is_string If true, null-terminate at buf[size-1]
 * @param pos       Where to place the target byte
 */
inline void fillSearchCharPos(uint8_t* buf, size_t size, uint8_t target,
                               bool is_string, MatchPosition pos) {
    if (size == 0) return;

    fillLowercaseLetters(buf, size);
    if (is_string)
        buf[size - 1] = '\0';

    switch (pos) {
    case MatchPosition::BEGIN:
        buf[0] = target;
        break;
    case MatchPosition::MID:
        buf[size / 2] = target;
        break;
    case MatchPosition::END:
        if (is_string && size > 1)
            buf[size - 2] = target;
        else
            buf[size - 1] = target;
        break;
    case MatchPosition::NO_MATCH:
        break;
    }
}

/**
 * Fill two buffers for strcmp/memcmp benchmarking with mismatch at the given position.
 * Both buffers start equal (lowercase letters); a single-byte mismatch is introduced
 * at the specified position. NO_MATCH means buffers remain fully equal.
 *
 * @param buf1      First comparison buffer (filled with lowercase letters)
 * @param buf2      Second comparison buffer (copy of buf1, then mismatch added)
 * @param size      Buffer size
 * @param is_string If true, null-terminate both buffers at size-1
 * @param pos       Where to introduce the mismatch
 */
inline void fillComparePos(uint8_t* buf1, uint8_t* buf2, size_t size,
                            bool is_string, MatchPosition pos) {
    if (size == 0) return;

    fillLowercaseLetters(buf1, size);
    if (is_string)
        buf1[size - 1] = '\0';
    std::memcpy(buf2, buf1, size);

    if (pos == MatchPosition::NO_MATCH)
        return;

    size_t idx = 0;
    switch (pos) {
    case MatchPosition::BEGIN:
        idx = 0;
        break;
    case MatchPosition::MID:
        idx = size / 2;
        break;
    case MatchPosition::END:
        idx = is_string ? (size > 2 ? size - 2 : 0) : (size - 1);
        break;
    default:
        break;
    }
    buf2[idx] = (buf1[idx] < 'z') ? (buf1[idx] + 1) : (buf1[idx] - 1);
}

/**
 * Fill string (dst) and accept set (src) for strspn benchmarking.
 * Creates an accept set of length sqrt(size) and a string composed
 * of substrings and permutations of accept, matching legacy behavior.
 *
 * @param str     Buffer for the test string (at least size bytes)
 * @param accept  Buffer for the accept set (at least sqrt(size)+1 bytes)
 * @param size    Total string size including null terminator
 */
inline void fillStrspn(uint8_t* str, uint8_t* accept, size_t size) {
    if (size == 0) return;
    if (size < 4) {
        str[0] = 'a';
        str[size > 1 ? size - 1 : 0] = '\0';
        accept[0] = 'a';
        accept[1] = '\0';
        return;
    }

    size_t accept_len = static_cast<size_t>(
        std::ceil(std::sqrt(static_cast<double>(size))));
    if (accept_len < 2) accept_len = 2;
    if (accept_len >= size) accept_len = size / 2;

    for (size_t i = 0; i < accept_len; ++i)
        accept[i] = static_cast<uint8_t>(
            MIN_PRINTABLE_ASCII + (i % (MAX_PRINTABLE_ASCII - MIN_PRINTABLE_ASCII)));
    accept[accept_len] = '\0';

    size_t pos = 0;

    for (size_t i = 1; i < accept_len && pos + i < size - 1; ++i) {
        std::memcpy(str + pos, accept, i);
        pos += i;
    }

    while (pos + accept_len < size - 1) {
        std::memcpy(str + pos, accept, accept_len);
        pos += accept_len;
    }

    while (pos < size - 1) {
        str[pos] = accept[pos % accept_len];
        pos++;
    }
    str[pos] = '\0';
}

} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_DATA_FILL_HPP
