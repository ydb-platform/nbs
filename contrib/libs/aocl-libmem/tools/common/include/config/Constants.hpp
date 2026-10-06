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

#ifndef LIBMEM_COMMON_CONSTANTS_HPP
#define LIBMEM_COMMON_CONSTANTS_HPP

#include <cstddef>
#include <cstdint>

namespace libmem {
namespace common {

// Memory and alignment constants
constexpr size_t CACHE_LINE_SZ = 64;
constexpr size_t BOUNDARY_BYTES = 8;
constexpr size_t PAGE_SZ = 4096;
constexpr size_t XMM_SZ = 16;
constexpr size_t YMM_SZ = 32;
constexpr size_t ZMM_SZ = 64;

// Cacheline spill offset (used by benchmark alignment modes)
constexpr size_t CL_SPILL_OFFSET = 8;
constexpr size_t CACHELINE_MULTIPLE = 6;

// String constants
constexpr char NULL_TERM_CHAR = '\0';
constexpr const char* NULL_STRING = "";
constexpr uint8_t SINGLE_CHAR = 'A';

// ASCII character range
constexpr uint8_t MIN_PRINTABLE_ASCII = 32;
constexpr uint8_t MAX_PRINTABLE_ASCII = 126;

// Size constants
constexpr size_t NULL_BYTE = 1;
constexpr size_t LOWER_CHARS = 26;
constexpr size_t SIZE_MIN = 0;

// Hard cap on per-benchmark size. Set by CMake; #ifndef is the fallback.
#ifndef LIBMEM_MAX_BENCHMARK_SIZE_BYTES
#define LIBMEM_MAX_BENCHMARK_SIZE_BYTES (1ULL << 30)   // 1 GiB
#endif
constexpr size_t MAX_BENCHMARK_SIZE = LIBMEM_MAX_BENCHMARK_SIZE_BYTES;

// Vector size based on AVX feature
#ifdef AVX512_FEATURE_ENABLED
    constexpr size_t VEC_SZ = ZMM_SZ;
#else
    constexpr size_t VEC_SZ = YMM_SZ;
#endif

inline constexpr size_t PAGE_CNT(size_t size) {
    return (size + NULL_BYTE + CACHE_LINE_SZ) / PAGE_SZ +
           !!((size + NULL_BYTE + CACHE_LINE_SZ) % PAGE_SZ);
}

inline constexpr size_t NO_VECS(size_t size) {
    return size / VEC_SZ + !!(size % VEC_SZ);
}

} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_CONSTANTS_HPP
