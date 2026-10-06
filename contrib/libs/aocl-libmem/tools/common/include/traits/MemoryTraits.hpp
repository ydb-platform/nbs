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

#ifndef LIBMEM_COMMON_MEMORY_TRAITS_HPP
#define LIBMEM_COMMON_MEMORY_TRAITS_HPP

/**
 * @file MemoryTraits.hpp
 * @brief FunctionTraits specializations for memory functions
 */

#include "traits/FunctionTraits.hpp"
#include <cstring>

namespace libmem {
namespace common {
namespace traits {

// Tag types
struct MemcpyTag {};
struct MemmoveTag {};
struct MemsetTag {};
struct MemcmpTag {};
struct MemchrTag {};
struct MempcpyTag {};

template<>
struct FunctionTraits<MemcpyTag> : MemoryTraitsBase {
    using ReturnType = void*;
    using FunctionPtr = void*(*)(void*, const void*, size_t);

    static const char* name() { return "memcpy"; }

    static constexpr FunctionCategory category = FunctionCategory::COPY;
    static constexpr ReturnCategory return_category = ReturnCategory::DEST_PTR;
    static constexpr bool returns_dest = true;
    static constexpr size_t arg_count = 3;

    static void* invoke(void* dst, const void* src, size_t n) {
        return std::memcpy(dst, src, n);
    }
};

template<>
struct FunctionTraits<MemmoveTag> : MemoryTraitsBase {
    using ReturnType = void*;
    using FunctionPtr = void*(*)(void*, const void*, size_t);

    static const char* name() { return "memmove"; }

    static constexpr FunctionCategory category = FunctionCategory::COPY;
    static constexpr ReturnCategory return_category = ReturnCategory::DEST_PTR;
    static constexpr bool handles_overlap = true;
    static constexpr bool returns_dest = true;
    static constexpr size_t arg_count = 3;

    static void* invoke(void* dst, const void* src, size_t n) {
        return std::memmove(dst, src, n);
    }
};

template<>
struct FunctionTraits<MemsetTag> : MemoryTraitsBase {
    using ReturnType = void*;
    using FunctionPtr = void*(*)(void*, int, size_t);

    static const char* name() { return "memset"; }

    static constexpr FunctionCategory category = FunctionCategory::SET;
    static constexpr ReturnCategory return_category = ReturnCategory::DEST_PTR;
    static constexpr bool returns_dest = true;
    static constexpr size_t arg_count = 3;

    static void* invoke(void* s, int c, size_t n) {
        return std::memset(s, c, n);
    }
};

template<>
struct FunctionTraits<MemcmpTag> : MemoryTraitsBase {
    using ReturnType = int;
    using FunctionPtr = int(*)(const void*, const void*, size_t);

    static const char* name() { return "memcmp"; }

    static constexpr FunctionCategory category = FunctionCategory::COMPARE;
    static constexpr ReturnCategory return_category = ReturnCategory::COMPARISON;
    static constexpr size_t arg_count = 3;

    static int invoke(const void* s1, const void* s2, size_t n) {
        return std::memcmp(s1, s2, n);
    }
};

template<>
struct FunctionTraits<MemchrTag> : MemoryTraitsBase {
    using ReturnType = void*;
    using FunctionPtr = void*(*)(const void*, int, size_t);

    static const char* name() { return "memchr"; }

    static constexpr FunctionCategory category = FunctionCategory::SEARCH;
    static constexpr ReturnCategory return_category = ReturnCategory::FOUND_PTR;
    static constexpr size_t arg_count = 3;

    static void* invoke(void* s, int c, size_t n) {
        return std::memchr(s, c, n);
    }
};

template<>
struct FunctionTraits<MempcpyTag> : MemoryTraitsBase {
    using ReturnType = void*;
    using FunctionPtr = void*(*)(void*, const void*, size_t);

    static const char* name() { return "mempcpy"; }

    static constexpr FunctionCategory category = FunctionCategory::COPY;
    static constexpr ReturnCategory return_category = ReturnCategory::END_PTR;
    static constexpr size_t arg_count = 3;

    static void* invoke(void* dst, const void* src, size_t n) {
        return mempcpy(dst, src, n);
    }
};

} // namespace traits
} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_MEMORY_TRAITS_HPP
