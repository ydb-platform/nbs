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

#ifndef LIBMEM_COMMON_FUNCTION_TRAITS_HPP
#define LIBMEM_COMMON_FUNCTION_TRAITS_HPP

/**
 * @file FunctionTraits.hpp
 * @brief Base definitions for function traits system
 *
 * This file contains:
 * - Function category and return category enumerations
 * - Primary FunctionTraits template declaration
 * - Trait helper utilities
 */

#include <cstddef>

namespace libmem {
namespace common {
namespace traits {

enum class FunctionCategory {
    COPY,       // memcpy, memmove, strcpy, strncpy
    SET,        // memset
    COMPARE,    // memcmp, strcmp, strncmp
    SEARCH,     // memchr, strchr, strstr
    LENGTH,     // strlen, strspn
    CONCAT      // strcat, strncat
};

enum class ReturnCategory {
    DEST_PTR,       // Returns destination pointer (memcpy, strcpy, etc.)
    END_PTR,        // Returns pointer past copied data (mempcpy)
    FOUND_PTR,      // Returns pointer to found item or NULL (memchr, strchr, strstr)
    COMPARISON,     // Returns int comparison result (memcmp, strcmp, strncmp)
    SIZE,           // Returns size_t (strlen, strspn)
    VOID            // Returns void (none currently)
};

template<typename Tag>
struct FunctionTraits;

struct MemoryTraitsBase {
    static constexpr bool has_size_param = true;
    static constexpr bool is_string_func = false;
    static constexpr bool is_bounded = true;
    static constexpr bool null_terminates = false;
    static constexpr bool handles_overlap = false;
    static constexpr bool returns_dest = false;
};

struct StringTraitsBase {
    static constexpr bool has_size_param = false;
    static constexpr bool is_string_func = true;
    static constexpr bool is_bounded = false;
    static constexpr bool null_terminates = true;
    static constexpr bool handles_overlap = false;
    static constexpr bool returns_dest = false;
};

} // namespace traits
} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_FUNCTION_TRAITS_HPP
