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

#ifndef LIBMEM_COMMON_INVOKE_ADAPTER_HPP
#define LIBMEM_COMMON_INVOKE_ADAPTER_HPP

/**
 * @file InvokeAdapter.hpp
 * @brief Single-point dispatch from uint8_t* buffers to FunctionTraits::invoke.
 *
 * Handles all casting (uint8_t* to char* or void*) and argument-shape selection
 * (2-arg vs 3-arg, with or without size) in one place using if constexpr.
 *
 * Used by both benchmark and validator to avoid duplicating dispatch logic.
 */

#include "traits/MemoryTraits.hpp"
#include "traits/StringTraits.hpp"
#include <type_traits>

namespace libmem {
namespace common {
namespace traits {

template<typename Tag>
struct InvokeAdapter {
    using Traits = FunctionTraits<Tag>;
    using ReturnType = typename Traits::ReturnType;

    /**
     * Universal invoke from raw uint8_t* buffers.
     *
     * @param dst     Primary buffer (destination for copy/set/concat, first operand
     *                for compare, buffer for search/length).
     * @param src     Secondary buffer (source for copy/concat, second operand for
     *                compare, needle for strstr, accept for strspn). May be nullptr
     *                for single-buffer operations (set, memchr, strchr, strlen, strnlen).
     * @param size    Size/length parameter. Ignored by unbounded string functions.
     * @param int_arg Value argument for memset (fill byte) and memchr/strchr
     *                (search character). Ignored by other functions.
     */
    static ReturnType invoke(uint8_t* dst, const uint8_t* src,
                             size_t size, int int_arg = 0) {
        constexpr auto cat = Traits::category;

        if constexpr (cat == FunctionCategory::COPY) {
            if constexpr (!Traits::is_string_func)
                return Traits::invoke(dst, src, size);
            else if constexpr (Traits::has_size_param)
                return Traits::invoke(as_char(dst), as_cchar(src), size);
            else
                return Traits::invoke(as_char(dst), as_cchar(src));
        }
        else if constexpr (cat == FunctionCategory::SET) {
            return Traits::invoke(dst, int_arg, size);
        }
        else if constexpr (cat == FunctionCategory::COMPARE) {
            if constexpr (!Traits::is_string_func)
                return Traits::invoke(dst, src, size);
            else if constexpr (Traits::has_size_param)
                return Traits::invoke(as_cchar(dst), as_cchar(src), size);
            else
                return Traits::invoke(as_cchar(dst), as_cchar(src));
        }
        else if constexpr (cat == FunctionCategory::SEARCH) {
            if constexpr (std::is_same_v<Tag, StrstrTag>)
                return Traits::invoke(as_char(dst), as_cchar(src));
            else if constexpr (Traits::is_string_func)
                return Traits::invoke(as_char(dst), int_arg);
            else
                return Traits::invoke(dst, int_arg, size);
        }
        else if constexpr (cat == FunctionCategory::LENGTH) {
            if constexpr (Traits::arg_count == 1)
                return Traits::invoke(as_cchar(dst));
            else if constexpr (Traits::has_size_param)
                return Traits::invoke(as_cchar(dst), size);
            else
                return Traits::invoke(as_cchar(dst), as_cchar(src));
        }
        else if constexpr (cat == FunctionCategory::CONCAT) {
            if constexpr (Traits::has_size_param)
                return Traits::invoke(as_char(dst), as_cchar(src), size);
            else
                return Traits::invoke(as_char(dst), as_cchar(src));
        }
    }

private:
    static char* as_char(uint8_t* p) {
        return reinterpret_cast<char*>(p);
    }
    static const char* as_cchar(const uint8_t* p) {
        return reinterpret_cast<const char*>(p);
    }
};

} // namespace traits
} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_INVOKE_ADAPTER_HPP
