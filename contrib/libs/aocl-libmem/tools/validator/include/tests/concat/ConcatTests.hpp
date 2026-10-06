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

#ifndef LIBMEM_VALIDATOR_CONCAT_TESTS_HPP
#define LIBMEM_VALIDATOR_CONCAT_TESTS_HPP

/**
 * @file ConcatTests.hpp
 * @brief Concatenation tests for strcat, strncat
 */

#include "core/TestCase.hpp"
#include <cstring>

namespace libmem {
namespace validator {

// ============================================================================
// Basic Concat Tests
// ============================================================================

/**
 * BasicConcatTest - Basic concatenation test for strcat
 */
template<typename Tag>
class BasicConcatTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicConcat"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 2) {
            return TestResult::success();
        }

        auto buf = this->allocate(TestContext(ctx.size * 2, ctx.dst_align, ctx.src_align));

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        buf.dst[0] = 'X';
        buf.dst[1] = '\0';

        size_t src_len = ctx.size > 2 ? ctx.size / 2 : 1;
        fillLowercaseLetters(buf.src, src_len);
        buf.src[src_len] = '\0';

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src)
        );

        if (buf.dst[0] != 'X') {
            return this->error("prefix modified during concat");
        }

        for (size_t i = 0; i < src_len; ++i) {
            if (buf.dst[1 + i] != buf.src[i]) {
                return this->errorAt(1 + i, "concat mismatch, expected=0x%02x, actual=0x%02x",
                                    buf.src[i], buf.dst[1 + i]);
            }
        }

        if (buf.dst[1 + src_len] != '\0') {
            return this->error("NULL terminator missing after concat");
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return value mismatch");
        }

        return TestResult::success();
    }
};

// ============================================================================
// Bounded Concat Tests (strncat)
// ============================================================================

/**
 * StrncatCopyTest - Basic test for strncat
 */
template<typename Tag>
class StrncatCopyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicConcat"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        auto buf = this->allocate(ctx);

        if (!buf.valid()) return this->error("allocation failed");

        buf.dst[0] = 'X';
        buf.dst[1] = '\0';

        size_t src_len = ctx.size > 2 ? ctx.size / 2 : 1;
        fillLowercaseLetters(buf.src, src_len);
        buf.src[src_len] = '\0';

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            src_len
        );

        if (buf.dst[0] != 'X') {
            return this->error("dst prefix modified");
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return mismatch");
        }

        return TestResult::success();
    }
};

// ============================================================================
// Multi-Null Concat Tests
// ============================================================================

/**
 * MultiNullConcatTest - Test concat with NULL in source
 */
template<typename Tag>
class MultiNullConcatTest : public TestCase<Tag> {
public:
    const char* name() const override { return "MultiNullConcat"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 5) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        buf.dst[0] = 'X';
        buf.dst[1] = '\0';
        std::memset(buf.dst + 2, 0xFF, ctx.size - 2);

        size_t src_len = ctx.size / 2;
        fillLowercaseLetters(buf.src, src_len);
        size_t null_pos = src_len / 2;
        buf.src[null_pos] = '\0';
        buf.src[src_len] = '\0';

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src)
        );

        if (buf.dst[0] != 'X') {
            return this->error("dst prefix modified");
        }

        size_t expected_len = 1 + null_pos;
        if (buf.dst[expected_len] != '\0') {
            return this->error("concat didn't stop at NULL");
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return value mismatch");
        }

        return TestResult::success();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_CONCAT_TESTS_HPP

