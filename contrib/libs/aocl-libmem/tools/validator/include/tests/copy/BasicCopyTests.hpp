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

#ifndef LIBMEM_VALIDATOR_BASIC_COPY_TESTS_HPP
#define LIBMEM_VALIDATOR_BASIC_COPY_TESTS_HPP

/**
 * @file BasicCopyTests.hpp
 * @brief Basic copy validation tests.
 */

#include "core/TestCase.hpp"

namespace libmem {
namespace validator {

/**
 * BasicCopyTest - Basic non-overlapping copy validation
 */
template<typename Tag>
class BasicCopyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicCopy"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocate(ctx);

        if (!buf.valid()) {
            return this->error("failed to allocate buffers");
        }

        fillLowercaseLetters(buf.src, ctx.size);

        if (TestCase<Tag>::Traits::is_string_func) {
            nullTerminate(buf.src, ctx.size - 1);
        }

        this->prepareBoundary(buf.dst, ctx.size);

        void* ret = this->invoke(buf.dst, buf.src, ctx.size);

        TestResult result = this->validateCopy(buf, ret);
        if (!result.passed) return result;

        return this->validateBoundary(buf.dst, ctx.size);
    }
};

/**
 * BasicCopyNoSizeTest - Copy without size parameter
 */
template<typename Tag>
class BasicCopyNoSizeTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicCopy"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 1) {
            return TestResult::success();
        }
        auto buf = this->allocate(ctx);

        if (!buf.valid()) {
            return this->error("failed to allocate buffers");
        }

        fillLowercaseLetters(buf.src, ctx.size);
        nullTerminate(buf.src, ctx.size - 1);

        this->prepareBoundary(buf.dst, ctx.size);

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src)
        );

        for (size_t i = 0; i < ctx.size; ++i) {
            if (buf.dst[i] != buf.src[i]) {
                return this->errorAt(i, "data mismatch, expected=0x%02x, actual=0x%02x",
                                     buf.src[i], buf.dst[i]);
            }
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return mismatch, expected=%p, actual=%p", buf.dst, ret);
        }

        return this->validateBoundary(buf.dst, ctx.size);
    }
};

/**
 * MultiNullCopyTest - Test that copy stops at first NULL
 */
template<typename Tag>
class MultiNullCopyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "MultiNull"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 3) {
            return TestResult::success();
        }
        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, ctx.size);

        size_t null_pos = ctx.size / 2;
        buf.src[null_pos] = '\0';
        buf.src[ctx.size - 1] = '\0';

        std::memset(buf.dst, 0xFF, ctx.size);

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src)
        );

        for (size_t i = 0; i <= null_pos; ++i) {
            if (buf.dst[i] != buf.src[i]) {
                return this->errorAt(i, "multi-null copy mismatch");
            }
        }

        for (size_t i = null_pos + 1; i < ctx.size; ++i) {
            if (buf.dst[i] != 0xFF) {
                return this->errorAt(i, "copy went beyond NULL terminator");
            }
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return value mismatch");
        }

        return TestResult::success();
    }
};

/**
 * StrncpyStrlenGreaterTest - strncpy when strlen(src) > n
 */
template<typename Tag>
class StrncpyStrlenGreaterTest : public TestCase<Tag> {
public:
    const char* name() const override { return "StrlenGreaterThanN"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 2) {
            return TestResult::success();
        }
        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, ctx.size + 1);
        buf.src[ctx.size + 1] = '\0';

        for (size_t i = ctx.size; i < ctx.size + BOUNDARY_BYTES; ++i) {
            buf.dst[i] = '#';
        }

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        for (size_t i = 0; i < ctx.size; ++i) {
            if (buf.dst[i] != buf.src[i]) {
                return this->errorAt(i, "strlen>n copy mismatch");
            }
        }

        for (size_t i = ctx.size; i < ctx.size + BOUNDARY_BYTES; ++i) {
            if (buf.dst[i] != '#') {
                return this->errorAt(i, "strlen>n modified beyond n bytes");
            }
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return value mismatch");
        }

        return TestResult::success();
    }
};

/**
 * StrncpyStrlenEqualTest - strncpy when strlen(src) == n-1
 */
template<typename Tag>
class StrncpyStrlenEqualTest : public TestCase<Tag> {
public:
    const char* name() const override { return "StrlenEqualN"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 2) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, ctx.size - 1);
        buf.src[ctx.size - 1] = '\0';

        for (size_t i = ctx.size; i < ctx.size + BOUNDARY_BYTES; ++i) {
            buf.dst[i] = '#';
        }

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        for (size_t i = 0; i < ctx.size; ++i) {
            if (buf.dst[i] != buf.src[i]) {
                return this->errorAt(i, "strlen=n copy mismatch");
            }
        }

        for (size_t i = ctx.size; i < ctx.size + BOUNDARY_BYTES; ++i) {
            if (buf.dst[i] != '#') {
                return this->errorAt(i, "strlen=n modified beyond n bytes");
            }
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return value mismatch");
        }

        return TestResult::success();
    }
};

/**
 * StrncpyStrlenLessTest - strncpy when strlen(src) < n (should pad with NULLs)
 */
template<typename Tag>
class StrncpyStrlenLessTest : public TestCase<Tag> {
public:
    const char* name() const override { return "StrlenLessThanN"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 4) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        size_t src_len = ctx.size / 2;
        fillLowercaseLetters(buf.src, src_len);
        buf.src[src_len] = '\0';

        for (size_t i = 0; i < ctx.size; ++i) {
            buf.dst[i] = 'X';
        }

        for (size_t i = ctx.size; i < ctx.size + BOUNDARY_BYTES; ++i) {
            buf.dst[i] = '#';
        }

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        for (size_t i = 0; i <= src_len; ++i) {
            if (buf.dst[i] != buf.src[i]) {
                return this->errorAt(i, "strlen<n string mismatch");
            }
        }

        for (size_t i = src_len + 1; i < ctx.size; ++i) {
            if (buf.dst[i] != '\0') {
                return this->errorAt(i, "strlen<n NULL padding missing, got 0x%02x", buf.dst[i]);
            }
        }

        for (size_t i = ctx.size; i < ctx.size + BOUNDARY_BYTES; ++i) {
            if (buf.dst[i] != '#') {
                return this->errorAt(i, "strlen<n modified beyond n bytes");
            }
        }

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return value mismatch");
        }

        return TestResult::success();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_BASIC_COPY_TESTS_HPP

