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

#ifndef LIBMEM_VALIDATOR_LENGTH_TESTS_HPP
#define LIBMEM_VALIDATOR_LENGTH_TESTS_HPP

/**
 * @file LengthTests.hpp
 * @brief Length tests for strlen, strspn
 */

#include "core/TestCase.hpp"

namespace libmem {
namespace validator {

// ============================================================================
// Basic Length Test
// ============================================================================

/**
 * BasicLengthTest - Basic string length test
 */
template<typename Tag>
class BasicLengthTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicLength"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);  // +1 for NULL

        if (!buf.valid()) {
            return this->error("failed to allocate buffer");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);

        size_t ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst)
        );

        if (ret != ctx.size) {
            return this->error("length mismatch, expected=%zu, actual=%zu", ctx.size, ret);
        }

        return TestResult::success();
    }
};

// ============================================================================
// Span Tests (strspn)
// ============================================================================

/**
 * StrspnTest - Basic strspn test with randomized accept set
 */
template<typename Tag>
class StrspnTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicSpan"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        char alpha[LOWER_CHARS + 1];
        for (size_t i = 0; i < LOWER_CHARS; ++i) alpha[i] = 'a' + i;
        alpha[LOWER_CHARS] = '\0';
        shuffleChars(alpha, static_cast<int>(LOWER_CHARS));
        const int accept_len = 10;
        alpha[accept_len] = '\0';

        for (size_t i = 0; i < ctx.size; ++i)
            buf.dst[i] = alpha[rand() % accept_len];
        nullTerminate(buf.dst, ctx.size);

        size_t ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst), alpha);

        if (ret != ctx.size) {
            return this->error("expected span=%zu, got %zu", ctx.size, ret);
        }

        return TestResult::success();
    }
};

// ============================================================================
// Strspn — Accept Length Variation Tests
// ============================================================================

/**
 * StrspnSingleCharAcceptTest - accept has exactly one random character
 */
template<typename Tag>
class StrspnSingleCharAcceptTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SingleCharAccept"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size < 1) return TestResult::success();

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        char c = 'a' + (rand() % LOWER_CHARS);
        char accept_buf[2] = { c, '\0' };
        char reject = (c == 'z') ? 'a' : (char)(c + 1);

        fillWithByte(buf.dst, ctx.size, (uint8_t)c);
        nullTerminate(buf.dst, ctx.size);

        size_t ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst), accept_buf);

        if (ret != ctx.size) {
            return this->error("expected span=%zu, got %zu", ctx.size, ret);
        }

        if (ctx.size >= 2) {
            size_t mid = ctx.size / 2;
            buf.dst[mid] = reject;

            ret = TestCase<Tag>::Traits::invoke(
                reinterpret_cast<const char*>(buf.dst), accept_buf);

            if (ret != mid) {
                return this->error("single-char partial: expected %zu, got %zu", mid, ret);
            }
        }

        return TestResult::success();
    }
};

/**
 * StrspnAccept16Test - accept is exactly 16 random unique characters
 * (boundary between PCMPISTRI and LUT paths).
 */
template<typename Tag>
class StrspnAccept16Test : public TestCase<Tag> {
public:
    const char* name() const override { return "Accept16Boundary"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size < 1) return TestResult::success();

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        char alpha[LOWER_CHARS + 1];
        for (size_t i = 0; i < LOWER_CHARS; ++i) alpha[i] = 'a' + i;
        alpha[LOWER_CHARS] = '\0';
        shuffleChars(alpha, static_cast<int>(LOWER_CHARS));
        const int accept_len = 16;
        char reject = alpha[accept_len];
        alpha[accept_len] = '\0';

        for (size_t i = 0; i < ctx.size; ++i)
            buf.dst[i] = alpha[rand() % accept_len];
        nullTerminate(buf.dst, ctx.size);

        size_t ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst), alpha);

        if (ret != ctx.size) {
            return this->error("accept-16 full span: expected %zu, got %zu", ctx.size, ret);
        }

        if (ctx.size >= 2) {
            size_t mid = ctx.size / 2;
            buf.dst[mid] = reject;

            ret = TestCase<Tag>::Traits::invoke(
                reinterpret_cast<const char*>(buf.dst), alpha);

            if (ret != mid) {
                return this->error("accept-16 partial: expected %zu, got %zu", mid, ret);
            }
        }

        return TestResult::success();
    }
};

// ============================================================================
// Bounded Length Test (strnlen)
// ============================================================================

/**
 * BasicStrnlenTest
 *
 * Runs all maxlen scenarios for the given size:
 *   1. maxlen > strlen       → should return strlen
 *   2. maxlen < strlen       → should return maxlen
 *   3. maxlen == strlen      → should return strlen
 *   4. maxlen == 0           → should return 0
 *   5. null before maxlen    → should stop at null, not maxlen
 */
template<typename Tag>
class BasicStrnlenTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicStrnlen"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("failed to allocate buffer");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);

        const char* str = reinterpret_cast<const char*>(buf.dst);
        size_t ret;
        size_t maxlen;

        maxlen = ctx.size + 1 + (rand() % 100);
        ret = TestCase<Tag>::Traits::invoke(str, maxlen);
        if (ret != ctx.size) {
            return this->error("(maxlen > strlen) expected=%zu, actual=%zu, maxlen=%zu",
                               ctx.size, ret, maxlen);
        }

        if (ctx.size > 1) {
            maxlen = ctx.size / 2;
            ret = TestCase<Tag>::Traits::invoke(str, maxlen);
            if (ret != maxlen) {
                return this->error("(maxlen < strlen) expected=%zu, actual=%zu, strlen=%zu",
                                   maxlen, ret, ctx.size);
            }
        }

        maxlen = ctx.size;
        ret = TestCase<Tag>::Traits::invoke(str, maxlen);
        if (ret != ctx.size) {
            return this->error("(maxlen == strlen) expected=%zu, actual=%zu",
                               ctx.size, ret);
        }

        ret = TestCase<Tag>::Traits::invoke(str, static_cast<size_t>(0));
        if (ret != 0) {
            return this->error("(maxlen == 0) expected=0, actual=%zu", ret);
        }

        if (ctx.size > 2) {
            size_t null_pos = ctx.size / 2;
            buf.dst[null_pos] = '\0';
            maxlen = ctx.size;
            ret = TestCase<Tag>::Traits::invoke(str, maxlen);
            if (ret != null_pos) {
                return this->error("(null before maxlen) expected=%zu, actual=%zu, maxlen=%zu",
                                   null_pos, ret, maxlen);
            }
        }

        return TestResult::success();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_LENGTH_TESTS_HPP

