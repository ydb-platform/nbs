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

#ifndef LIBMEM_VALIDATOR_STRRCHR_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_STRRCHR_VALIDATOR_HPP

#include <cstring>
#include "core/TestRunner.hpp"
#include "traits/StringTraits.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/search/SearchTests.hpp"
#include "fill/DataFill.hpp"

namespace libmem {
namespace validator {

/**
 * StrrchrEmptyStringTest - strrchr("", '\0') must return pointer to the
 * null terminator; strrchr("", 'x') must return NULL.
 */
template<typename Tag>
class StrrchrEmptyStringTest : public TestCase<Tag> {
public:
    const char* name() const override { return "EmptyString"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        char empty[1] = {'\0'};

        // strrchr("", '\0') must point to the null terminator.
        char* ret = TestCase<Tag>::Traits::invoke(empty, static_cast<int>('\0'));
        if (ret != empty)
            return this->error("strrchr(\"\", '\\0') expected=%p got=%p",
                               static_cast<void*>(empty), static_cast<void*>(ret));

        // strrchr("", 'x') must return NULL.
        ret = TestCase<Tag>::Traits::invoke(empty, static_cast<int>('x'));
        if (ret != nullptr)
            return this->error("strrchr(\"\", 'x') expected NULL got=%p",
                               static_cast<void*>(ret));

        return TestResult::success();
    }
};

/**
 * StrrchrHighByteSearchTest - c is converted to unsigned char before
 * comparison (C standard 7.24.5.5). Values outside [0,255] must be
 * handled via truncation:
 *   c=256  -> 0x00 -> matches '\0' -> returns pointer to null terminator
 *   c=-1   -> 0xFF -> matches any 0xFF byte in the string
 */
template<typename Tag>
class StrrchrHighByteSearchTest : public TestCase<Tag> {
public:
    const char* name() const override { return "HighByteSearch"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size < 2) return TestResult::success();

        // --- c = 256 (truncates to 0x00) ---
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), 256);
        char* expected = reinterpret_cast<char*>(buf.dst + ctx.size);
        if (ret != expected)
            return this->error("c=256 (->0x00): expected=%p got=%p",
                               static_cast<void*>(expected),
                               static_cast<void*>(ret));

        // --- c = -1 (truncates to 0xFF) ---
        auto buf2 = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf2.valid()) return this->error("allocation failed (buf2)");

        std::memset(buf2.dst, 0xFF, ctx.size);
        buf2.dst[ctx.size] = '\0';

        ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf2.dst), -1);
        // Last 0xFF byte is at index ctx.size - 1.
        expected = reinterpret_cast<char*>(buf2.dst + ctx.size - 1);
        if (ret != expected)
            return this->error("c=-1 (->0xFF): expected last at=%p got=%p",
                               static_cast<void*>(expected),
                               static_cast<void*>(ret));

        return TestResult::success();
    }
};

/**
 * StrrchrSearchAtStartOnlyTest - the target character exists only at
 * position 0, so rightmost == leftmost == 0.  StringSearchFoundTest
 * places '!' at a random position and would only reach this case by
 * chance; this test covers it deterministically.
 */
template<typename Tag>
class StrrchrSearchAtStartOnlyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SearchAtStartOnly"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size < 1) return TestResult::success();

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);
        buf.dst[0] = '!';  // single occurrence at position 0

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), static_cast<int>('!'));
        char* expected = reinterpret_cast<char*>(buf.dst);
        if (ret != expected)
            return this->error("rightmost==leftmost==0: expected=%p got=%p",
                               static_cast<void*>(expected),
                               static_cast<void*>(ret));

        return TestResult::success();
    }
};

/**
 * StrrchrAllSameCharTest - all characters identical (exercises the
 * all-ones SIMD mask path). strrchr must return the LAST (rightmost)
 * occurrence, i.e. the byte immediately before the null terminator.
 */
template<typename Tag>
class StrrchrAllSameCharTest : public TestCase<Tag> {
public:
    const char* name() const override { return "AllSameChar"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size < 1) return TestResult::success();

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        std::memset(buf.dst, 'x', ctx.size);
        nullTerminate(buf.dst, ctx.size);

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), static_cast<int>('x'));
        char* expected = reinterpret_cast<char*>(buf.dst + ctx.size - 1);
        if (ret != expected)
            return this->error("all-same: expected last at=%p got=%p",
                               static_cast<void*>(expected),
                               static_cast<void*>(ret));

        return TestResult::success();
    }
};

template<typename Tag>
class StringSearchLastMatchTest : public TestCase<Tag> {
public:
    const char* name() const override { return "LastMatch"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 3) {
            return TestResult::success();
        }

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);

        const size_t first = 0;
        const size_t second = ctx.size / 2;
        const size_t last = ctx.size - 1;
        buf.dst[first] = '!';
        buf.dst[second] = '!';
        buf.dst[last] = '!';

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), static_cast<int>('!'));

        char* expected = reinterpret_cast<char*>(buf.dst + last);
        if (ret != expected) {
            return this->error("expected last match at %p, got %p", expected, ret);
        }

        return TestResult::success();
    }
};

/**
 * StrrchrValidator - Validates strrchr implementation
 */
class StrrchrValidator : public TestRunner<traits::StrrchrTag, StrrchrValidator> {
public:
    void registerTests() {
        tests()
            .add<StringSearchFoundTest<traits::StrrchrTag>>("Char found")
            .add<StringSearchLastMatchTest<traits::StrrchrTag>>("Last char match")
            .add<StringSearchNotFoundTest<traits::StrrchrTag>>("Char not found")
            .add<StringSearchNullCharTest<traits::StrrchrTag>>("Search NULL char")
            .add<StrrchrEmptyStringTest<traits::StrrchrTag>>("Empty string")
            .add<StrrchrHighByteSearchTest<traits::StrrchrTag>>("High-byte search")
            .add<StrrchrSearchAtStartOnlyTest<traits::StrrchrTag>>("Search at start only")
            .add<StrrchrAllSameCharTest<traits::StrrchrTag>>("All same char")
            .add<PageBoundaryStrrchrTest<traits::StrrchrTag>>("Page boundary")
            .add<PageOverrunStrrchrTest<traits::StrrchrTag>>("Page overrun");
    }
};

REGISTER_VALIDATOR(StrrchrValidator, "strrchr")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_STRRCHR_VALIDATOR_HPP
