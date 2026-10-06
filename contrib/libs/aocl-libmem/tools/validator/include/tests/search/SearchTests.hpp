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

#ifndef LIBMEM_VALIDATOR_SEARCH_TESTS_HPP
#define LIBMEM_VALIDATOR_SEARCH_TESTS_HPP

/**
 * @file SearchTests.hpp
 * @brief Search tests for memchr, strchr, strstr
 */

#include "core/TestCase.hpp"

namespace libmem {
namespace validator {

// ============================================================================
// Basic Search Tests (memchr)
// ============================================================================

/**
 * FoundSearchTest - Test search that finds the target
 */
template<typename Tag>
class FoundSearchTest : public TestCase<Tag> {
public:
    const char* name() const override { return "FoundSearch"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        // Need at least 1 byte to search
        if (ctx.size < 1) {
            return TestResult::success();
        }

        auto buf = this->allocateSingleBuf(ctx.size, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("failed to allocate buffer");
        }

        fillLowercaseLetters(buf.dst, ctx.size);

        size_t pos = ctx.size > 1 ? (rand() % (ctx.size - 1)) : 0;
        buf.dst[pos] = '!';

        void* ret = this->invoke(buf.dst, '!', ctx.size);

        void* expected = buf.dst + pos;
        if (ret != expected) {
            return this->error("search mismatch, expected=%p, actual=%p", expected, ret);
        }

        return TestResult::success();
    }
};

/**
 * NotFoundSearchTest - Test search that doesn't find the target
 */
template<typename Tag>
class NotFoundSearchTest : public TestCase<Tag> {
public:
    const char* name() const override { return "NotFoundSearch"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("failed to allocate buffer");
        }

        fillLowercaseLetters(buf.dst, ctx.size);

        void* ret = this->invoke(buf.dst, '#', ctx.size);

        if (ret != nullptr) {
            return this->error("expected NULL for not-found, got %p", ret);
        }

        return TestResult::success();
    }
};

/**
 * SearchAtStartTest - Character found at position 0
 */
template<typename Tag>
class SearchAtStartTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SearchAtStart"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 1) {
            return TestResult::success();
        }

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        buf.dst[0] = '!';  // Place at start
        buf.dst[ctx.size] = '\0';

        void* ret = this->invoke(buf.dst, '!', ctx.size);

        if (ret != buf.dst) {
            return this->error("expected to find at start, got %p vs %p", buf.dst, ret);
        }

        return TestResult::success();
    }
};

/**
 * SearchAtEndTest - Character found at last position
 */
template<typename Tag>
class SearchAtEndTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SearchAtEnd"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 2) {
            return TestResult::success();
        }

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        buf.dst[ctx.size - 1] = '!';
        buf.dst[ctx.size] = '\0';

        void* ret = this->invoke(buf.dst, '!', ctx.size);

        void* expected = buf.dst + ctx.size - 1;
        if (ret != expected) {
            return this->error("expected to find at end, got %p vs %p", expected, ret);
        }

        return TestResult::success();
    }
};

/**
 * SearchNullCharTest - Search for null character '\0' in buffer
 */
template<typename Tag>
class SearchNullCharTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SearchNullChar"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 2) {
            return TestResult::success();
        }

        auto buf = this->allocateSingleBuf(ctx.size, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        // Fill with non-null characters
        for (size_t i = 0; i < ctx.size; ++i) {
            buf.dst[i] = 'a' + (rand() % LOWER_CHARS);
        }

        // Place null character at a random position
        size_t pos = rand() % ctx.size;
        buf.dst[pos] = '\0';

        void* ret = this->invoke(buf.dst, '\0', ctx.size);

        void* expected = buf.dst + pos;
        if (ret != expected) {
            return this->error("expected to find null at %p, got %p", expected, ret);
        }

        return TestResult::success();
    }
};

// ============================================================================
// String Search Tests (strchr)
// ============================================================================

/**
 * StringSearchFoundTest - For strchr
 */
template<typename Tag>
class StringSearchFoundTest : public TestCase<Tag> {
public:
    const char* name() const override { return "CharFound"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 1) {
            return TestResult::success();
        }

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);

        size_t pos = ctx.size > 1 ? (rand() % ctx.size) : 0;
        buf.dst[pos] = '!';

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), static_cast<int>('!'));

        char* expected = reinterpret_cast<char*>(buf.dst + pos);
        if (ret != expected) {
            return this->error("search mismatch, expected=%p, actual=%p", expected, ret);
        }

        return TestResult::success();
    }
};

/**
 * StringSearchNotFoundTest - For strchr with character not in string
 */
template<typename Tag>
class StringSearchNotFoundTest : public TestCase<Tag> {
public:
    const char* name() const override { return "CharNotFound"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), static_cast<int>('#'));

        if (ret != nullptr) {
            return this->error("expected NULL for not-found, got %p", ret);
        }

        return TestResult::success();
    }
};

/**
 * StringSearchNullCharTest - strchr searching for NULL terminator
 */
template<typename Tag>
class StringSearchNullCharTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SearchNullChar"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        buf.dst[ctx.size] = '\0';

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), static_cast<int>('\0'));

        char* expected = reinterpret_cast<char*>(buf.dst + ctx.size);
        if (ret != expected) {
            return this->error("expected to find NULL at %p, got %p", expected, ret);
        }

        return TestResult::success();
    }
};

// ============================================================================
// Substring Search Tests (strstr)
// ============================================================================

/**
 * StrstrFoundTest - Substring search that finds the target
 */
template<typename Tag>
class StrstrFoundTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SubstringFound"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        // Need enough space for needle (3 bytes) + null terminator
        if (ctx.size < 4) {
            return TestResult::success();
        }

        auto buf = this->allocateSingleBuf(ctx.size + 10, ctx.dst_align);

        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.dst, ctx.size);
        nullTerminate(buf.dst, ctx.size);

        char needle[4] = "abc";
        size_t needle_len = 3;

        size_t pos = ctx.size > needle_len ? ctx.size / 2 : 0;
        if (pos + needle_len < ctx.size) {
            for (size_t i = 0; i < needle_len; ++i) {
                buf.dst[pos + i] = needle[i];
            }
        }

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), needle);

        if (ret == nullptr && pos + needle_len < ctx.size) {
            return this->error("expected to find substring");
        }

        return TestResult::success();
    }
};

/**
 * StrstrNotFoundTest - Substring not in haystack
 */
template<typename Tag>
class StrstrNotFoundTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SubstringNotFound"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        buf.dst[ctx.size] = '\0';

        const char* needle = "XYZ";

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), needle);

        if (ret != nullptr) {
            return this->error("expected NULL for not-found, got %p", ret);
        }

        return TestResult::success();
    }
};

/**
 * StrstrEmptyNeedleTest - Empty needle returns haystack
 */
template<typename Tag>
class StrstrEmptyNeedleTest : public TestCase<Tag> {
public:
    const char* name() const override { return "EmptyNeedle"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.dst, ctx.size);
        buf.dst[ctx.size] = '\0';

        const char* empty_needle = "";

        char* ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<char*>(buf.dst), empty_needle);

        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("empty needle should return haystack, got %p", ret);
        }

        return TestResult::success();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_SEARCH_TESTS_HPP

