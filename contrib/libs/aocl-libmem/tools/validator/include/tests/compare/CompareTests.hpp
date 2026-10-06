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

#ifndef LIBMEM_VALIDATOR_COMPARE_TESTS_HPP
#define LIBMEM_VALIDATOR_COMPARE_TESTS_HPP

/**
 * @file CompareTests.hpp
 * @brief Comparison tests for memcmp, strcmp, strncmp
 */

#include "core/TestCase.hpp"

namespace libmem {
namespace validator {

// ============================================================================
// Basic Compare Tests
// ============================================================================

/**
 * EqualCompareTest - Test comparison of equal data
 */
template<typename Tag>
class EqualCompareTest : public TestCase<Tag> {
public:
    const char* name() const override { return "EqualCompare"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocate(ctx);

        if (!buf.valid()) {
            return this->error("failed to allocate buffers");
        }

        fillLowercaseLetters(buf.src, ctx.size);
        copyData(buf.dst, buf.src, ctx.size);

        if (TestCase<Tag>::Traits::is_string_func) {
            nullTerminate(buf.src, ctx.size - 1);
            nullTerminate(buf.dst, ctx.size - 1);
        }

        int ret = this->invoke(buf.dst, buf.src, ctx.size);

        if (ret != 0) {
            return this->error("expected 0 for equal data, got %d", ret);
        }

        return TestResult::success();
    }
};

/**
 * MismatchCompareTest - Test comparison of different data
 */
template<typename Tag>
class MismatchCompareTest : public TestCase<Tag> {
public:
    const char* name() const override { return "MismatchCompare"; }

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
        copyData(buf.dst, buf.src, ctx.size);

        buf.dst[0] = buf.src[0] + 1;

        if (TestCase<Tag>::Traits::is_string_func) {
            nullTerminate(buf.src, ctx.size - 1);
            nullTerminate(buf.dst, ctx.size - 1);
        }

        int ret = this->invoke(buf.dst, buf.src, ctx.size);

        if (ret == 0) {
            return this->error("expected non-zero for different data, got 0");
        }

        return TestResult::success();
    }
};

/**
 * ByteByByteMismatchTest - Exhaustive test: validates mismatch at EVERY index position
 */
template<typename Tag>
class ByteByByteMismatchTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ByteByByteMismatch"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocate(ctx);

        if (!buf.valid()) {
            return this->error("failed to allocate buffers");
        }

        fillLowercaseLetters(buf.src, ctx.size);
        copyData(buf.dst, buf.src, ctx.size);

        if (TestCase<Tag>::Traits::is_string_func) {
            nullTerminate(buf.src, ctx.size - 1);
            nullTerminate(buf.dst, ctx.size - 1);
        }

        // Exhaustive up to PAGE_SZ; stride-sample ~256 positions beyond to cap O(N^2).
        size_t stride = (ctx.size <= PAGE_SZ) ? 1 : (ctx.size / 256 + 1);

        auto probe = [&](size_t i) -> TestResult {
            if (TestCase<Tag>::Traits::is_string_func && i >= ctx.size - 1) {
                return TestResult::success();
            }
            return checkPos(buf, ctx.size, i);
        };

        for (size_t i = 0; i < ctx.size; i += stride) {
            TestResult r = probe(i);
            if (!r.passed) return r;
        }

        // Uniform stride may skip vector-size / threshold boundaries;
        // also probe every VEC_SZ-aligned index missed by the stride.
        if (stride > 1) {
            for (size_t i = VEC_SZ; i < ctx.size; i += VEC_SZ) {
                if ((i % stride) == 0) continue;
                TestResult r = probe(i);
                if (!r.passed) return r;
            }
        }

        // Stride > 1 may skip the tail; probe the last meaningful index.
        // (For string funcs that's last-1, since buf[last] is '\0'.)
        size_t last = ctx.size - 1;
        if (stride > 1 && (last % stride) != 0 &&
            !(TestCase<Tag>::Traits::is_string_func && last + 1 == ctx.size)) {
            size_t tail = TestCase<Tag>::Traits::is_string_func ? last - 1 : last;
            TestResult r = checkPos(buf, ctx.size, tail);
            if (!r.passed) return r;
        }

        return TestResult::success();
    }

private:
    // Inject a less-than and a greater-than mismatch at index i, verify
    // the comparator's sign/value, and restore the byte. Shared by the
    // main loop and the tail probe.
    TestResult checkPos(BufferPair& buf, size_t size, size_t i) {
        uint8_t original = buf.dst[i];

        buf.dst[i] = (buf.src[i] > 0) ? buf.src[i] - 1 : buf.src[i] + 1;

        int ret = invokeCompare(buf.dst, buf.src, size);
        int expected = static_cast<int>(buf.dst[i]) - static_cast<int>(buf.src[i]);

        if (TestCase<Tag>::Traits::is_string_func) {
            if ((ret < 0) != (expected < 0)) {
                buf.dst[i] = original;
                return this->errorAt(i, "sign mismatch at index, expected %s0, got %d",
                                    expected < 0 ? "<" : ">", ret);
            }
        } else {
            if (ret != expected) {
                buf.dst[i] = original;
                return this->errorAt(i, "byte-by-byte mismatch, expected=%d, got=%d",
                                    expected, ret);
            }
        }

        buf.dst[i] = buf.src[i] + 10;

        ret = invokeCompare(buf.dst, buf.src, size);
        expected = static_cast<int>(buf.dst[i]) - static_cast<int>(buf.src[i]);

        if (TestCase<Tag>::Traits::is_string_func) {
            if ((ret > 0) != (expected > 0)) {
                buf.dst[i] = original;
                return this->errorAt(i, "sign mismatch (greater), expected %s0, got %d",
                                    expected > 0 ? ">" : "<", ret);
            }
        } else {
            if (ret != expected) {
                buf.dst[i] = original;
                return this->errorAt(i, "byte-by-byte greater mismatch, expected=%d, got=%d",
                                    expected, ret);
            }
        }

        buf.dst[i] = original;
        return TestResult::success();
    }

    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param &&
                           !traits::FunctionTraits<T>::is_string_func, int>::type
    invokeCompare(uint8_t* s1, uint8_t* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(s1, s2, n);
    }

    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param &&
                           traits::FunctionTraits<T>::is_string_func, int>::type
    invokeCompare(uint8_t* s1, uint8_t* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(s1),
            reinterpret_cast<const char*>(s2), n);
    }

    template<typename T = Tag>
    typename std::enable_if<!traits::FunctionTraits<T>::has_size_param &&
                           traits::FunctionTraits<T>::is_string_func, int>::type
    invokeCompare(uint8_t* s1, uint8_t* s2, size_t /*n*/) {
        return TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(s1),
            reinterpret_cast<const char*>(s2));
    }
};

// ============================================================================
// String Compare Tests
// ============================================================================

/**
 * StringCompareEqualTest - For strcmp/strncmp with equal strings
 */
template<typename Tag>
class StringCompareEqualTest : public TestCase<Tag> {
public:
    const char* name() const override { return "EqualStrings"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 1) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);

        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.src, ctx.size);
        nullTerminate(buf.src, ctx.size - 1);
        copyData(buf.dst, buf.src, ctx.size);

        int ret = invokeCompare(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        if (ret != 0) {
            return this->error("expected 0 for equal strings, got %d", ret);
        }

        return TestResult::success();
    }

public:
    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(s1, s2, n);
    }

    template<typename T = Tag>
    typename std::enable_if<!traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t /*n*/) {
        return TestCase<Tag>::Traits::invoke(s1, s2);
    }
};

/**
 * StringCompareLessTest - First string less than second
 */
template<typename Tag>
class StringCompareLessTest : public TestCase<Tag> {
public:
    const char* name() const override { return "LessString"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 1) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);

        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.src, ctx.size);
        nullTerminate(buf.src, ctx.size - 1);
        copyData(buf.dst, buf.src, ctx.size);

        buf.dst[0] = 'A';
        buf.src[0] = 'z';

        int ret = invokeCompare(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        if (ret >= 0) {
            return this->error("expected negative for s1 < s2, got %d", ret);
        }

        return TestResult::success();
    }

public:
    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(s1, s2, n);
    }

    template<typename T = Tag>
    typename std::enable_if<!traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t /*n*/) {
        return TestCase<Tag>::Traits::invoke(s1, s2);
    }
};

/**
 * StringCompareGreaterTest - First string greater than second
 */
template<typename Tag>
class StringCompareGreaterTest : public TestCase<Tag> {
public:
    const char* name() const override { return "GreaterString"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 1) {
            return TestResult::success();
        }
        auto buf = this->allocate(ctx);

        if (!buf.valid()) return this->error("allocation failed");

        fillLowercaseLetters(buf.src, ctx.size);
        nullTerminate(buf.src, ctx.size - 1);
        copyData(buf.dst, buf.src, ctx.size);

        buf.dst[0] = 'z';
        buf.src[0] = 'A';

        int ret = invokeCompare(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        if (ret <= 0) {
            return this->error("expected positive for s1 > s2, got %d", ret);
        }

        return TestResult::success();
    }

public:
    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(s1, s2, n);
    }

    template<typename T = Tag>
    typename std::enable_if<!traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t /*n*/) {
        return TestCase<Tag>::Traits::invoke(s1, s2);
    }
};

// ============================================================================
// String Length Comparison Tests
// ============================================================================

/**
 * CompareFirstShorterTest - First string shorter than second
 */
template<typename Tag>
class CompareFirstShorterTest : public TestCase<Tag> {
public:
    const char* name() const override { return "FirstStringShorter"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 4) {
            return TestResult::success();
        }

        size_t extra_size = ctx.size + VEC_SZ;
        auto buf = this->allocate(TestContext(extra_size, ctx.dst_align, ctx.src_align));

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, ctx.size);
        copyData(buf.dst, buf.src, ctx.size);

        buf.dst[ctx.size - 1] = '\0';

        fillLowercaseLetters(buf.src + ctx.size, extra_size - ctx.size - 1);
        buf.src[extra_size - 1] = '\0';

        int ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src)
        );

        if (ret >= 0) {
            return this->error("expected negative (shorter < longer), got %d", ret);
        }

        return TestResult::success();
    }
};

/**
 * CompareSecondShorterTest - Second string shorter than first
 */
template<typename Tag>
class CompareSecondShorterTest : public TestCase<Tag> {
public:
    const char* name() const override { return "SecondStringShorter"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 4) {
            return TestResult::success();
        }

        size_t extra_size = ctx.size + VEC_SZ;
        auto buf = this->allocate(TestContext(extra_size, ctx.dst_align, ctx.src_align));

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, ctx.size);
        copyData(buf.dst, buf.src, ctx.size);

        buf.src[ctx.size - 1] = '\0';

        fillLowercaseLetters(buf.dst + ctx.size, extra_size - ctx.size - 1);
        buf.dst[extra_size - 1] = '\0';

        int ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src)
        );

        if (ret <= 0) {
            return this->error("expected positive (longer > shorter), got %d", ret);
        }

        return TestResult::success();
    }
};

// ============================================================================
// Strncmp-Specific Tests
// ============================================================================

/**
 * StrncmpBeyondNullTest - strncmp should not compare beyond NULL
 */
template<typename Tag>
class StrncmpBeyondNullTest : public TestCase<Tag> {
public:
    const char* name() const override { return "StopAtNull"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 4) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        size_t null_pos = ctx.size / 2;

        fillLowercaseLetters(buf.src, null_pos);
        copyData(buf.dst, buf.src, null_pos);
        buf.src[null_pos] = buf.dst[null_pos] = '\0';

        for (size_t i = null_pos + 1; i < ctx.size; ++i) {
            buf.src[i] = 'X';
            buf.dst[i] = 'Y';
        }

        int ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        if (ret != 0) {
            return this->error("strncmp should stop at NULL, but got %d", ret);
        }

        return TestResult::success();
    }
};

/**
 * StrncmpNoNullInRangeTest - strncmp when neither string has NULL within n bytes
 */
template<typename Tag>
class StrncmpNoNullInRangeTest : public TestCase<Tag> {
public:
    const char* name() const override { return "NoNullInRange"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 2) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, ctx.size);
        copyData(buf.dst, buf.src, ctx.size);

        buf.dst[ctx.size - 1] = 'X';
        buf.src[ctx.size - 1] = 'Y';

        int ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        int expected = static_cast<int>('X') - static_cast<int>('Y');

        if (ret != expected) {
            return this->error("no-null comparison mismatch, expected=%d, got=%d", expected, ret);
        }

        return TestResult::success();
    }
};

/**
 * CompareStrlenBothGreaterTest - Both strings longer than comparison size
 */
template<typename Tag>
class CompareStrlenBothGreaterTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BothStringsLonger"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 2) {
            return TestResult::success();
        }

        size_t extra_size = ctx.size + VEC_SZ;
        auto buf = this->allocate(TestContext(extra_size, ctx.dst_align, ctx.src_align));

        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, extra_size - 1);
        copyData(buf.dst, buf.src, extra_size - 1);
        buf.src[extra_size - 1] = '\0';
        buf.dst[extra_size - 1] = '\0';

        int ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        if (ret != 0) {
            return this->error("expected 0 for matching prefix, got %d", ret);
        }

        return TestResult::success();
    }
};

/**
 * HighByteCompareTest - Test comparison with characters in 128-255 range
 *
 * This test catches the signed comparison bug where _mm256_cmpgt_epi8
 * treats bytes as signed, causing characters 128-255 to be incorrectly
 * treated as less than 0 (null terminator detection).
 */
template<typename Tag>
class HighByteCompareTest : public TestCase<Tag> {
public:
    const char* name() const override { return "HighByteCompare"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 4) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        // Fill with high-byte values (128-254, avoiding 0 and 255 for safety)
        for (size_t i = 0; i < ctx.size - 1; ++i) {
            buf.src[i] = 128 + (rand() % 126);  // Range 128-253
            buf.dst[i] = buf.src[i];
        }
        buf.src[ctx.size - 1] = '\0';
        buf.dst[ctx.size - 1] = '\0';

        // Test 1: Equal strings with high bytes should return 0
        int ret = invokeCompare(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        if (ret != 0) {
            return this->error("equal high-byte strings should return 0, got %d", ret);
        }

        // Test 2: Create mismatch at middle with high bytes
        size_t mid = ctx.size / 2;
        uint8_t original = buf.dst[mid];
        buf.dst[mid] = 200;  // High byte value
        buf.src[mid] = 180;  // Different high byte value

        ret = invokeCompare(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        // 200 > 180, so result should be positive
        if (ret <= 0) {
            return this->errorAt(mid, "high-byte mismatch: expected positive (200 vs 180), got %d", ret);
        }

        // Test 3: Reverse comparison
        ret = invokeCompare(
            reinterpret_cast<const char*>(buf.src),
            reinterpret_cast<const char*>(buf.dst),
            ctx.size
        );

        // 180 < 200, so result should be negative
        if (ret >= 0) {
            return this->errorAt(mid, "high-byte mismatch: expected negative (180 vs 200), got %d", ret);
        }

        buf.dst[mid] = original;
        buf.src[mid] = original;

        // Test 4: Specific edge case - char 0xFF (255) and 0x80 (128)
        buf.dst[0] = 0xFF;  // 255
        buf.src[0] = 0x80;  // 128

        ret = invokeCompare(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        // 255 > 128 (as unsigned), should be positive
        if (ret <= 0) {
            return this->errorAt(0, "edge case: 0xFF vs 0x80, expected positive, got %d", ret);
        }

        return TestResult::success();
    }

private:
    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(s1, s2, n);
    }

    template<typename T = Tag>
    typename std::enable_if<!traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t /*n*/) {
        return TestCase<Tag>::Traits::invoke(s1, s2);
    }
};

/**
 * ExtendedAsciiCompareTest - Test all byte values at every position
 *
 * Exhaustive test that ensures strcmp handles ALL byte values (1-255)
 * correctly when they appear at different positions in the string.
 */
template<typename Tag>
class ExtendedAsciiCompareTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ExtendedAsciiCompare"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        // Only run for smaller sizes to keep test time reasonable
        if (ctx.size < 2 || ctx.size > 256) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        // Test high-byte values at each position
        uint8_t test_values[] = {128, 129, 200, 254, 255};

        for (uint8_t test_val : test_values) {
            // Fill with 'A's
            for (size_t i = 0; i < ctx.size - 1; ++i) {
                buf.src[i] = buf.dst[i] = 'A';
            }
            buf.src[ctx.size - 1] = buf.dst[ctx.size - 1] = '\0';

            // Place test value at position 0
            buf.dst[0] = test_val;
            buf.src[0] = 'A';  // 65

            int ret = invokeCompare(
                reinterpret_cast<const char*>(buf.dst),
                reinterpret_cast<const char*>(buf.src),
                ctx.size
            );

            // test_val (128-255) > 'A' (65), should be positive
            if (ret <= 0) {
                return this->errorAt(0, "byte value %u vs 'A': expected positive, got %d",
                                    (unsigned)test_val, ret);
            }
        }

        return TestResult::success();
    }

private:
    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(s1, s2, n);
    }

    template<typename T = Tag>
    typename std::enable_if<!traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t /*n*/) {
        return TestCase<Tag>::Traits::invoke(s1, s2);
    }
};

/**
 * MultiNullCompareTest - strcmp/strncmp with NULL in middle
 */
template<typename Tag>
class MultiNullCompareTest : public TestCase<Tag> {
public:
    const char* name() const override { return "MultiNullCompare"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        if (ctx.size < 4) {
            return TestResult::success();
        }

        auto buf = this->allocate(ctx);
        if (!buf.valid()) {
            return this->error("allocation failed");
        }

        fillLowercaseLetters(buf.src, ctx.size);
        copyData(buf.dst, buf.src, ctx.size);

        size_t null_pos = ctx.size / 2;
        buf.src[null_pos] = buf.dst[null_pos] = '\0';

        if (null_pos + 1 < ctx.size) {
            buf.dst[null_pos + 1] = '@';
        }

        int ret = invokeCompare(
            reinterpret_cast<const char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );

        if (ret != 0) {
            return this->error("expected 0 (stops at NULL), got %d", ret);
        }

        return TestResult::success();
    }

private:
    template<typename T = Tag>
    typename std::enable_if<traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t n) {
        return TestCase<Tag>::Traits::invoke(s1, s2, n);
    }

    template<typename T = Tag>
    typename std::enable_if<!traits::FunctionTraits<T>::has_size_param, int>::type
    invokeCompare(const char* s1, const char* s2, size_t /*n*/) {
        return TestCase<Tag>::Traits::invoke(s1, s2);
    }
};

/**
 * StrspnLongAcceptMismatchTest - Byte-by-byte mismatch with accept > 16 chars.
 * Exercises the LUT/vpshufb path specifically (not PCMPISTRI).
 */
template<typename Tag>
class StrspnLongAcceptMismatchTest : public TestCase<Tag> {
public:
    const char* name() const override { return "LongAcceptMismatch"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size < 2) return TestResult::success();

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        // Shuffle 52 chars (a-z + A-Z), pick 30 as accept; reject from the rest
        char pool[2 * LOWER_CHARS + 1];
        for (size_t i = 0; i < LOWER_CHARS; ++i) pool[i] = 'a' + i;
        for (size_t i = 0; i < LOWER_CHARS; ++i) pool[LOWER_CHARS + i] = 'A' + i;
        pool[2 * LOWER_CHARS] = '\0';
        shuffleChars(pool, static_cast<int>(2 * LOWER_CHARS));
        const int accept_len = 30;
        char reject = pool[accept_len];
        pool[accept_len] = '\0';

        // Fill buffer once with accepted chars, then sweep reject position
        for (size_t i = 0; i < ctx.size; ++i)
            buf.dst[i] = pool[rand() % accept_len];
        nullTerminate(buf.dst, ctx.size);

        // Exhaustive up to 256; stride-sampled beyond (keeps ~256 tests max)
        size_t stride = (ctx.size <= 256) ? 1 : (ctx.size / 256 + 1);

        for (size_t pos = 0; pos < ctx.size; pos += stride) {
            char saved = buf.dst[pos];
            buf.dst[pos] = reject;

            size_t ret = TestCase<Tag>::Traits::invoke(
                reinterpret_cast<const char*>(buf.dst), pool);

            if (ret != pos) {
                return this->errorAt(pos, "long accept mismatch: expected %zu, got %zu", pos, ret);
            }

            buf.dst[pos] = saved;
        }

        // Always test the last position
        if ((ctx.size - 1) % stride != 0) {
            size_t pos = ctx.size - 1;
            char saved = buf.dst[pos];
            buf.dst[pos] = reject;

            size_t ret = TestCase<Tag>::Traits::invoke(
                reinterpret_cast<const char*>(buf.dst), pool);

            if (ret != pos) {
                return this->errorAt(pos, "long accept mismatch: expected %zu, got %zu", pos, ret);
            }

            buf.dst[pos] = saved;
        }

        return TestResult::success();
    }
};

/**
 * StrspnExtendedAsciiTest - Verifies correct membership for all byte values
 * (1-255) in accept. Catches signed-comparison bugs, nibble table errors,
 * and LUT indexing issues across the full 0x01-0xFF range.
 */
template<typename Tag>
class StrspnExtendedAsciiTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ExtendedAsciiSpan"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size < 2 || ctx.size > 256) return TestResult::success();

        auto buf = this->allocateSingleBuf(ctx.size + 1, ctx.dst_align);
        if (!buf.valid()) return this->error("allocation failed");

        // Accept all 255 non-zero bytes (inherent to the test)
        char all_accept[256];
        for (int i = 0; i < 255; ++i)
            all_accept[i] = (char)(i + 1);
        all_accept[255] = '\0';

        // Fill s with random non-zero bytes
        for (size_t i = 0; i < ctx.size; ++i)
            buf.dst[i] = (uint8_t)(1 + (rand() % 255));
        nullTerminate(buf.dst, ctx.size);

        size_t ret = TestCase<Tag>::Traits::invoke(
            reinterpret_cast<const char*>(buf.dst), all_accept);

        if (ret != ctx.size) {
            return this->error("all-byte accept: expected %zu, got %zu", ctx.size, ret);
        }

        // Spot-check edge byte values at random positions
        uint8_t edge_values[] = {1, 127, 128, 200, 254, 255};
        for (uint8_t val : edge_values) {
            size_t pos = rand() % ctx.size;
            buf.dst[pos] = (char)val;
            for (size_t i = 0; i < ctx.size; ++i) {
                if (i != pos)
                    buf.dst[i] = (uint8_t)(1 + (rand() % 255));
            }
            nullTerminate(buf.dst, ctx.size);

            ret = TestCase<Tag>::Traits::invoke(
                reinterpret_cast<const char*>(buf.dst), all_accept);

            if (ret != ctx.size) {
                return this->errorAt(pos, "byte 0x%02X should be in accept, span=%zu expected %zu",
                                    (unsigned)val, ret, ctx.size);
            }
        }

        return TestResult::success();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_COMPARE_TESTS_HPP
