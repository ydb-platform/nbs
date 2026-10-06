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

#ifndef LIBMEM_VALIDATOR_ZERO_SIZE_TESTS_HPP
#define LIBMEM_VALIDATOR_ZERO_SIZE_TESTS_HPP

/**
 * @file ZeroSizeTests.hpp
 * @brief Tests for size=0 edge cases across all function types
 * 
 * These tests validate correct behavior when called with size=0:
 * - Copy functions should return NULL (or dst for memmove)
 * - Fill functions should return NULL
 * - Compare functions should return 0
 * - Search functions should return NULL
 */

#include "core/TestCase.hpp"
#include <cstring>

namespace libmem {
namespace validator {

// ============================================================================
// Zero-Size Copy Tests
// ============================================================================

/**
 * ZeroSizeCopyTest - Test copy function with size=0 and NULL pointers
 * 
 * For: memcpy, memmove, mempcpy
 * Expected: Return NULL (or dst for memmove), no crash
 */
template<typename Tag>
class ZeroSizeCopyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSize"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        void* ret = this->invoke(static_cast<void*>(nullptr), 
                                  static_cast<const void*>(nullptr), 
                                  static_cast<size_t>(0));
        
        if (ret != nullptr) {
            return this->error("expected NULL return for size=0, got %p", ret);
        }
        
        return TestResult::success();
    }
};

// ============================================================================
// Zero-Size Fill Tests
// ============================================================================

/**
 * ZeroSizeFillTest - Test fill function with size=0
 * 
 * For: memset
 */
template<typename Tag>
class ZeroSizeFillTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSize"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        void* ret = this->invoke(static_cast<void*>(nullptr), 0, static_cast<size_t>(0));
        
        if (ret != nullptr) {
            return this->error("expected NULL return for size=0, got %p", ret);
        }
        
        return TestResult::success();
    }
};

// ============================================================================
// Zero-Size Compare Tests
// ============================================================================

/**
 * ZeroSizeCompareTest - Test compare function with size=0
 * 
 * For: memcmp, strncmp
 * Expected: Return 0 (equal)
 */
template<typename Tag>
class ZeroSizeCompareTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSize"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        int ret = this->invoke(static_cast<const void*>(nullptr), 
                               static_cast<const void*>(nullptr), 
                               static_cast<size_t>(0));
        
        if (ret != 0) {
            return this->error("expected 0 for size=0 compare, got %d", ret);
        }
        
        return TestResult::success();
    }
};

/**
 * ZeroSizeStringCompareTest - For strcmp with empty strings
 */
template<typename Tag>
class ZeroSizeStringCompareTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSize"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        const char* empty1 = "";
        const char* empty2 = "";
        
        int ret = TestCase<Tag>::Traits::invoke(empty1, empty2);
        
        if (ret != 0) {
            return this->error("expected 0 for empty strings, got %d", ret);
        }
        
        return TestResult::success();
    }
};

// ============================================================================
// Zero-Size Search Tests
// ============================================================================

/**
 * ZeroSizeSearchTest - Test search function with size=0
 * 
 * For: memchr
 * Expected: Return NULL
 */
template<typename Tag>
class ZeroSizeSearchTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSize"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        uint8_t dummy = 'x';
        void* ret = this->invoke(static_cast<void*>(&dummy), 'x', static_cast<size_t>(0));
        
        if (ret != nullptr) {
            return this->error("expected NULL for size=0 search, got %p", ret);
        }
        
        return TestResult::success();
    }
};

// ============================================================================
// Zero-Size Length Tests
// ============================================================================

/**
 * ZeroSizeLengthTest - strlen of empty string
 */
template<typename Tag>
class ZeroSizeLengthTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSizeLength"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        const char* empty = "";
        size_t ret = TestCase<Tag>::Traits::invoke(empty);
        
        if (ret != 0) {
            return this->error("expected 0 for empty string, got %zu", ret);
        }
        
        return TestResult::success();
    }
};

/**
 * ZeroSizeBoundedLengthTest - strnlen with maxlen=0
 *
 * strnlen(s, 0) must return 0 regardless of the string content.
 */
template<typename Tag>
class ZeroSizeBoundedLengthTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSizeBoundedLength"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        const char* str = "non-empty string";
        size_t ret = TestCase<Tag>::Traits::invoke(str, static_cast<size_t>(0));

        if (ret != 0) {
            return this->error("expected 0 for maxlen=0, got %zu", ret);
        }

        const char* empty = "";
        ret = TestCase<Tag>::Traits::invoke(empty, static_cast<size_t>(0));

        if (ret != 0) {
            return this->error("expected 0 for empty string with maxlen=0, got %zu", ret);
        }

        return TestResult::success();
    }
};

/**
 * ZeroSizeSpanTest - strspn with empty string
 */
template<typename Tag>
class ZeroSizeSpanTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSizeSpan"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        const char* empty = "";
        const char* accept = "abc";
        
        size_t ret = TestCase<Tag>::Traits::invoke(empty, accept);
        
        if (ret != 0) {
            return this->error("expected 0 for empty string, got %zu", ret);
        }
        
        return TestResult::success();
    }
};

// ============================================================================
// Zero-Size String Copy Tests
// ============================================================================

/**
 * ZeroSizeStringCopyTest - For strcpy with empty string
 */
template<typename Tag>
class ZeroSizeStringCopyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSize"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        char dst[4] = "???";
        const char* src = "";
        
        char* ret = TestCase<Tag>::Traits::invoke(dst, src);
        
        if (dst[0] != '\0') {
            return this->error("expected dst[0]='\\0', got '%c'", dst[0]);
        }
        if (ret != dst) {
            return this->error("return mismatch, expected=%p, actual=%p", dst, ret);
        }
        
        return TestResult::success();
    }
};

// ============================================================================
// Zero-Size Concat Tests
// ============================================================================

/**
 * ZeroSizeConcatTest - strcat/strncat with empty source
 */
template<typename Tag>
class ZeroSizeConcatTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSizeConcat"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        char dst[16] = "test";
        const char* empty_src = "";
        
        char* ret = TestCase<Tag>::Traits::invoke(dst, empty_src);
        
        if (std::strcmp(dst, "test") != 0) {
            return this->error("dst modified when concatenating empty string");
        }
        
        if (ret != dst) {
            return this->error("return value mismatch");
        }
        
        return TestResult::success();
    }
};

/**
 * ZeroSizeBoundedConcatTest - strncat with n=0
 */
template<typename Tag>
class ZeroSizeBoundedConcatTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSizeBoundedConcat"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        char dst[16] = "test";
        const char* src = "append";
        
        char* ret = TestCase<Tag>::Traits::invoke(dst, src, static_cast<size_t>(0));
        
        if (std::strcmp(dst, "test") != 0) {
            return this->error("dst modified when n=0");
        }
        
        if (ret != dst) {
            return this->error("return value mismatch");
        }
        
        return TestResult::success();
    }
};

/**
 * ZeroSizeBoundedCopyTest - strncpy with n=0
 */
template<typename Tag>
class ZeroSizeBoundedCopyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "ZeroSizeBoundedCopy"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        
        char dst[16] = "original";
        const char* src = "replace";
        
        char* ret = TestCase<Tag>::Traits::invoke(dst, src, static_cast<size_t>(0));
        
        if (std::strcmp(dst, "original") != 0) {
            return this->error("dst modified when n=0");
        }
        
        if (ret != dst) {
            return this->error("return value mismatch");
        }
        
        return TestResult::success();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_ZERO_SIZE_TESTS_HPP

