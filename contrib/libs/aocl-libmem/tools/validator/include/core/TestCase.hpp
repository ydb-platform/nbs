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

#ifndef LIBMEM_VALIDATOR_TEST_CASE_HPP
#define LIBMEM_VALIDATOR_TEST_CASE_HPP

#include "config/Logging.hpp"
#include "traits/FunctionTraits.hpp"
#include "config/Constants.hpp"
#include "buffer/BufferPair.hpp"
#include "buffer/BufferFactory.hpp"
#include "fill/DataFill.hpp"
#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

namespace libmem {
namespace validator {

using namespace common;

// ============================================================================
// TestResult - Unified result from test execution
// ============================================================================

struct TestResult {
    bool passed;
    std::string message;
    size_t error_index;

    TestResult() : passed(true), error_index(SIZE_MAX) {}

    static TestResult success() {
        TestResult r;
        r.passed = true;
        return r;
    }

    static TestResult failure(const std::string& msg, size_t idx = SIZE_MAX) {
        TestResult r;
        r.passed = false;
        r.message = msg;
        r.error_index = idx;
        return r;
    }

    static TestResult failure(const char* msg, size_t idx = SIZE_MAX) {
        return failure(std::string(msg), idx);
    }
};

// ============================================================================
// TestContext - Test execution context
// ============================================================================

struct TestContext {
    size_t size;
    uint32_t dst_align;
    uint32_t src_align;
    AllocParams alloc_params;

    TestContext() : size(0), dst_align(0), src_align(0) {}

    TestContext(size_t sz, uint32_t dst, uint32_t src)
        : size(sz), dst_align(dst), src_align(src) {}
};

// ============================================================================
// TestCase - Base class for all tests
// ============================================================================

/**
 * TestCase - Unified base class for all test cases
 *
 * Every test in the framework inherits from this class. It provides:
 * - Type-safe function invocation via FunctionTraits<Tag>
 * - Buffer allocation and management
 * - Common validation helpers
 * - Consistent error reporting
 */
template<typename Tag>
class TestCase {
public:
    using Traits = traits::FunctionTraits<Tag>;
    using ReturnType = typename Traits::ReturnType;

    virtual ~TestCase() = default;

    virtual const char* name() const = 0;
    virtual TestResult execute(const TestContext& ctx) = 0;

    // Override to false if the test ignores ctx.src_align / ctx.dst_align;
    // such tests are skipped on non-canonical passes of validateSweep().
    virtual bool usesAlignmentContext() const { return true; }

protected:
    TestContext ctx_;

    // ========================================================================
    // Buffer Allocation
    // ========================================================================

    BufferPair allocate(const TestContext& ctx) {
        ctx_ = ctx;
        AllocParams p = ctx.alloc_params;
        p.src_offset = ctx.src_align;
        p.dst_offset = ctx.dst_align;
        return allocateDual(ctx.size, ctx.size, p);
    }

    BufferPair allocateSingleBuf(size_t size, uint32_t align) {
        AllocParams p = ctx_.alloc_params;
        p.src_offset = align;
        return allocateSingle(size, p);
    }

    BufferPair allocateOverlapBuf(size_t size, size_t offset) {
        AllocParams p = ctx_.alloc_params;
        return allocateOverlap(size, offset, p);
    }

    // ========================================================================
    // Boundary Checking
    // ========================================================================

    void prepareBoundary(uint8_t* buf, size_t size) {
        for (size_t i = 1; i <= BOUNDARY_BYTES; ++i) {
            *(buf - i) = '#';
            *(buf + size + i - 1) = '#';
        }
    }

    bool checkBoundary(uint8_t* buf, size_t size) {
        for (size_t i = 1; i <= BOUNDARY_BYTES; ++i) {
            if (*(buf - i) != '#' || *(buf + size + i - 1) != '#') {
                return false;
            }
        }
        return true;
    }

    // ========================================================================
    // Function Invocation
    // ========================================================================

    template<typename Arg1, typename Arg2, typename Arg3>
    ReturnType invoke(Arg1 a1, Arg2 a2, Arg3 a3) {
        return Traits::invoke(a1, a2, a3);
    }

    template<typename Arg1, typename Arg2>
    ReturnType invoke(Arg1 a1, Arg2 a2) {
        return Traits::invoke(a1, a2);
    }

    template<typename Arg1>
    ReturnType invoke(Arg1 a1) {
        return Traits::invoke(a1);
    }

    // ========================================================================
    // Common Validation Helpers
    // ========================================================================

    TestResult validateCopy(const BufferPair& buf, void* ret_value) {
        for (size_t i = 0; i < buf.size; ++i) {
            if (buf.dst[i] != buf.src[i]) {
                return error("data mismatch at index %zu, expected=0x%02x, actual=0x%02x",
                            i, buf.src[i], buf.dst[i]);
            }
        }

        void* expected = Traits::returns_dest
            ? static_cast<void*>(buf.dst)
            : (Traits::return_category == traits::ReturnCategory::END_PTR
                ? static_cast<void*>(buf.dst + buf.size)
                : static_cast<void*>(buf.dst));

        if (ret_value != expected) {
            return error("return value mismatch, expected=%p, actual=%p", expected, ret_value);
        }

        return TestResult::success();
    }

    TestResult validateFill(const BufferPair& buf, uint8_t expected_value, void* ret_value) {
        for (size_t i = 0; i < buf.size; ++i) {
            if (buf.dst[i] != expected_value) {
                return error("fill mismatch at index %zu, expected=0x%02x, actual=0x%02x",
                            i, expected_value, buf.dst[i]);
            }
        }

        if (ret_value != buf.dst) {
            return error("return value mismatch, expected=%p, actual=%p", buf.dst, ret_value);
        }

        return TestResult::success();
    }

    TestResult validateCompare(int actual, int expected) {
        int sign_actual = (actual > 0) ? 1 : (actual < 0) ? -1 : 0;
        int sign_expected = (expected > 0) ? 1 : (expected < 0) ? -1 : 0;

        if (sign_actual != sign_expected) {
            return error("comparison mismatch, expected sign=%d, actual sign=%d (actual=%d)",
                        sign_expected, sign_actual, actual);
        }
        return TestResult::success();
    }

    TestResult validateCompareExact(int actual, int expected) {
        if (actual != expected) {
            return error("comparison mismatch, expected=%d, actual=%d", expected, actual);
        }
        return TestResult::success();
    }

    TestResult validateFound(void* actual, void* expected) {
        if (actual != expected) {
            return error("search result mismatch, expected=%p, actual=%p", expected, actual);
        }
        return TestResult::success();
    }

    TestResult validateNotFound(void* actual) {
        if (actual != nullptr) {
            return error("expected NULL but got %p", actual);
        }
        return TestResult::success();
    }

    TestResult validateSize(size_t actual, size_t expected) {
        if (actual != expected) {
            return error("size mismatch, expected=%zu, actual=%zu", expected, actual);
        }
        return TestResult::success();
    }

    TestResult validateBoundary(uint8_t* buf, size_t size) {
        if (!checkBoundary(buf, size)) {
            return error("boundary check failed (buffer overrun detected)");
        }
        return TestResult::success();
    }

    // ========================================================================
    // Error Formatting
    // ========================================================================

    TestResult error(const char* message) {
        char full_msg[1024];
        std::snprintf(full_msg, sizeof(full_msg),
            "ERROR:[%s:%s] %s [size=%zu, dst_align=%u, src_align=%u]",
            Traits::name(), name(), message, ctx_.size, ctx_.dst_align, ctx_.src_align);
        return TestResult::failure(full_msg);
    }

    template<typename T, typename... Args>
    TestResult error(const char* format, T first, Args... args) {
        char buf[512];
        std::snprintf(buf, sizeof(buf), format, first, args...);
        char full_msg[1024];
        std::snprintf(full_msg, sizeof(full_msg),
            "ERROR:[%s:%s] %s [size=%zu, dst_align=%u, src_align=%u]",
            Traits::name(), name(), buf, ctx_.size, ctx_.dst_align, ctx_.src_align);
        return TestResult::failure(full_msg);
    }

    TestResult errorAt(size_t index, const char* message) {
        char full_msg[1024];
        std::snprintf(full_msg, sizeof(full_msg),
            "ERROR:[%s:%s] %s @index=%zu [size=%zu, dst_align=%u, src_align=%u]",
            Traits::name(), name(), message, index, ctx_.size, ctx_.dst_align, ctx_.src_align);
        return TestResult::failure(full_msg, index);
    }

    template<typename T, typename... Args>
    TestResult errorAt(size_t index, const char* format, T first, Args... args) {
        char buf[512];
        std::snprintf(buf, sizeof(buf), format, first, args...);

        char full_msg[1024];
        std::snprintf(full_msg, sizeof(full_msg),
            "ERROR:[%s:%s] %s @index=%zu [size=%zu, dst_align=%u, src_align=%u]",
            Traits::name(), name(), buf, index, ctx_.size, ctx_.dst_align, ctx_.src_align);
        return TestResult::failure(full_msg, index);
    }
};

// ============================================================================
// ITestCase - Type-erased interface for test registration
// ============================================================================

class ITestCase {
public:
    virtual ~ITestCase() = default;
    virtual const char* name() const = 0;
    virtual TestResult execute(const TestContext& ctx) = 0;
    virtual bool usesAlignmentContext() const = 0;
};

template<typename TestType>
class TestCaseWrapper : public ITestCase {
    TestType test_;

public:
    TestCaseWrapper() = default;

    const char* name() const override {
        return test_.name();
    }

    TestResult execute(const TestContext& ctx) override {
        return test_.execute(ctx);
    }

    bool usesAlignmentContext() const override {
        return test_.usesAlignmentContext();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_TEST_CASE_HPP
