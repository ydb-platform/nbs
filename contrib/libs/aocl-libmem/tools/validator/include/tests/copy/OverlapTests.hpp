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

#ifndef LIBMEM_VALIDATOR_OVERLAP_TESTS_HPP
#define LIBMEM_VALIDATOR_OVERLAP_TESTS_HPP

/**
 * Tests overlapping copies at multiple offsets:
 * - Vector register sizes (XMM=16, YMM=32, ZMM=64)
 * - ctx.size/2 for general overlap coverage
 */

#include "core/TestCase.hpp"
#include <vector>
#include <array>

namespace libmem {
namespace validator {

enum class OverlapDirection {
    Forward,   // dst > src (dst starts after src)
    Backward   // dst < src (src starts after dst)
};


template<typename Tag, OverlapDirection Dir>
class OverlapTest : public TestCase<Tag> {
public:
    const char* name() const override {
        return Dir == OverlapDirection::Forward ? "ForwardOverlap" : "BackwardOverlap";
    }

    // Offsets derived from ctx.size only; ctx.*_align unused.
    bool usesAlignmentContext() const override { return false; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        // Offsets to test
        const std::array<size_t, 4> offsets = { XMM_SZ, YMM_SZ, ZMM_SZ, ctx.size / 2 };

        for (size_t offset : offsets) {
            // Skip invalid offsets (no meaningful overlap)
            if (offset == 0 || offset >= ctx.size) {
                continue;
            }

            TestResult result = testAtOffset(ctx, offset);
            if (!result.passed) {
                return result;
            }
        }

        return TestResult::success();
    }

private:
    /**
     * Allocate overlapping buffers with specific offset
     * - Forward:  src = base, dst = base + offset
     * - Backward: dst = base, src = base + offset
     */
    BufferPair allocateWithOffset(size_t size, size_t offset) {
        auto buf = this->allocateOverlapBuf(size, offset);
        if (!buf.valid()) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        if constexpr (Dir == OverlapDirection::Forward) {
            buf.src = base;
            buf.dst = base + offset;
        } else {
            buf.dst = base;
            buf.src = base + offset;
        }

        return buf;
    }

    /**
     * Test overlap copy at a specific offset
     */
    TestResult testAtOffset(const TestContext& ctx, size_t offset) {
        BufferPair buf = allocateWithOffset(ctx.size, offset);
        if (!buf.valid()) {
            return this->error("failed to allocate buffer [offset=%zu]", offset);
        }

        std::vector<uint8_t> expected(ctx.size);
        fillDeterministic(buf.src, expected, ctx.size, offset);

        // Invoke the copy function
        void* ret = this->invoke(buf.dst, buf.src, ctx.size);

        // Validate copy correctness
        TestResult result = validateOverlapCopy(buf.dst, expected, offset);
        if (!result.passed) {
            return result;
        }

        // Validate return value
        if (ret != buf.dst) {
            return this->error("return mismatch [offset=%zu], expected=%p, actual=%p",
                               offset, static_cast<void*>(buf.dst), ret);
        }

        return TestResult::success();
    }

    /**
     * Fill buffer with deterministic pattern based on index and offset
     */
    void fillDeterministic(uint8_t* buf, std::vector<uint8_t>& expected, size_t size, size_t offset) {
        // Use different multipliers for forward/backward to vary patterns
        constexpr uint8_t multiplier = (Dir == OverlapDirection::Forward) ? 7 : 11;
        for (size_t i = 0; i < size; ++i) {
            uint8_t val = static_cast<uint8_t>((i * multiplier + offset) & 0xFF);
            buf[i] = val;
            expected[i] = val;
        }
    }

    /**
     * Validate that destination matches expected values
     */
    TestResult validateOverlapCopy(const uint8_t* dst, const std::vector<uint8_t>& expected, size_t offset) {
        const char* direction = (Dir == OverlapDirection::Forward) ? "forward" : "backward";
        for (size_t i = 0; i < expected.size(); ++i) {
            if (dst[i] != expected[i]) {
                return this->errorAt(i, "%s overlap mismatch [offset=%zu], expected=0x%02x, actual=0x%02x",
                                     direction, offset, expected[i], dst[i]);
            }
        }
        return TestResult::success();
    }
};

template<typename Tag>
using ForwardOverlapTest = OverlapTest<Tag, OverlapDirection::Forward>;

template<typename Tag>
using BackwardOverlapTest = OverlapTest<Tag, OverlapDirection::Backward>;


// InPlaceCopyTest where dst == src (complete overlap)
template<typename Tag>
class InPlaceCopyTest : public TestCase<Tag> {
public:
    const char* name() const override { return "InPlaceCopy"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        BufferPair buf = this->allocateSingleBuf(ctx.size, ctx.dst_align);
        if (!buf.valid()) {
            return this->error("failed to allocate buffer");
        }

        // Fill buffer with deterministic pattern
        std::vector<uint8_t> expected(ctx.size);
        for (size_t i = 0; i < ctx.size; ++i) {
            uint8_t val = static_cast<uint8_t>((i * 13 + 17) & 0xFF);
            buf.dst[i] = val;
            expected[i] = val;
        }

        // Copy in place
        void* ret = this->invoke(buf.dst, buf.dst, ctx.size);

        // Validate data is preserved
        for (size_t i = 0; i < ctx.size; ++i) {
            if (buf.dst[i] != expected[i]) {
                return this->errorAt(i, "in-place copy corrupted data, expected=0x%02x, actual=0x%02x",
                                     expected[i], buf.dst[i]);
            }
        }

        // Validate return value
        if (ret != buf.dst) {
            return this->error("return mismatch, expected=%p, actual=%p",
                               static_cast<void*>(buf.dst), ret);
        }

        return TestResult::success();
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_OVERLAP_TESTS_HPP
