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

#ifndef LIBMEM_VALIDATOR_PAGE_CROSS_TEST_CASE_HPP
#define LIBMEM_VALIDATOR_PAGE_CROSS_TEST_CASE_HPP

/**
 * @file PageCrossTestCase.hpp
 * @brief Base classes for page-level tests
 *
 * Layout:
 *   1. PageBoundaryTestCase  - tests correctness across page boundaries
 *   2. PageOverrunTestCase   - tests bounds safety via trap page
 */

#include "core/TestCase.hpp"
#include "invoke/InvokeAdapter.hpp"
#include "buffer/BufferFactory.hpp"
#include <cstdlib>
#include <vector>

namespace libmem {
namespace validator {

// ============================================================================
// PageBoundaryTestCase
//
// Positions buffers so data crosses internal page boundaries.
// Iterates over alignment offsets and positions near boundaries.
// Subclasses implement runPageBoundaryTest() with function-specific logic.
// ============================================================================

template<typename Tag>
class PageBoundaryTestCase : public TestCase<Tag> {
public:
    const char* name() const override { return "PageBoundary"; }

    // Uses internal START_OFFSETS, not ctx.*_align.
    bool usesAlignmentContext() const override { return false; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        const size_t size = ctx.size;

        if (size < minTestSize()) {
            return TestResult::success();
        }

        auto positions = usePositions() ? getTestPositions(size) : std::vector<size_t>{0};

        for (uint32_t off1 : START_OFFSETS) {
            for (uint32_t off2 : START_OFFSETS) {
                if (requiresDifferentOffsets() && off1 == off2) continue;

                for (size_t pos : positions) {
                    if (usePositions() && pos >= size - 1) continue;

                    auto result = runPageBoundaryTest(size, pos, off1, off2);
                    if (!result.passed) {
                        return result;
                    }
                }

                // Subclasses that ignore off2 collapse this loop to 1 iter.
                if (!usesOff2()) break;
            }
        }

        return TestResult::success();
    }

protected:
    virtual size_t minTestSize() const { return 8; }
    virtual bool requiresDifferentOffsets() const { return false; }
    virtual bool usePositions() const { return true; }

    // Override to false when the subclass ignores off2.
    virtual bool usesOff2() const { return true; }

    virtual TestResult runPageBoundaryTest(size_t size, size_t pos,
                                           uint32_t off1, uint32_t off2) = 0;

    std::vector<size_t> getTestPositions(size_t size) const {
        std::vector<size_t> positions = {4, 16, 32};

        if (size > PAGE_SZ) {
            positions.push_back(PAGE_SZ - 6);
            positions.push_back(PAGE_SZ - 1);
            positions.push_back(PAGE_SZ);
            positions.push_back(PAGE_SZ + 1);
            positions.push_back(PAGE_SZ + 100);
        }

        if (size > 2 * PAGE_SZ) {
            positions.push_back(2 * PAGE_SZ - 1);
            positions.push_back(2 * PAGE_SZ);
            positions.push_back(2 * PAGE_SZ + 1);
        }

        if (size > CACHE_LINE_SZ) {
            positions.push_back(size - 10);
        }

        return positions;
    }

    // Reseed both std::rand() and globalRandomPool() so each iteration
    // produces its own deterministic pattern.
    void seedRandom(size_t size, size_t pos, uint32_t off1, uint32_t off2) const {
        unsigned s = static_cast<unsigned>(size ^ pos ^ off1 ^ off2);
        std::srand(s);
        globalRandomPool().seed(static_cast<uint64_t>(s));
    }

    BufferPair setupPageCross(size_t size, uint32_t offset) {
        AllocParams p = this->ctx_.alloc_params;
        p.page_mode = PageMode::PageCross;
        p.cross_offset = offset;
        return allocateSingle(size, p);
    }

    bool fitsInAlloc(const BufferPair& buf, uint8_t* ptr, size_t needed) const {
        return ptr && buf.valid() &&
               (ptr + needed <= buf.srcAllocBase() + buf.srcAllocSize());
    }

    static constexpr uint32_t START_OFFSETS[] = {1, 16, 31, 32, 48, 63, 64};
    static constexpr size_t NUM_OFFSETS = sizeof(START_OFFSETS) / sizeof(START_OFFSETS[0]);
};

template<typename Tag>
constexpr uint32_t PageBoundaryTestCase<Tag>::START_OFFSETS[];

// ============================================================================
// PageOverrunTestCase
//
// Positions buffers so the operation region ends exactly at a trap page
// (PROT_NONE). Any read/write past the region causes an immediate SEGFAULT.
// Subclasses implement runPageOverrunTest() with function-specific logic.
// ============================================================================

template<typename Tag>
class PageOverrunTestCase : public TestCase<Tag> {
public:
    const char* name() const override { return "PageOverrun"; }

    // Trap-page setup drives alignment; ctx.*_align unused.
    bool usesAlignmentContext() const override { return false; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        if (ctx.size == 0) return TestResult::success();
        return runPageOverrunTest(ctx.size);
    }

protected:
    using Traits = typename TestCase<Tag>::Traits;

    /**
     * Allocate buffer with ptr+size ending exactly at the trap page.
     * Any access past ptr[size-1] will SEGFAULT.
     */
    BufferPair setupAtGuard(size_t size) {
        AllocParams p = this->ctx_.alloc_params;
        p.page_mode = PageMode::PageGuarded;
        return allocateSingle(size, p);
    }

    /**
     * Fill buffer with appropriate data based on function type.
     * String functions: non-null bytes + '\0' at buf[size-1]
     * Memory functions: full byte range (0-255)
     */
    void fillForTest(uint8_t* buf, size_t size) {
        if (Traits::is_string_func) {
            fillNonNull(buf, size);
            buf[size - 1] = '\0';
        } else {
            fillFullByteRange(buf, size);
        }
    }

    /**
     * Replace all occurrences of a character in buffer (for search tests).
     */
    void removeChar(uint8_t* buf, size_t size, uint8_t ch,
                    uint8_t replacement = 0xFE) {
        for (size_t i = 0; i < size; ++i) {
            if (buf[i] == ch) buf[i] = replacement;
        }
    }

    virtual TestResult runPageOverrunTest(size_t size) = 0;
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_PAGE_CROSS_TEST_CASE_HPP
