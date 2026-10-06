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

#ifndef LIBMEM_VALIDATOR_FILL_TESTS_HPP
#define LIBMEM_VALIDATOR_FILL_TESTS_HPP

/**
 * @file FillTests.hpp
 * @brief Fill tests for memset
 */

#include "core/TestCase.hpp"

namespace libmem {
namespace validator {

// ============================================================================
// Basic Fill Test
// ============================================================================

/**
 * BasicFillTest - Basic fill validation
 * 
 * For: memset
 */
template<typename Tag>
class BasicFillTest : public TestCase<Tag> {
public:
    const char* name() const override { return "BasicFill"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocateSingleBuf(ctx.size, ctx.dst_align);
        
        if (!buf.valid()) {
            return this->error("failed to allocate buffer");
        }
        
        int value = rand() % 256;
        
        this->prepareBoundary(buf.dst, ctx.size);
        
        void* ret = this->invoke(buf.dst, value, ctx.size);
        
        TestResult result = this->validateFill(buf, static_cast<uint8_t>(value), ret);
        if (!result.passed) return result;
        
        return this->validateBoundary(buf.dst, ctx.size);
    }
};

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_FILL_TESTS_HPP

