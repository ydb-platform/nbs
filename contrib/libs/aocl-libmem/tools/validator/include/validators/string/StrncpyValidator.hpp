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

#ifndef LIBMEM_VALIDATOR_STRNCPY_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_STRNCPY_VALIDATOR_HPP

#include "core/TestRunner.hpp"
#include "traits/StringTraits.hpp"
#include "tests/common/ZeroSizeTests.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/copy/BasicCopyTests.hpp"

namespace libmem {
namespace validator {

/**
 * StrncpyCopyTest - Custom test for strncpy
 */
class StrncpyCopyTest : public TestCase<traits::StrncpyTag> {
public:
    const char* name() const override { return "BasicCopy"; }
    
    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;
        auto buf = this->allocate(ctx);
        
        if (!buf.valid()) return this->error("allocation failed");
        
        fillLowercaseLetters(buf.src, ctx.size);
        nullTerminate(buf.src, ctx.size - 1);
        
        char* ret = traits::FunctionTraits<traits::StrncpyTag>::invoke(
            reinterpret_cast<char*>(buf.dst),
            reinterpret_cast<const char*>(buf.src),
            ctx.size
        );
        
        for (size_t i = 0; i < ctx.size; ++i) {
            if (buf.dst[i] != buf.src[i]) {
                return this->errorAt(i, "data mismatch");
            }
        }
        
        if (ret != reinterpret_cast<char*>(buf.dst)) {
            return this->error("return mismatch");
        }
        
        return TestResult::success();
    }
};

/**
 * StrncpyValidator - Validates strncpy implementation
 */
class StrncpyValidator : public TestRunner<traits::StrncpyTag, StrncpyValidator> {
public:
    void registerTests() {
        tests()
            .zeroSize<ZeroSizeBoundedCopyTest<traits::StrncpyTag>>("Size zero")
            .add<StrncpyCopyTest>("Basic copy")
            .add<StrncpyStrlenGreaterTest<traits::StrncpyTag>>("strlen > n")
            .add<StrncpyStrlenEqualTest<traits::StrncpyTag>>("strlen == n")
            .add<StrncpyStrlenLessTest<traits::StrncpyTag>>("strlen < n (padding)")
            .add<PageBoundaryCopyTest<traits::StrncpyTag>>("Page boundary")
            .add<PageOverrunCopyTest<traits::StrncpyTag>>("Page overrun");
    }
};

REGISTER_VALIDATOR(StrncpyValidator, "strncpy")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_STRNCPY_VALIDATOR_HPP

