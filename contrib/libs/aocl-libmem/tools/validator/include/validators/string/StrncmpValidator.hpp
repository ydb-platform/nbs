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

#ifndef LIBMEM_VALIDATOR_STRNCMP_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_STRNCMP_VALIDATOR_HPP

#include "core/TestRunner.hpp"
#include "traits/StringTraits.hpp"
#include "tests/common/ZeroSizeTests.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/compare/CompareTests.hpp"

namespace libmem {
namespace validator {

/**
 * StrncmpZeroSizeTest - Custom zero-size test for strncmp
 */
class StrncmpZeroSizeTest : public TestCase<traits::StrncmpTag> {
public:
    const char* name() const override { return "ZeroSize"; }

    TestResult execute(const TestContext& ctx) override {
        this->ctx_ = ctx;

        const char* s1 = "test";
        const char* s2 = "different";

        int ret = traits::FunctionTraits<traits::StrncmpTag>::invoke(s1, s2, 0);

        if (ret != 0) {
            return this->error("expected 0 for size=0, got %d", ret);
        }

        return TestResult::success();
    }
};

/**
 * StrncmpValidator - Validates strncmp implementation
 */
class StrncmpValidator : public TestRunner<traits::StrncmpTag, StrncmpValidator> {
public:
    void registerTests() {
        tests()
            .zeroSize<StrncmpZeroSizeTest>("Size zero")
            .add<StringCompareEqualTest<traits::StrncmpTag>>("Equal strings")
            .add<StringCompareLessTest<traits::StrncmpTag>>("Less string")
            .add<StringCompareGreaterTest<traits::StrncmpTag>>("Greater string")
            .add<ByteByByteMismatchTest<traits::StrncmpTag>>("Byte-by-byte mismatch")
            .add<StrncmpBeyondNullTest<traits::StrncmpTag>>("Stop at NULL")
            .add<StrncmpNoNullInRangeTest<traits::StrncmpTag>>("No NULL in range")
            .add<CompareStrlenBothGreaterTest<traits::StrncmpTag>>("Both strings longer")
            .add<MultiNullCompareTest<traits::StrncmpTag>>("Multi-null compare")
            .add<HighByteCompareTest<traits::StrncmpTag>>("High-byte compare (128-255)")
            .add<ExtendedAsciiCompareTest<traits::StrncmpTag>>("Extended ASCII compare")
            .add<PageBoundaryCompareTest<traits::StrncmpTag>>("Page boundary")
            .add<PageOverrunCompareTest<traits::StrncmpTag>>("Page overrun");
    }
};

REGISTER_VALIDATOR(StrncmpValidator, "strncmp")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_STRNCMP_VALIDATOR_HPP

