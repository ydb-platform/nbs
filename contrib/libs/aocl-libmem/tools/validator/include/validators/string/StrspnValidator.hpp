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

#ifndef LIBMEM_VALIDATOR_STRSPN_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_STRSPN_VALIDATOR_HPP

#include "core/TestRunner.hpp"
#include "traits/StringTraits.hpp"
#include "tests/common/ZeroSizeTests.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/length/LengthTests.hpp"
#include "tests/search/SearchTests.hpp"
#include "tests/compare/CompareTests.hpp"

namespace libmem {
namespace validator {

/**
 * StrspnValidator - Validates strspn implementation
 */
class StrspnValidator : public TestRunner<traits::StrspnTag, StrspnValidator> {
public:
    void registerTests() {
        tests()
            .zeroSize<ZeroSizeSpanTest<traits::StrspnTag>>("Empty string")
            .add<StrspnTest<traits::StrspnTag>>("Basic span")
            .add<PageBoundaryStrspnTest<traits::StrspnTag>>("Page boundary")
            .add<PageOverrunLengthTest<traits::StrspnTag>>("Page overrun")
            .add<StrspnSingleCharAcceptTest<traits::StrspnTag>>("Single-char accept")
            .add<StrspnAccept16Test<traits::StrspnTag>>("Accept 16-char boundary")
            .add<StrspnLongAcceptMismatchTest<traits::StrspnTag>>("Long accept mismatch")
            .add<StrspnExtendedAsciiTest<traits::StrspnTag>>("Extended ASCII (1-255)")
            .add<AcceptPageCrossStrspnTest<traits::StrspnTag>>("Accept page-cross")
            .add<BothPageCrossStrspnTest<traits::StrspnTag>>("Both s+accept page-cross");
    }
};

REGISTER_VALIDATOR(StrspnValidator, "strspn")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_STRSPN_VALIDATOR_HPP

