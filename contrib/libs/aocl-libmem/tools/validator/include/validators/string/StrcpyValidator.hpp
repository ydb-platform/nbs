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

#ifndef LIBMEM_VALIDATOR_STRCPY_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_STRCPY_VALIDATOR_HPP

#include "core/TestRunner.hpp"
#include "traits/StringTraits.hpp"
#include "tests/common/ZeroSizeTests.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/copy/BasicCopyTests.hpp"

namespace libmem {
namespace validator {

/**
 * StrcpyValidator - Validates strcpy implementation
 */
class StrcpyValidator : public TestRunner<traits::StrcpyTag, StrcpyValidator> {
public:
    void registerTests() {
        tests()
            .zeroSize<ZeroSizeStringCopyTest<traits::StrcpyTag>>("Empty string copy")
            .add<BasicCopyNoSizeTest<traits::StrcpyTag>>("Basic copy")
            .add<MultiNullCopyTest<traits::StrcpyTag>>("Multi-null")
            .add<PageBoundaryCopyTest<traits::StrcpyTag>>("Page boundary")
            .add<PageOverrunCopyTest<traits::StrcpyTag>>("Page overrun");
    }
};

REGISTER_VALIDATOR(StrcpyValidator, "strcpy")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_STRCPY_VALIDATOR_HPP

