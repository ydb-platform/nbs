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

#ifndef LIBMEM_VALIDATOR_STRSTR_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_STRSTR_VALIDATOR_HPP

#include "core/TestRunner.hpp"
#include "traits/StringTraits.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/search/SearchTests.hpp"

namespace libmem {
namespace validator {

/**
 * StrstrValidator - Validates strstr implementation
 */
class StrstrValidator : public TestRunner<traits::StrstrTag, StrstrValidator> {
public:
    void registerTests() {
        tests()
            .add<StrstrFoundTest<traits::StrstrTag>>("Substring found")
            .add<StrstrNotFoundTest<traits::StrstrTag>>("Substring not found")
            .add<StrstrEmptyNeedleTest<traits::StrstrTag>>("Empty needle")
            .add<PageBoundaryStrstrTest<traits::StrstrTag>>("Page boundary")
            .add<PageOverrunStrstrTest<traits::StrstrTag>>("Page overrun");
    }
};

REGISTER_VALIDATOR(StrstrValidator, "strstr")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_STRSTR_VALIDATOR_HPP

