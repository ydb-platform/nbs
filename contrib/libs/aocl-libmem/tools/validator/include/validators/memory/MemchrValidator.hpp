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

#ifndef LIBMEM_VALIDATOR_MEMCHR_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_MEMCHR_VALIDATOR_HPP

#include "core/TestRunner.hpp"
#include "traits/MemoryTraits.hpp"
#include "tests/common/ZeroSizeTests.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/search/SearchTests.hpp"

namespace libmem {
namespace validator {

/**
 * MemchrValidator - Validates memchr implementation
 */
class MemchrValidator : public TestRunner<traits::MemchrTag, MemchrValidator> {
public:
    void registerTests() {
        tests()
            .zeroSize<ZeroSizeSearchTest<traits::MemchrTag>>("Size zero")
            .add<FoundSearchTest<traits::MemchrTag>>("Found search")
            .add<NotFoundSearchTest<traits::MemchrTag>>("Not found search")
            .add<SearchAtStartTest<traits::MemchrTag>>("Found at start")
            .add<SearchAtEndTest<traits::MemchrTag>>("Found at end")
            .add<SearchNullCharTest<traits::MemchrTag>>("Search null char")
            .add<PageBoundaryMemchrTest<traits::MemchrTag>>("Page boundary")
            .add<PageOverrunMemchrTest<traits::MemchrTag>>("Page overrun");
    }
};

REGISTER_VALIDATOR(MemchrValidator, "memchr")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_MEMCHR_VALIDATOR_HPP

