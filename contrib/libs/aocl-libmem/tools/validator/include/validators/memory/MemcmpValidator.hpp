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

#ifndef LIBMEM_VALIDATOR_MEMCMP_VALIDATOR_HPP
#define LIBMEM_VALIDATOR_MEMCMP_VALIDATOR_HPP

#include "core/TestRunner.hpp"
#include "traits/MemoryTraits.hpp"
#include "tests/common/ZeroSizeTests.hpp"
#include "tests/common/PageCrossTests.hpp"
#include "tests/compare/CompareTests.hpp"

namespace libmem {
namespace validator {

/**
 * MemcmpValidator - Validates memcmp implementation
 */
class MemcmpValidator : public TestRunner<traits::MemcmpTag, MemcmpValidator> {
public:
    void registerTests() {
        tests()
            .zeroSize<ZeroSizeCompareTest<traits::MemcmpTag>>("Size zero")
            .add<EqualCompareTest<traits::MemcmpTag>>("Equal data")
            .add<MismatchCompareTest<traits::MemcmpTag>>("Different data")
            .add<ByteByByteMismatchTest<traits::MemcmpTag>>("Byte-by-byte mismatch")
            .add<PageBoundaryCompareTest<traits::MemcmpTag>>("Page boundary")
            .add<PageOverrunCompareTest<traits::MemcmpTag>>("Page overrun");
    }
};

REGISTER_VALIDATOR(MemcmpValidator, "memcmp")

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_MEMCMP_VALIDATOR_HPP

