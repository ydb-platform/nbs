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

#ifndef LIBMEM_BENCHMARK_CACHE_CONTROL_HPP
#define LIBMEM_BENCHMARK_CACHE_CONTROL_HPP

#include "config/Constants.hpp"
#include "buffer/BufferPair.hpp"
#include <immintrin.h>

namespace libmem {
namespace benchmark {

namespace CacheControl {

inline void flushRegion(const void* addr, size_t size) {
    if (size == 0) return;
    size_t iters = (size - 1) / common::CACHE_LINE_SZ;
    const uint8_t* p = static_cast<const uint8_t*>(addr);
    do {
        _mm_clflushopt(const_cast<void*>(static_cast<const void*>(p + iters * common::CACHE_LINE_SZ)));
    } while (iters--);
    // clflushopt is weakly ordered: fence so all invalidations complete before
    // the timed loads that follow.
    _mm_mfence();
}

inline void flushSrc(const common::BufferPair& buf, size_t size) {
    flushRegion(buf.src, size);
}

inline void flushBoth(const common::BufferPair& buf, size_t size) {
    flushRegion(buf.src, size);
    if (buf.dst != buf.src) {
        flushRegion(buf.dst, size);
    }
}

} // namespace CacheControl

} // namespace benchmark
} // namespace libmem

#endif // LIBMEM_BENCHMARK_CACHE_CONTROL_HPP
