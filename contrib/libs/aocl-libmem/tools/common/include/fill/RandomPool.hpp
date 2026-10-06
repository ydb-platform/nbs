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

#ifndef LIBMEM_COMMON_RANDOM_POOL_HPP
#define LIBMEM_COMMON_RANDOM_POOL_HPP

/**
 * @file RandomPool.hpp
 * @brief xoshiro256** PRNG with batched byte-fill helpers.
 *
 * Shared by validator and benchmark via DataFill.hpp.
 */

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace libmem {
namespace common {

class RandomPool {
public:
    RandomPool() { seed(0xA5A5A5A5A5A5A5A5ULL); }

    // SplitMix64-expand a 64-bit seed into the 256-bit xoshiro state.
    void seed(uint64_t s) {
        uint64_t z = s;
        s_[0] = splitmix64(z);
        s_[1] = splitmix64(z);
        s_[2] = splitmix64(z);
        s_[3] = splitmix64(z);
        // xoshiro256** has an all-zero fixed point; bump state if hit.
        if ((s_[0] | s_[1] | s_[2] | s_[3]) == 0) s_[0] = 1;
    }

    inline uint64_t next_u64() {
        const uint64_t result = rotl(s_[1] * 5, 7) * 9;
        const uint64_t t = s_[1] << 17;
        s_[2] ^= s_[0];
        s_[3] ^= s_[1];
        s_[1] ^= s_[2];
        s_[0] ^= s_[3];
        s_[2] ^= t;
        s_[3] = rotl(s_[3], 45);
        return result;
    }

    inline uint32_t next_u32() { return static_cast<uint32_t>(next_u64() >> 32); }

    // Uniform in [0,n); Lemire's debiased bounded-range avoids modulo bias.
    inline uint32_t uniform_u32(uint32_t n) {
        if (n == 0) return 0;
        uint64_t m = static_cast<uint64_t>(next_u32()) * static_cast<uint64_t>(n);
        return static_cast<uint32_t>(m >> 32);
    }

    inline void fill_bytes(uint8_t* buf, size_t n) {
        // Specialized: bulk memcpy, no per-byte transform.
        size_t i = 0;
        while (i + 8 <= n) {
            uint64_t v = next_u64();
            std::memcpy(buf + i, &v, 8);
            i += 8;
        }
        if (i < n) {
            uint64_t v = next_u64();
            for (size_t j = 0; j < n - i; ++j)
                buf[i + j] = static_cast<uint8_t>(v >> (j * 8));
        }
    }

    // Bytes in [1..255]; any 0 from the PRNG is remapped to 0xFF.
    inline void fill_non_null(uint8_t* buf, size_t n) {
        fill_with([](uint64_t v, size_t b) {
            uint8_t x = static_cast<uint8_t>(v >> (b * 8));
            return x ? x : static_cast<uint8_t>(0xFF);
        }, buf, n);
    }

    // Lowercase 'a'..'z' via (u*26)>>8 (no modulo).
    inline void fill_lowercase(uint8_t* buf, size_t n) {
        fill_with([](uint64_t v, size_t b) {
            uint32_t u = static_cast<uint32_t>((v >> (b * 8)) & 0xFFu);
            return static_cast<uint8_t>('a' + ((u * 26u) >> 8));
        }, buf, n);
    }

    // Printable ASCII [32..126] via (u*95)>>8.
    inline void fill_printable(uint8_t* buf, size_t n) {
        fill_with([](uint64_t v, size_t b) {
            uint32_t u = static_cast<uint32_t>((v >> (b * 8)) & 0xFFu);
            return static_cast<uint8_t>(32u + ((u * 95u) >> 8));
        }, buf, n);
    }

    // Bytes in [0..255]; any 0 is remapped to [1..128] using high bits of v.
    inline void fill_full_range_non_zero(uint8_t* buf, size_t n) {
        fill_with([](uint64_t v, size_t b) {
            uint8_t x = static_cast<uint8_t>(v >> (b * 8));
            if (x == 0) {
                uint8_t r = static_cast<uint8_t>((v >> 56) & 0x7Fu);
                x = static_cast<uint8_t>(1u + r);
            }
            return x;
        }, buf, n);
    }

private:
    // Shared 8-byte-block driver: one PRNG word per block, xform(v, b)
    // emits each output byte. Inlined; codegen matches the hand loops.
    template <typename Xform>
    inline void fill_with(Xform xform, uint8_t* buf, size_t n) {
        size_t i = 0;
        while (i < n) {
            uint64_t v = next_u64();
            size_t lim = (n - i < 8) ? (n - i) : 8;
            for (size_t b = 0; b < lim; ++b) {
                buf[i + b] = xform(v, b);
            }
            i += lim;
        }
    }

    uint64_t s_[4];

    static inline uint64_t rotl(uint64_t x, int k) {
        return (x << k) | (x >> (64 - k));
    }

    static inline uint64_t splitmix64(uint64_t& z) {
        z += 0x9E3779B97F4A7C15ULL;
        uint64_t x = z;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
        return x ^ (x >> 31);
    }
};

// Process-wide singleton; seeded from main(), reseeded per-iteration
// by PageBoundary tests via seedRandom().
inline RandomPool& globalRandomPool() {
    static RandomPool pool;
    return pool;
}

} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_RANDOM_POOL_HPP
