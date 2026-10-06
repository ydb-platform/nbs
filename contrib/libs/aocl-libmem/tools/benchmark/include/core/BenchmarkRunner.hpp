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

#ifndef LIBMEM_BENCHMARK_RUNNER_HPP
#define LIBMEM_BENCHMARK_RUNNER_HPP

/**
 * @file BenchmarkRunner.hpp
 * @brief Trait-dispatched benchmark runner. One template replaces 10 runner methods.
 *
 * Uses FunctionTraits<Tag> + InvokeAdapter<Tag> for compile-time dispatch.
 */

#include <benchmark/benchmark.h>
#include "traits/MemoryTraits.hpp"
#include "traits/StringTraits.hpp"
#include "invoke/InvokeAdapter.hpp"
#include "buffer/BufferFactory.hpp"
#include "fill/DataFill.hpp"
#include "fill/RandomPool.hpp"
#include "core/BenchmarkConfig.hpp"
#include "core/CacheControl.hpp"
#include "config/Logging.hpp"
#include <cstring>
#include <type_traits>

namespace libmem {
namespace benchmark {

using namespace common::traits;

// ============================================================================
// BenchmarkRunner<Tag> -- the single templated benchmark entry point
// ============================================================================

template<typename Tag>
class BenchmarkRunner {
    using Traits = FunctionTraits<Tag>;

public:
    static void run(::benchmark::State& state, const BenchmarkConfig& config) {
        size_t size = static_cast<size_t>(state.range(0));

        auto buf = setupBuffer(size, config);
        fillBuffer(buf, size);

        LIBMEM_DEBUG("%s: size=%zu, src=%p, dst=%p, mode=%s",
                    Traits::name(), size,
                    static_cast<void*>(buf.src), static_cast<void*>(buf.dst),
                    (config.cache_mode == CacheMode::Hot) ? "hot" : "cold");

        if (Traits::handles_overlap && config.overlap_type == 'd') {
            runOverlapBoth(state, buf, size, config);
        } else if (config.cache_mode == CacheMode::Hot) {
            for (auto _ : state) {
                ::benchmark::DoNotOptimize(invoke(buf, size, config));
                perIterReset(buf, size);
            }
        } else {
            for (auto _ : state) {
                state.PauseTiming();
                flushForCategory(buf, size);
                perIterReset(buf, size);
                state.ResumeTiming();
                ::benchmark::DoNotOptimize(invoke(buf, size, config));
            }
        }

        reportResult(state);
    }

private:
    // ================================================================
    // setupBuffer -- trait-driven allocation via BufferFactory
    // ================================================================

    static common::BufferPair setupBuffer(size_t size, const BenchmarkConfig& config) {
        common::AllocParams params = config.alloc_params;
        applyAlignmentOffsets(params, config);

        bool is_overlap = Traits::handles_overlap && config.overlap_type != 'n';

        if (params.page_mode == common::PageMode::PageCross) {
            uint32_t page_offset = common::PAGE_SZ / 2;
            if (size < common::PAGE_SZ)
                page_offset = size / 2;
            params.cross_offset = page_offset;
        }

        LIBMEM_DEBUG("  alloc: page_mode=%d, backend=%d, src_off=%u, dst_off=%u",
                    static_cast<int>(params.page_mode),
                    static_cast<int>(params.backend),
                    params.src_offset, params.dst_offset);

        common::BufferPair buf;

        if ((Traits::category == FunctionCategory::SET ||
             Traits::category == FunctionCategory::LENGTH) &&
            !std::is_same<Tag, common::traits::StrspnTag>::value) {
            buf = common::allocateSingle(size, params);
        } else if (is_overlap) {
            buf = common::allocateSingle(3 * size, params);
        } else if (Traits::category == FunctionCategory::CONCAT) {
            buf = common::allocateDual(size, 2 * size, params);
        } else {
            buf = common::allocateDual(size, size, params);
        }

        if (is_overlap && size > 0 && buf.valid()) {
            // CAP-COUPLED: src_off can reach 2 * size; uint32_t holds it
            // safely only while MAX_BENCHMARK_SIZE <= 2 GiB. To raise the
            // cap, widen offsets to size_t and use a 64-bit RNG.
            uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
            common::RandomPool& rp = common::globalRandomPool();
            uint32_t dst_off = rp.uniform_u32(static_cast<uint32_t>(size)) + 1;
            uint32_t src_off = dst_off +
                rp.uniform_u32(static_cast<uint32_t>(size)) + 1;
            buf.dst = base + dst_off;
            buf.src = base + src_off;
            buf.size = size;
            LIBMEM_DEBUG("  overlap: dst_off=%u, src_off=%u, gap=%u",
                        dst_off, src_off, src_off - dst_off);
        }

        return buf;
    }

    static void applyAlignmentOffsets(common::AllocParams& params,
                                      const BenchmarkConfig& config) {
        if (config.align_option == AlignOption::Default)
            return;

        if (config.align_option == AlignOption::Unaligned) {
            common::RandomPool& rp = common::globalRandomPool();
            uint32_t src_rnd = rp.uniform_u32(
                static_cast<uint32_t>(common::CL_SPILL_OFFSET)) + 1;
            uint32_t dst_rnd;
            do {
                dst_rnd = rp.uniform_u32(
                    static_cast<uint32_t>(common::CL_SPILL_OFFSET)) + 1;
            } while (dst_rnd == src_rnd);

            if (config.spill == 'm') {
                params.src_offset = static_cast<uint32_t>(
                    common::CACHE_LINE_SZ - src_rnd);
                params.dst_offset = static_cast<uint32_t>(
                    common::CACHE_LINE_SZ - dst_rnd);
            } else {
                params.src_offset = src_rnd;
                params.dst_offset = dst_rnd;
            }
        }
        // AlignOption::Aligned: offsets stay 0 (truly aligned)
    }

    // ================================================================
    // fillBuffer -- string vs memory fill
    // ================================================================

    static void fillBuffer(common::BufferPair& buf, size_t size) {
        if (size == 0) return;

        if (std::is_same<Tag, common::traits::StrspnTag>::value) {
            common::fillStrspn(buf.src, buf.dst, size);
            return;
        }

        // Plant answer at END so the function actually scans
        // `size` bytes (no early exit)
        if (Traits::category == FunctionCategory::SEARCH ||
            Traits::category == FunctionCategory::COMPARE) {
            fillForPosition(buf, size, common::MatchPosition::END);
            return;
        }

        if (Traits::is_string_func) {
            common::fillWithByte(buf.src, size, 'x');
            buf.src[size - 1] = '\0';
            if (buf.dst != buf.src) {
                std::memmove(buf.dst, buf.src, size);
            }
        } else {
            common::fillWithByte(buf.src, size, '$');
            if (buf.dst != buf.src) {
                std::memmove(buf.dst, buf.src, size);
            }
        }
    }

    // ================================================================
    // fillForPosition -- per-function filler for search/compare
    // ================================================================

    static void fillForPosition(common::BufferPair& buf, size_t size,
                                 common::MatchPosition pos) {
        if (size == 0) return;

        if (std::is_same<Tag, StrstrTag>::value) {
            common::fillStrstrPos(buf.src, buf.dst, size, pos);
        } else if (std::is_same<Tag, StrchrTag>::value) {
            common::fillSearchCharPos(buf.src, size, 'X', true, pos);
        } else if (std::is_same<Tag, MemchrTag>::value) {
            common::fillSearchCharPos(buf.src, size, 'X', false, pos);
        } else if (Traits::category == FunctionCategory::COMPARE) {
            common::fillComparePos(buf.dst, buf.src, size,
                                    Traits::is_string_func, pos);
        }
    }

    // ================================================================
    // invoke -- delegates to InvokeAdapter after selecting BufferPair pointers
    // ================================================================

    static auto invoke(common::BufferPair& buf, size_t size,
                       const BenchmarkConfig& config) -> typename Traits::ReturnType {
        using Adapter = common::traits::InvokeAdapter<Tag>;
        constexpr auto cat = Traits::category;

        if constexpr (cat == FunctionCategory::COPY) {
            if constexpr (Traits::handles_overlap) {
                if (config.overlap_type == 'f')
                    return Adapter::invoke(buf.dst, buf.src, size);
                if (config.overlap_type == 'b')
                    return Adapter::invoke(buf.src, buf.dst, size);
            }
            return Adapter::invoke(buf.dst, buf.src, size);
        }
        else if constexpr (cat == FunctionCategory::SET) {
            return Adapter::invoke(buf.dst, nullptr, size, 'x');
        }
        else if constexpr (cat == FunctionCategory::COMPARE) {
            return Adapter::invoke(buf.dst, buf.src, size);
        }
        else if constexpr (cat == FunctionCategory::SEARCH) {
            if constexpr (std::is_same_v<Tag, StrstrTag>)
                return Adapter::invoke(buf.src, buf.dst, size);
            else
                return Adapter::invoke(buf.src, nullptr, size, 'X');
        }
        else if constexpr (cat == FunctionCategory::LENGTH) {
            if constexpr (Traits::arg_count == 2 && !Traits::has_size_param)
                return Adapter::invoke(buf.src, buf.dst, size);
            else
                return Adapter::invoke(buf.src, nullptr, size);
        }
        else if constexpr (cat == FunctionCategory::CONCAT) {
            return Adapter::invoke(buf.dst, buf.src, size);
        }
    }

    // ================================================================
    // perIterReset -- CONCAT resets null terminator each iteration
    // ================================================================

    static void perIterReset(common::BufferPair& buf, size_t size) {
        if constexpr (Traits::category == FunctionCategory::CONCAT) {
            if (size > 0)
                buf.dst[size - 1] = '\0';
        }
    }

    // ================================================================
    // flushForCategory -- SINGLE layouts flush src only
    // ================================================================

    static void flushForCategory(const common::BufferPair& buf, size_t size) {
        if constexpr (std::is_same_v<Tag, common::traits::StrspnTag> ||
                      std::is_same_v<Tag, common::traits::StrstrTag>) {
            CacheControl::flushBoth(buf, size);
        } else if constexpr (Traits::category == FunctionCategory::SET ||
                             Traits::category == FunctionCategory::LENGTH ||
                             Traits::category == FunctionCategory::SEARCH) {
            CacheControl::flushSrc(buf, size);
        } else {
            CacheControl::flushBoth(buf, size);
        }
    }

    // ================================================================
    // runOverlapBoth -- both forward+backward per iteration
    // ================================================================

    static void runOverlapBoth(::benchmark::State& state, common::BufferPair& buf,
                               size_t size, const BenchmarkConfig& config) {
        if constexpr (!Traits::handles_overlap) return;
        else {
            using Adapter = common::traits::InvokeAdapter<Tag>;
            if (config.cache_mode == CacheMode::Hot) {
                for (auto _ : state) {
                    ::benchmark::DoNotOptimize(Adapter::invoke(buf.src, buf.dst, size));
                    ::benchmark::DoNotOptimize(Adapter::invoke(buf.dst, buf.src, size));
                }
            } else {
                for (auto _ : state) {
                    state.PauseTiming();
                    flushForCategory(buf, size);
                    state.ResumeTiming();
                    ::benchmark::DoNotOptimize(Adapter::invoke(buf.src, buf.dst, size));
                    ::benchmark::DoNotOptimize(Adapter::invoke(buf.dst, buf.src, size));
                }
            }
        }
    }

    // ================================================================
    // reportResult -- throughput and size counters
    // ================================================================

    static void reportResult(::benchmark::State& state) {
        state.counters["Throughput(Bytes/s)"] =
            ::benchmark::Counter(state.iterations() * state.range(0),
                                 ::benchmark::Counter::kIsRate);
        state.counters["Size(Bytes)"] =
            ::benchmark::Counter(static_cast<double>(state.range(0)),
                                 ::benchmark::Counter::kDefaults,
                                 ::benchmark::Counter::kIs1024);
    }
};

} // namespace benchmark
} // namespace libmem

#endif // LIBMEM_BENCHMARK_RUNNER_HPP
