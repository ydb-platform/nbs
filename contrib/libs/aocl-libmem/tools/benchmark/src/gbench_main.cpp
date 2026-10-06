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

/**
 * @file gbench_main.cpp
 * @brief Entry point for the redesigned benchmark framework.
 *
 * Replaces the monolithic gbench.cpp with a trait-dispatched runner.
 * CLI interface is identical to the original for backward compatibility
 * with the Python orchestration layer (gbm.py).
 */

#include <benchmark/benchmark.h>
#include "core/BenchmarkConfig.hpp"
#include "core/BenchmarkDispatch.hpp"
#include "config/Logging.hpp"
#include "fill/RandomPool.hpp"
#include <cstdlib>
#include <ctime>
#include <iostream>

using namespace libmem::benchmark;

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    // Seed legacy rand() and globalRandomPool() from the same clock value.
    unsigned int seed = static_cast<unsigned int>(time(nullptr));
    srand(seed);
    libmem::common::globalRandomPool().seed(static_cast<uint64_t>(seed));

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <function> <mode> <start> <end> <iter> <align> <spill>"
                  << " <page> <overlap> [backend] [layout]" << std::endl;
        std::cerr << "  align:   d=default, a=aligned, u=unaligned" << std::endl;
        std::cerr << "  page:    n=none, x=cross, t=tail, g=guarded (tail + guard)" << std::endl;
        std::cerr << "  backend: p=posix (default), n=operator_new" << std::endl;
        std::cerr << "  layout:  i=independent (default), c=contiguous" << std::endl;
        std::cerr << "  Note: layout and overlap (-o) are mutually exclusive" << std::endl;
        std::cerr << "\nSupported functions:" << std::endl;
        for (size_t i = 0; i < NUM_BENCHMARKS; ++i)
            std::cerr << "  " << BENCHMARKS[i].name << std::endl;
        return 1;
    }

    BenchmarkConfig config = BenchmarkConfig::fromArgs(argc, argv);

    const BenchmarkEntry* entry = findBenchmark(config.function_name.c_str());
    LIBMEM_DEBUG("Dispatch lookup: %s -> %s",
                config.function_name.c_str(),
                entry ? "found" : "NOT FOUND");
    if (!entry) {
        std::cerr << "ERROR: Unknown function '" << config.function_name << "'" << std::endl;
        std::cerr << "\nSupported functions:" << std::endl;
        for (size_t i = 0; i < NUM_BENCHMARKS; ++i)
            std::cerr << "  " << BENCHMARKS[i].name << std::endl;
        return 1;
    }

    BenchmarkFn fn = entry->fn;
    auto benchLambda = [&config, fn](::benchmark::State& state) {
        fn(state, config);
    };

    auto* bm = ::benchmark::RegisterBenchmark(
        config.benchName().c_str(), benchLambda)->RangeMultiplier(2);

    if (config.iterator == 0)
        bm->Range(config.size_start, config.size_end);
    else
        bm->DenseRange(config.size_start, config.size_end, config.iterator);

    ::benchmark::RunSpecifiedBenchmarks();
    return 0;
}
