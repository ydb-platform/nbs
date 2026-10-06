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

#ifndef LIBMEM_BENCHMARK_DISPATCH_HPP
#define LIBMEM_BENCHMARK_DISPATCH_HPP

#include "core/BenchmarkRunner.hpp"
#include <cstring>

namespace libmem {
namespace benchmark {

using BenchmarkFn = void(*)(::benchmark::State&, const BenchmarkConfig&);

struct BenchmarkEntry {
    const char* name;
    BenchmarkFn fn;
};

static const BenchmarkEntry BENCHMARKS[] = {
    {"memcpy",   &BenchmarkRunner<common::traits::MemcpyTag>::run},
    {"memmove",  &BenchmarkRunner<common::traits::MemmoveTag>::run},
    {"memset",   &BenchmarkRunner<common::traits::MemsetTag>::run},
    {"memcmp",   &BenchmarkRunner<common::traits::MemcmpTag>::run},
    {"memchr",   &BenchmarkRunner<common::traits::MemchrTag>::run},
    {"mempcpy",  &BenchmarkRunner<common::traits::MempcpyTag>::run},
    {"strcpy",   &BenchmarkRunner<common::traits::StrcpyTag>::run},
    {"strcmp",   &BenchmarkRunner<common::traits::StrcmpTag>::run},
    {"strncpy",  &BenchmarkRunner<common::traits::StrncpyTag>::run},
    {"strncmp",  &BenchmarkRunner<common::traits::StrncmpTag>::run},
    {"strlen",   &BenchmarkRunner<common::traits::StrlenTag>::run},
    {"strnlen",  &BenchmarkRunner<common::traits::StrnlenTag>::run},
    {"strcat",   &BenchmarkRunner<common::traits::StrcatTag>::run},
    {"strncat",  &BenchmarkRunner<common::traits::StrncatTag>::run},
    {"strstr",   &BenchmarkRunner<common::traits::StrstrTag>::run},
    {"strspn",   &BenchmarkRunner<common::traits::StrspnTag>::run},
    {"strchr",   &BenchmarkRunner<common::traits::StrchrTag>::run},
};

static constexpr size_t NUM_BENCHMARKS = sizeof(BENCHMARKS) / sizeof(BENCHMARKS[0]);

inline const BenchmarkEntry* findBenchmark(const char* name) {
    for (size_t i = 0; i < NUM_BENCHMARKS; ++i) {
        if (std::strcmp(BENCHMARKS[i].name, name) == 0)
            return &BENCHMARKS[i];
    }
    return nullptr;
}

} // namespace benchmark
} // namespace libmem

#endif // LIBMEM_BENCHMARK_DISPATCH_HPP
