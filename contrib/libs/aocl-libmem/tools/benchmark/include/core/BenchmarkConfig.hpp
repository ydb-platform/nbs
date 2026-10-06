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

#ifndef LIBMEM_BENCHMARK_CONFIG_HPP
#define LIBMEM_BENCHMARK_CONFIG_HPP

#include "alloc/AllocParams.hpp"
#include "config/Constants.hpp"
#include "config/Logging.hpp"
#include <cstdio>
#include <cstdlib>
#include <string>

namespace libmem {
namespace benchmark {

enum class CacheMode {
    Hot,
    Cold
};

enum class AlignOption : uint8_t {
    Default,    // -a d: cache-line aligned, offset 0
    Aligned,    // -a a: page-aligned, offset 0
    Unaligned,  // -a u: page-aligned, different random offsets
};

struct BenchmarkConfig {
    std::string function_name;
    CacheMode cache_mode = CacheMode::Hot;
    unsigned int size_start = 0;
    unsigned int size_end = 0;
    unsigned int iterator = 0;
    common::AllocParams alloc_params;
    AlignOption align_option = AlignOption::Default;
    char spill = 'n';
    char overlap_type = 'd';

    std::string benchName() const {
        std::string mode = (cache_mode == CacheMode::Cold) ? "_COLD" : "_HOT";
        return function_name + mode;
    }

    /**
     * Parse CLI args:
     * argv[1]=function argv[2]=mode argv[3]=start argv[4]=end
     * argv[5]=iter argv[6]=align argv[7]=spill argv[8]=page
     * argv[9]=overlap argv[10]=backend argv[11]=layout
     */
    static BenchmarkConfig fromArgs(int argc, char** argv) {
        BenchmarkConfig cfg;

        if (argc < 2) return cfg;
        cfg.function_name = argv[1];

        if (argc > 2 && argv[2] != nullptr)
            cfg.cache_mode = (*argv[2] == 'c') ? CacheMode::Cold : CacheMode::Hot;

        // CAP-COUPLED: unsigned int + std::atoi caps at INT_MAX (~2 GiB).
        // To raise MAX_BENCHMARK_SIZE above 2 GiB, widen fields to size_t
        // and switch atoi -> strtoull.
        if (argc > 3 && argv[3] != nullptr)
            cfg.size_start = static_cast<unsigned int>(std::atoi(argv[3]));
        if (argc > 4 && argv[4] != nullptr)
            cfg.size_end = static_cast<unsigned int>(std::atoi(argv[4]));

        if (argc > 5 && argv[5] != nullptr)
            cfg.iterator = static_cast<unsigned int>(std::atoi(argv[5]));

        if (cfg.size_start > common::MAX_BENCHMARK_SIZE ||
            cfg.size_end   > common::MAX_BENCHMARK_SIZE) {
            std::fprintf(stderr,
                "ERROR: size exceeds MAX_BENCHMARK_SIZE (%zu bytes).\n"
                "       size_start=%u, size_end=%u\n",
                common::MAX_BENCHMARK_SIZE, cfg.size_start, cfg.size_end);
            std::exit(1);
        }

        if (argc > 6) {
            char align = *argv[6];
            if (align == 'a') {
                cfg.align_option = AlignOption::Aligned;
                cfg.alloc_params.base_align = common::BaseAlign::Page;
            } else if (align == 'u') {
                cfg.align_option = AlignOption::Unaligned;
                cfg.alloc_params.base_align = common::BaseAlign::Page;
            }
        }

        if (argc > 7 && argv[7] != nullptr)
            cfg.spill = *argv[7];

        if (argc > 8 && argv[8] != nullptr) {
            char page = *argv[8];
            switch (page) {
                case 'x': cfg.alloc_params.page_mode = common::PageMode::PageCross; break;
                case 't': cfg.alloc_params.page_mode = common::PageMode::PageTail; break;
                case 'g': cfg.alloc_params.page_mode = common::PageMode::PageGuarded; break;
                default:  break;
            }
        }

        if (argc > 9 && argv[9] != nullptr)
            cfg.overlap_type = *argv[9];

        if (argc > 10 && argv[10] != nullptr) {
            switch (*argv[10]) {
                case 'n': cfg.alloc_params.backend = common::BackendType::OperatorNew; break;
                default:  break;
            }
        }

        if (argc > 11 && argv[11] != nullptr) {
            if (*argv[11] == 'c')
                cfg.alloc_params.contiguous = true;
        }

        LIBMEM_INFO("FUNCTION: %s  MODE: %c",
                    cfg.function_name.c_str(),
                    (cfg.cache_mode == CacheMode::Cold) ? 'c' : 'h');
        LIBMEM_INFO("SIZE: %u %u", cfg.size_start, cfg.size_end);

        return cfg;
    }
};

} // namespace benchmark
} // namespace libmem

#endif // LIBMEM_BENCHMARK_CONFIG_HPP
