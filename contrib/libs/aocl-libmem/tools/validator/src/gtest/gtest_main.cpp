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

#include "adapters/GTestRunner.hpp"
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <string>
#include <vector>

using namespace libmem::validator::testing;

void printUsage(const char* program) {
    std::printf("Usage: %s <function> [size] [src_align(0-63)] [dst_align(0-63)] [all_alignments] [--test=<name>]\n", program);
    std::printf("       %s --list-tests\n\n", program);
    std::printf("Examples:\n");
    std::printf("  %s memcpy 64                         Run all memcpy tests at size 64\n", program);
    std::printf("  %s memmove 40 --test=BackwardOverlap Run specific test\n", program);
    std::printf("  %s memcpy 128 0 0 1                  Run with all alignments\n", program);
}

int main(int argc, char** argv) {
    srand(static_cast<unsigned int>(time(nullptr)));

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            printUsage(argv[0]);
            return 0;
        }
        if (std::strcmp(argv[i], "--list-tests") == 0) {
            DynamicTestRunner::listTests();
            return 0;
        }
    }

    // Require function name as first argument
    if (argc < 2 || argv[1][0] == '-') {
        printUsage(argv[0]);
        return 1;
    }

    // Parse arguments
    DynamicTestRunner::RunConfig config;
    config.function = argv[1];

    if (argc >= 3 && argv[2][0] != '-') {
        config.size = std::strtoul(argv[2], nullptr, 10);
    }

    if (argc >= 4 && argv[3][0] != '-') {
        int val = std::atoi(argv[3]);
        if (val < 0 || val >= static_cast<int>(libmem::validator::CACHE_LINE_SZ)) {
            std::fprintf(stderr, "Error: src_align must be in range [0, %zu)\n",
                         libmem::validator::CACHE_LINE_SZ);
            return 1;
        }
        config.src_align = static_cast<uint32_t>(val);
    }

    if (argc >= 5 && argv[4][0] != '-') {
        int val = std::atoi(argv[4]);
        if (val < 0 || val >= static_cast<int>(libmem::validator::CACHE_LINE_SZ)) {
            std::fprintf(stderr, "Error: dst_align must be in range [0, %zu)\n",
                         libmem::validator::CACHE_LINE_SZ);
            return 1;
        }
        config.dst_align = static_cast<uint32_t>(val);
    }

    if (argc >= 6 && argv[5][0] != '-') {
        config.all_alignments = (std::atoi(argv[5]) != 0);
    }

    // Check for --test= flag
    for (int i = 1; i < argc; ++i) {
        if (std::strncmp(argv[i], "--test=", 7) == 0) {
            config.test_filter = argv[i] + 7;
        }
    }

    std::printf("=== libmem GTest Validator ===\n");
    std::printf("Function: %s\n", config.function.c_str());
    std::printf("Size: %zu\n", config.size);
    std::printf("Alignments: src=%u, dst=%u%s\n",
                config.src_align, config.dst_align,
                config.all_alignments ? " (testing all 64x64)" : "");
    if (!config.test_filter.empty()) {
        std::printf("Test filter: %s\n", config.test_filter.c_str());
    }
    std::printf("==============================\n\n");

    return DynamicTestRunner::run(config);
}
