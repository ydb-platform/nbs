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

#ifndef LIBMEM_VALIDATOR_GTEST_RUNNER_HPP
#define LIBMEM_VALIDATOR_GTEST_RUNNER_HPP

#include "core/TestRunner.hpp"
#include "validators/AllValidators.hpp"

#include <string>
#include <vector>
#include <cstdio>
#include <algorithm>
#include <cctype>

namespace libmem {
namespace validator {
namespace testing {

/**
 * DynamicTestRunner - Runs tests from ValidatorRegistry with GTest-style output
 *
 * Uses ValidatorRegistry as the single source of truth for test discovery.
 * Each *Validator::registerTests() is the only place tests are listed.
 */
class DynamicTestRunner {
public:
    struct RunConfig {
        std::string function;
        size_t size;
        uint32_t src_align;
        uint32_t dst_align;
        std::string test_filter;
        bool all_alignments;

        RunConfig() : size(0), src_align(0), dst_align(0), all_alignments(false) {}
    };

    struct RunResult {
        int total;
        int passed;
        int failed;
        std::vector<std::string> failures;

        RunResult() : total(0), passed(0), failed(0) {}
    };

    static int run(const RunConfig& config) {
        auto validator = ValidatorRegistry::instance().create(config.function.c_str());
        if (!validator) {
            std::printf("[==========] No tests found for function '%s'\n",
                       config.function.c_str());
            return 1;
        }

        const auto& entries = validator->getTestEntries();

        std::vector<const TestEntry*> selected;
        for (const auto& entry : entries) {
            if (config.size == 0 && !entry.is_zero_size) continue;
            if (config.size != 0 && entry.is_zero_size) continue;

            if (!config.test_filter.empty()) {
                if (toLower(entry.instance->name()) != toLower(config.test_filter))
                    continue;
            }
            selected.push_back(&entry);
        }

        if (selected.empty()) {
            std::printf("[==========] No matching tests for function '%s'",
                       config.function.c_str());
            if (!config.test_filter.empty())
                std::printf(" (filter: %s)", config.test_filter.c_str());
            std::printf("\n");
            return 1;
        }

        std::vector<std::pair<uint32_t, uint32_t>> alignments;
        if (config.all_alignments) {
            for (uint32_t d = 0; d < VEC_SZ; ++d)
                for (uint32_t s = 0; s < VEC_SZ; ++s)
                    alignments.push_back({d, s});
        } else {
            alignments.push_back({config.dst_align, config.src_align});
        }

        int totalTests = static_cast<int>(selected.size() * alignments.size());
        RunResult result;

        std::printf("[==========] Running %d test%s from 1 test suite.\n",
                   totalTests, totalTests == 1 ? "" : "s");
        std::printf("[----------] Global test environment set-up.\n");
        std::printf("[----------] %d test%s from %s\n",
                   totalTests, totalTests == 1 ? "" : "s",
                   config.function.c_str());

        for (const auto* entry : selected) {
            for (const auto& align : alignments) {
                TestContext ctx(config.size, align.first, align.second);

                ITestCase* test = entry->instance.get();
                std::string fullName = formatTestName(
                    config.function, test->name(), ctx);
                std::printf("[ RUN      ] %s\n", fullName.c_str());

                TestResult testResult = test->execute(ctx);
                result.total++;

                if (testResult.passed) {
                    result.passed++;
                    std::printf("[       OK ] %s\n", fullName.c_str());
                } else {
                    result.failed++;
                    result.failures.push_back(fullName + ": " + testResult.message);
                    std::printf("[  FAILED  ] %s\n", fullName.c_str());
                    std::printf("             Error: %s\n", testResult.message.c_str());
                    if (testResult.error_index != static_cast<size_t>(-1)) {
                        std::printf("             Error at index: %zu\n", testResult.error_index);
                    }
                }
            }
        }

        std::printf("[----------] %d test%s from %s\n",
                   result.total, result.total == 1 ? "" : "s",
                   config.function.c_str());
        std::printf("\n");
        std::printf("[----------] Global test environment tear-down\n");
        std::printf("[==========] %d test%s from 1 test suite ran.\n",
                   result.total, result.total == 1 ? "" : "s");

        if (result.passed > 0) {
            std::printf("[  PASSED  ] %d test%s.\n",
                       result.passed, result.passed == 1 ? "" : "s");
        }

        if (result.failed > 0) {
            std::printf("[  FAILED  ] %d test%s, listed below:\n",
                       result.failed, result.failed == 1 ? "" : "s");
            for (const auto& failure : result.failures) {
                std::printf("[  FAILED  ] %s\n", failure.c_str());
            }
            std::printf("\n %d FAILED TEST%s\n",
                       result.failed, result.failed == 1 ? "" : "S");
        }

        return result.failed > 0 ? 1 : 0;
    }

    static void listTests() {
        auto& reg = ValidatorRegistry::instance();
        std::printf("Available tests:\n");
        for (const auto& funcName : reg.getFunctionNames()) {
            std::printf("\n%s:\n", funcName.c_str());
            auto validator = reg.create(funcName.c_str());
            for (const auto& entry : validator->getTestEntries()) {
                std::printf("  - %s%s\n", entry.instance->name(),
                           entry.is_zero_size ? " (zero-size)" : "");
            }
        }
    }

private:
    static std::string toLower(const std::string& str) {
        std::string result = str;
        std::transform(result.begin(), result.end(), result.begin(),
            [](unsigned char c) { return std::tolower(c); });
        return result;
    }

    static std::string toLower(const char* str) {
        return toLower(std::string(str));
    }

    static std::string formatTestName(const std::string& func,
                                       const char* test,
                                       const TestContext& ctx) {
        char buf[256];
        std::snprintf(buf, sizeof(buf), "%s/%s.Run/Size%zu_DstAlign%u_SrcAlign%u",
                     func.c_str(), test,
                     ctx.size, ctx.dst_align, ctx.src_align);
        return std::string(buf);
    }
};

} // namespace testing
} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_GTEST_RUNNER_HPP
