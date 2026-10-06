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

#ifndef LIBMEM_VALIDATOR_TEST_RUNNER_HPP
#define LIBMEM_VALIDATOR_TEST_RUNNER_HPP

#include "config/Logging.hpp"
#include "core/TestCase.hpp"
#include "traits/FunctionTraits.hpp"
#include "config/Constants.hpp"
#include <vector>
#include <memory>
#include <functional>
#include <cstdio>

namespace libmem {
namespace validator {

// ============================================================================
// TestRegistry - Interface for test registration
// ============================================================================

/**
 * TestEntry - Entry in the test registry. Holds the test instance directly;
 * one instance per registered test, reused across all validate() calls.
 */
struct TestEntry {
    std::unique_ptr<ITestCase> instance;
    const char* description;
    bool is_zero_size;

    TestEntry(std::unique_ptr<ITestCase> inst, const char* desc, bool zero)
        : instance(std::move(inst)), description(desc), is_zero_size(zero) {}
};

/**
 * TestRegistry - Interface for registering tests
 */
class TestRegistry {
    std::vector<TestEntry> entries_;

public:
    /**
     * Register a regular test. Instance is constructed once at registration
     * and reused across every validate() call.
     */
    template<typename TestType>
    TestRegistry& add(const char* description) {
        entries_.emplace_back(
            std::unique_ptr<ITestCase>(new TestCaseWrapper<TestType>()),
            description,
            false);
        return *this;
    }

    /**
     * Register a zero-size test.
     */
    template<typename TestType>
    TestRegistry& zeroSize(const char* description) {
        entries_.emplace_back(
            std::unique_ptr<ITestCase>(new TestCaseWrapper<TestType>()),
            description,
            true);
        return *this;
    }

    /**
     * Check if registry has any tests
     */
    bool empty() const { return entries_.empty(); }

    /**
     * Get number of tests
     */
    size_t size() const { return entries_.size(); }

    /**
     * Get all entries
     */
    const std::vector<TestEntry>& entries() const { return entries_; }

    /**
     * Get test names
     */
    std::vector<std::string> getTestNames() const {
        std::vector<std::string> names;
        for (const auto& e : entries_) {
            names.push_back(e.instance->name());
        }
        return names;
    }
};

// ============================================================================
// TestStats - Statistics from running tests
// ============================================================================

struct TestStats {
    size_t passed;
    size_t failed;
    size_t skipped;

    TestStats() : passed(0), failed(0), skipped(0) {}

    size_t total() const { return passed + failed + skipped; }
    bool allPassed() const { return failed == 0; }

    void merge(const TestStats& other) {
        passed += other.passed;
        failed += other.failed;
        skipped += other.skipped;
    }

    std::string summary() const {
        char buf[128];
        if (allPassed()) {
            std::snprintf(buf, sizeof(buf), "PASS (%zu/%zu tests)", passed, total());
        } else {
            std::snprintf(buf, sizeof(buf), "FAIL (%zu passed, %zu failed)", passed, failed);
        }
        return std::string(buf);
    }
};

// ============================================================================
// IValidator - Interface for all validators
// ============================================================================

class IValidator {
public:
    virtual ~IValidator() = default;
    virtual const char* getName() const = 0;

    // Run every registered test at one (align1, align2) pair. No skipping.
    virtual void validate(size_t size, uint32_t align1, uint32_t align2) = 0;

    // Full VEC_SZ x VEC_SZ sweep. Alignment-insensitive tests run only
    // on the (0,0) pass; emits a LIBMEM_SUMMARY line at the end.
    virtual void validateSweep(size_t size) = 0;

    virtual const TestStats& getStats() const = 0;
    virtual std::vector<std::string> getTestNames() const = 0;
    virtual const std::vector<TestEntry>& getTestEntries() const = 0;

    common::AllocParams alloc_params;
};

// ============================================================================
// TestRunner - Unified validator base
// ============================================================================

/**
 * TestRunner - Unified base class for all validators
 */
template<typename Tag, typename Derived>
class TestRunner : public IValidator {
public:
    using Traits = traits::FunctionTraits<Tag>;

    TestRunner() {
        // Runs during base construction -- derived data members are NOT yet
        // initialized. registerTests() must only call tests().add<>() and
        // must NOT access any derived-class data members.
        static_cast<Derived*>(this)->registerTests();
    }

    virtual ~TestRunner() = default;

    // ========================================================================
    // IValidator interface
    // ========================================================================

    const char* getName() const override {
        return Traits::name();
    }

    void validate(size_t size, uint32_t align1, uint32_t align2) final override {
        // Normalize alignments
        uint32_t src_align = align1 % VEC_SZ;
        uint32_t dst_align = align2 % VEC_SZ;

        LIBMEM_DEBUG("Function: %s", Traits::name());
        LIBMEM_DEBUG("  Size: %zu bytes", size);
        LIBMEM_DEBUG("  Src alignment offset: %u (original: %u)", src_align, align1);
        LIBMEM_DEBUG("  Dst alignment offset: %u (original: %u)", dst_align, align2);
        LIBMEM_DEBUG("  VEC_SZ: %zu bytes", static_cast<size_t>(VEC_SZ));

        // Handle size==0 with zero-size tests
        if (size == 0) {
            LIBMEM_DEBUG("  Mode: Zero-size tests");
            runZeroSizeTests(dst_align, src_align);
            return;
        }

        LIBMEM_DEBUG("  Mode: Regular tests");
        LIBMEM_DEBUG("  Registered tests: %zu", registry_.size());

        // Not a sweep: counters discarded.
        size_t unused_run = 0, unused_skipped = 0, unused_passes = 0;
        runRegularTests(size, dst_align, src_align, /*inside_sweep=*/false,
                        unused_run, unused_skipped, unused_passes);
    }

    void validateSweep(size_t size) final override {
        LIBMEM_DEBUG("Function: %s", Traits::name());
        LIBMEM_DEBUG("  Size: %zu bytes", size);
        LIBMEM_DEBUG("  Mode: Alignment sweep (%zu x %zu = %zu passes)",
                  static_cast<size_t>(VEC_SZ), static_cast<size_t>(VEC_SZ),
                  static_cast<size_t>(VEC_SZ) * static_cast<size_t>(VEC_SZ));

        // Per-sweep counters: local, passed by ref to runRegularTests.
        size_t sweep_total_run = 0;
        size_t sweep_total_skipped = 0;
        size_t sweep_non_canonical_passes = 0;

        if (size == 0) {
            // Zero-size paths can be alignment-sensitive; sweep all pairs.
            for (uint32_t src = 0; src < VEC_SZ; ++src) {
                for (uint32_t dst = 0; dst < VEC_SZ; ++dst) {
                    LIBMEM_DEBUG("Sweep pass (zero-size): src=%u, dst=%u", src, dst);
                    runZeroSizeTests(dst, src);
                }
            }
            return;
        }

        for (uint32_t src = 0; src < VEC_SZ; ++src) {
            for (uint32_t dst = 0; dst < VEC_SZ; ++dst) {
                LIBMEM_DEBUG("Sweep pass: src=%u, dst=%u", src, dst);
                runRegularTests(size, dst, src, /*inside_sweep=*/true,
                                sweep_total_run, sweep_total_skipped,
                                sweep_non_canonical_passes);
            }
        }

        LIBMEM_SUMMARY("%s size=%zu sweep: ran %zu tests, skipped %zu "
                       "alignment-insensitive on %zu non-canonical passes",
                       Traits::name(), size,
                       sweep_total_run, sweep_total_skipped,
                       sweep_non_canonical_passes);
    }

    const TestStats& getStats() const override {
        return stats_;
    }

    // ========================================================================
    // Test introspection
    // ========================================================================

    std::vector<std::string> getTestNames() const override {
        return registry_.getTestNames();
    }

    const std::vector<TestEntry>& getTestEntries() const override {
        return registry_.entries();
    }

protected:
    /**
     * Get the test registry
     */
    TestRegistry& tests() { return registry_; }

private:
    TestRegistry registry_;
    TestStats stats_;

    // inside_sweep=true gates the alignment-insensitive skip. Sweep
    // counters are owned by validateSweep() and passed in by reference.
    void runRegularTests(size_t size, uint32_t dst_align, uint32_t src_align,
                         bool inside_sweep,
                         size_t& sweep_total_run,
                         size_t& sweep_total_skipped,
                         size_t& sweep_non_canonical_passes) {
        TestContext ctx(size, dst_align, src_align);
        ctx.alloc_params = this->alloc_params;

        const bool canonical_pass = (src_align == 0 && dst_align == 0);
        const bool non_canonical_in_sweep = inside_sweep && !canonical_pass;
        if (non_canonical_in_sweep) sweep_non_canonical_passes++;

        [[maybe_unused]] size_t test_num = 0;
        [[maybe_unused]] size_t total_regular = 0;

        // Count regular tests
        for (const auto& entry : registry_.entries()) {
            if (!entry.is_zero_size) total_regular++;
        }

        LIBMEM_SUBSEPARATOR();
        LIBMEM_DEBUG("Running %zu regular tests for %s", total_regular, Traits::name());

        for (const auto& entry : registry_.entries()) {
            if (entry.is_zero_size) continue;

            ITestCase* test = entry.instance.get();
            test_num++;

            if (non_canonical_in_sweep && !test->usesAlignmentContext()) {
                LIBMEM_DEBUG("Skip [%zu/%zu]: %s::%s (alignment-insensitive)",
                         test_num, total_regular, Traits::name(), test->name());
                sweep_total_skipped++;
                continue;
            }

            LIBMEM_DEBUG("Test [%zu/%zu]: %s::%s", test_num, total_regular,
                     Traits::name(), test->name());
            LIBMEM_DEBUG("  Parameters: size=%zu, dst_align=%u, src_align=%u",
                     size, dst_align, src_align);

            TestResult result = test->execute(ctx);
            if (inside_sweep) sweep_total_run++;

            if (result.passed) {
                stats_.passed++;
                LIBMEM_PASS("%s::%s [size=%zu, dst=%u, src=%u]",
                        Traits::name(), test->name(), size, dst_align, src_align);
            } else {
                stats_.failed++;
                std::printf("[FAIL]  %s::%s [size=%zu, dst=%u, src=%u]\n",
                           Traits::name(), test->name(), size, dst_align, src_align);
                std::printf("        %s\n", result.message.c_str());
            }
        }

        LIBMEM_DEBUG("Completed: %zu passed, %zu failed",
                 stats_.passed, stats_.failed);
    }

    /**
     * Run all zero-size tests
     */
    void runZeroSizeTests(uint32_t dst_align, uint32_t src_align) {
        TestContext ctx(0, dst_align, src_align);
        ctx.alloc_params = this->alloc_params;

        [[maybe_unused]] size_t test_num = 0;
        size_t total_zero = 0;

        // Count zero-size tests
        for (const auto& entry : registry_.entries()) {
            if (entry.is_zero_size) total_zero++;
        }

        if (total_zero == 0) {
            LIBMEM_DEBUG("No zero-size tests registered for %s", Traits::name());
            return;
        }

        LIBMEM_SUBSEPARATOR();
        LIBMEM_DEBUG("Running %zu zero-size tests for %s", total_zero, Traits::name());

        for (const auto& entry : registry_.entries()) {
            if (!entry.is_zero_size) continue;

            ITestCase* test = entry.instance.get();
            test_num++;

            LIBMEM_DEBUG("Test [%zu/%zu]: %s::%s (zero-size)", test_num, total_zero,
                     Traits::name(), test->name());
            LIBMEM_DEBUG("  Parameters: size=0, dst_align=%u, src_align=%u",
                     dst_align, src_align);

            TestResult result = test->execute(ctx);

            if (result.passed) {
                stats_.passed++;
                LIBMEM_PASS("%s::%s [size=0, dst=%u, src=%u]",
                        Traits::name(), test->name(), dst_align, src_align);
            } else {
                stats_.failed++;
                std::printf("[FAIL]  %s::%s [size=0, dst=%u, src=%u]\n",
                           Traits::name(), test->name(), dst_align, src_align);
                std::printf("        %s\n", result.message.c_str());
            }
        }

        LIBMEM_DEBUG("Completed: %zu passed, %zu failed",
                 stats_.passed, stats_.failed);
    }
};

// ============================================================================
// ValidatorRegistry - Global registry for all validators
// ============================================================================

/**
 * ValidatorRegistry - Singleton registry for validator factories
 */
class ValidatorRegistry {
public:
    using FactoryFn = std::function<std::unique_ptr<IValidator>()>;

    static ValidatorRegistry& instance() {
        static ValidatorRegistry reg;
        return reg;
    }

    /**
     * Register a validator factory
     */
    void registerValidator(const char* name, FactoryFn factory) {
        factories_.push_back(std::make_pair(std::string(name), factory));
    }

    /**
     * Create a validator by name
     */
    std::unique_ptr<IValidator> create(const char* name) const {
        for (const auto& entry : factories_) {
            if (entry.first == name) {
                return entry.second();
            }
        }
        return nullptr;
    }

    /**
     * Get all registered function names
     */
    std::vector<std::string> getFunctionNames() const {
        std::vector<std::string> names;
        for (const auto& entry : factories_) {
            names.push_back(entry.first);
        }
        return names;
    }

    /**
     * Check if a function is registered
     */
    bool isRegistered(const char* name) const {
        for (const auto& entry : factories_) {
            if (entry.first == name) return true;
        }
        return false;
    }

private:
    ValidatorRegistry() = default;
    std::vector<std::pair<std::string, FactoryFn>> factories_;
};

/**
 * Helper macro to auto-register validators
 */
#define REGISTER_VALIDATOR(ValidatorClass, Name) \
    namespace { \
        struct ValidatorClass##Registrar { \
            ValidatorClass##Registrar() { \
                ValidatorRegistry::instance().registerValidator( \
                    Name, \
                    []() { return std::unique_ptr<IValidator>(new ValidatorClass()); } \
                ); \
            } \
        } g_##ValidatorClass##Registrar; \
    }

} // namespace validator
} // namespace libmem

#endif // LIBMEM_VALIDATOR_TEST_RUNNER_HPP

