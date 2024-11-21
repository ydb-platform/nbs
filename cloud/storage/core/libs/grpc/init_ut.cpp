#include "init.h"

#include <contrib/libs/grpc/include/grpc/support/log.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <latch>
#include <optional>
#include <thread>

namespace NCloud::NStorage::NGrpc {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestLogBackend
    : public TLogBackend
{
    static std::atomic_int Alive;
    static std::atomic_int Writes;

    TTestLogBackend()
    {
        ++Alive;
    }

    ~TTestLogBackend() override
    {
        --Alive;
    }

    [[nodiscard]] ELogPriority FiltrationLevel() const override
    {
        return TLOG_DEBUG;
    }

    void WriteData(const TLogRecord& rec) override
    {
        Y_UNUSED(rec);
        ++Writes;
    }

    void ReopenLog() override
    {}
};

std::atomic_int TTestLogBackend::Alive;
std::atomic_int TTestLogBackend::Writes;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInitTest)
{
    Y_UNIT_TEST(ShouldDestroyLogger)
    {
        std::optional<TGrpcInitializer> init1;
        std::optional<TGrpcInitializer> init2;

        UNIT_ASSERT_VALUES_EQUAL(0, TTestLogBackend::Alive.load());
        UNIT_ASSERT_VALUES_EQUAL(0, TTestLogBackend::Writes.load());

        gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);

        // Initialize GRPC
        init1.emplace();

        gpr_log("ut", 1, GPR_LOG_SEVERITY_INFO, "default logger");

        UNIT_ASSERT_VALUES_EQUAL(0, TTestLogBackend::Writes.load());

        // Setup logger
        GrpcLoggerInit(TLog{MakeHolder<TTestLogBackend>()}, true);

        gpr_log("ut", 2, GPR_LOG_SEVERITY_INFO, "custom logger");

        UNIT_ASSERT_VALUES_EQUAL(1, TTestLogBackend::Alive.load());
        UNIT_ASSERT_LT(0, TTestLogBackend::Writes.load());

        // GRPC has already been initialized - just increment internal refcount.
        init2.emplace();

        UNIT_ASSERT_VALUES_EQUAL(1, TTestLogBackend::Alive.load());

        // Decrement internal refcount, GRPC still running.
        init1.reset();

        UNIT_ASSERT_VALUES_EQUAL(1, TTestLogBackend::Alive.load());

        gpr_log("ut", 3, GPR_LOG_SEVERITY_INFO, "custom logger");

        // Shutdown GRPC
        init2.reset();

        // Custom GRPC logger shoud be destroyed.
        UNIT_ASSERT_VALUES_EQUAL(0, TTestLogBackend::Alive.load());

        // GRPC should use the default logger (even after deinitialization).

        const int expectedWrites = TTestLogBackend::Writes.load();

        gpr_log("ut", 4, GPR_LOG_SEVERITY_INFO, "default logger again");

        UNIT_ASSERT_VALUES_EQUAL(expectedWrites, TTestLogBackend::Writes.load());
    }

    Y_UNIT_TEST(ShouldDestroyLoggerInMultithreadEnvironment)
    {
        constexpr int threadCount = 10;
        std::latch start{threadCount + 1};
        std::latch stop{threadCount};

        TVector<std::thread> threads;
        threads.reserve(threadCount);

        for (int i = 0; i != threadCount; ++i) {
            threads.emplace_back([&start, &stop, i] {
                TGrpcInitializer grpcInitializer;

                if (!i) {
                    GrpcLoggerInit(TLog{MakeHolder<TTestLogBackend>()}, true);
                }

                start.arrive_and_wait();

                gpr_log("ut", i, GPR_LOG_SEVERITY_INFO, "custom logger");

                stop.arrive_and_wait();
            });
        }

        start.arrive_and_wait();

        for (auto& t: threads) {
            t.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(0, TTestLogBackend::Alive.load());
        UNIT_ASSERT_LE(threadCount, TTestLogBackend::Writes.load());
    }
}

}   // namespace NCloud::NStorage::NGrpc
