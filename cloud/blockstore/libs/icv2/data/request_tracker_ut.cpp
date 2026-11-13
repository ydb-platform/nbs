#include "request_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <atomic>
#include <thread>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestTrackerTest)
{
    Y_UNIT_TEST(FailedByteReservationReleasesRequestSlot)
    {
        TDataConfig config;
        config.MaxInflightRequests = 1;
        config.MaxInflightBytes = 16;
        auto tracker = std::make_shared<TRequestTracker>(config);
        UNIT_ASSERT_VALUES_EQUAL(
            tracker->Acquire(17).GetError().GetCode(),
            E_REJECTED);
        auto reservation = tracker->Acquire(16);
        UNIT_ASSERT(!HasError(reservation.GetError()));
        UNIT_ASSERT(!HasError(tracker->CheckRunning()));
        reservation.ExtractResult().reset();
        tracker->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            tracker->CheckRunning().GetCode(),
            E_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(
            tracker->Acquire(0).GetError().GetCode(),
            E_CANCELLED);
    }

    Y_UNIT_TEST(ConcurrentReservationsRespectLimitsAndReleaseBudget)
    {
        TDataConfig config;
        config.MaxInflightRequests = 4;
        config.MaxInflightBytes = 4;
        auto tracker = std::make_shared<TRequestTracker>(config);
        std::atomic<unsigned> active = 0;
        std::atomic<bool> failed = false;
        TVector<std::thread> threads;
        threads.reserve(8);
        for (unsigned i = 0; i < 8; ++i) {
            threads.emplace_back(
                [&]
                {
                    for (unsigned j = 0; j < 1000; ++j) {
                        auto result = tracker->Acquire(1);
                        if (HasError(result.GetError())) {
                            if (result.GetError().GetCode() != E_REJECTED) {
                                failed = true;
                            }
                            continue;
                        }
                        if (active.fetch_add(1) >= 4) {
                            failed = true;
                        }
                        std::this_thread::yield();
                        active.fetch_sub(1);
                    }
                });
        }
        for (auto& thread: threads) {
            thread.join();
        }
        UNIT_ASSERT(!failed.load());
        auto fullBudget = tracker->Acquire(4);
        UNIT_ASSERT(!HasError(fullBudget.GetError()));
        fullBudget.ExtractResult().reset();
        tracker->Stop();
    }

    Y_UNIT_TEST(ConcurrentAdmissionAndStop)
    {
        for (unsigned i = 0; i < 100; ++i) {
            auto tracker = std::make_shared<TRequestTracker>(TDataConfig{});
            std::atomic<bool> start = false;
            std::atomic<bool> failed = false;
            TVector<std::thread> workers;
            workers.reserve(2);
            for (unsigned worker = 0; worker < 2; ++worker) {
                workers.emplace_back(
                    [&]
                    {
                        while (!start.load()) {
                            std::this_thread::yield();
                        }
                        for (unsigned request = 0; request < 32; ++request) {
                            auto result = tracker->Acquire(1);
                            if (HasError(result.GetError()) &&
                                result.GetError().GetCode() != E_CANCELLED)
                            {
                                failed = true;
                            }
                        }
                    });
            }
            start = true;
            tracker->Stop();
            for (auto& worker: workers) {
                worker.join();
            }
            UNIT_ASSERT(!failed.load());
            UNIT_ASSERT_VALUES_EQUAL(
                tracker->Acquire(0).GetError().GetCode(),
                E_CANCELLED);
        }
    }

    Y_UNIT_TEST(ConcurrentStopAndLastRelease)
    {
        for (unsigned i = 0; i < 100; ++i) {
            auto tracker = std::make_shared<TRequestTracker>(TDataConfig{});
            auto reservation = tracker->Acquire(1).ExtractResult();
            std::atomic<bool> start = false;
            std::thread stopping(
                [&]
                {
                    while (!start.load()) {
                        std::this_thread::yield();
                    }
                    tracker->RequestStop();
                });
            std::thread releasing(
                [&]
                {
                    start = true;
                    reservation.reset();
                });
            tracker->RequestStop();
            releasing.join();
            stopping.join();
            tracker->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                tracker->Acquire(0).GetError().GetCode(),
                E_CANCELLED);
        }
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
