#include "quota_metrics.h"

#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/mutex.h>
#include <util/thread/factory.h>

#include <semaphore>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TQuotaMetricsTest)
{
    Y_UNIT_TEST(ShouldNotCrushOnNullptrRegister)
    {
        TUsedQuotaMetrics usedQuotaMetrics(CreateWallClockTimer());

        usedQuotaMetrics.Register(nullptr);
        usedQuotaMetrics.UpdateQuota(5);
        usedQuotaMetrics.UpdateMaxQuota();
        usedQuotaMetrics.UpdateMaxQuota(15);
    }

    Y_UNIT_TEST(ShouldNotRaceOnRegisterUpdateAndUnregister)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        TUsedQuotaMetrics metrics(CreateWallClockTimer());

        const ui32 total = 42;
        ui32 waiting = 0;
        TMutex mutex;
        std::counting_semaphore<total> semaphore(0);

        TVector<THolder<IThreadFactory::IThread>> workers;
        for (size_t i = 0; i < total; ++i) {
            workers.push_back(SystemThreadFactory()->Run(
                [&metrics, counters, i, &mutex, &waiting, &semaphore]()
                {
                    with_lock (mutex) {
                        ++waiting;
                        if (waiting == total) {
                            semaphore.release(total);
                        }
                    }

                    semaphore.acquire();

                    switch (i % 21) {
                        case 0: {
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 1: {
                            metrics.Unregister();
                            metrics.Register(std::move(counters));
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 2: {
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.Register(std::move(counters));
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 3: {
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            metrics.Register(std::move(counters));
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 4: {
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            metrics.Register(std::move(counters));
                            break;
                        }
                        case 5: {
                            metrics.Unregister();
                            metrics.Register(std::move(counters));
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 6: {
                            metrics.Register(std::move(counters));
                            metrics.UpdateQuota(i);
                            metrics.Unregister();
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 7: {
                            metrics.Register(std::move(counters));
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            metrics.Unregister();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 8: {
                            metrics.Register(std::move(counters));
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            metrics.Unregister();
                            break;
                        }
                        case 9: {
                            metrics.UpdateQuota(i);
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 10: {
                            metrics.Register(std::move(counters));
                            metrics.UpdateQuota(i);
                            metrics.Unregister();
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 11: {
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateMaxQuota();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 12: {
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateMaxQuota();
                            metrics.UpdateMaxQuota(i);
                            metrics.UpdateQuota(i);
                            break;
                        }
                        case 13: {
                            metrics.UpdateMaxQuota();
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 14: {
                            metrics.Register(std::move(counters));
                            metrics.UpdateMaxQuota();
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 15: {
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateMaxQuota();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota(i);
                            break;
                        }
                        case 16: {
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota(i);
                            metrics.UpdateMaxQuota();
                            break;
                        }
                        case 17: {
                            metrics.UpdateMaxQuota(i);
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            break;
                        }
                        case 18: {
                            metrics.Register(std::move(counters));
                            metrics.UpdateMaxQuota(i);
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            break;
                        }
                        case 19: {
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateMaxQuota(i);
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota();
                            break;
                        }
                        case 20: {
                            metrics.Register(std::move(counters));
                            metrics.Unregister();
                            metrics.UpdateQuota(i);
                            metrics.UpdateMaxQuota(i);
                            metrics.UpdateMaxQuota();
                            break;
                        }
                        default:
                            break;
                    }
                }));
        }

        for (auto& worker: workers) {
            worker->Join();
        }
    }

    Y_UNIT_TEST(ShouldNotTrackQuotaMetricsBeforeRegister)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        TUsedQuotaMetrics usedQuotaMetrics(CreateWallClockTimer());

        auto usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
        auto maxUsedQuotaCounter = totalCounters->FindCounter("MaxUsedQuota");

        UNIT_ASSERT_C(
            !usedQuotaCounter,
            "usedQuotaCounter should not be initialized");
        UNIT_ASSERT_C(
            !maxUsedQuotaCounter,
            "maxUsedQuotaCounter should not be initialized");

        usedQuotaMetrics.UpdateQuota(10);
        usedQuotaMetrics.UpdateMaxQuota();

        usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
        maxUsedQuotaCounter = totalCounters->FindCounter("MaxUsedQuota");

        UNIT_ASSERT_C(
            !usedQuotaCounter,
            "usedQuotaCounter should not be initialized");
        UNIT_ASSERT_C(
            !maxUsedQuotaCounter,
            "maxUsedQuotaCounter should not be initialized");

        usedQuotaMetrics.Register(monitoring->GetCounters());

        usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
        maxUsedQuotaCounter = totalCounters->FindCounter("MaxUsedQuota");

        UNIT_ASSERT_C(
            usedQuotaCounter,
            "usedQuotaCounter should be initialized");
        UNIT_ASSERT_C(
            maxUsedQuotaCounter,
            "maxUsedQuotaCounter should be initialized");

        UNIT_ASSERT_VALUES_EQUAL(0, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(5);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(5, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(5, maxUsedQuotaCounter->Val());
    }

    Y_UNIT_TEST(ShouldUnregisterAfterSnapshotReading)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        TUsedQuotaMetrics usedQuotaMetrics(CreateWallClockTimer());

        usedQuotaMetrics.Register(monitoring->GetCounters());
        usedQuotaMetrics.UpdateQuota(10);
        usedQuotaMetrics.UpdateMaxQuota();

        {
            auto usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
            auto maxUsedQuotaCounter =
                totalCounters->FindCounter("MaxUsedQuota");

            UNIT_ASSERT_C(
                usedQuotaCounter,
                "usedQuotaCounter should be initialized");
            UNIT_ASSERT_C(
                maxUsedQuotaCounter,
                "maxUsedQuotaCounter should be initialized");

            UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaCounter->Val());
            UNIT_ASSERT_VALUES_EQUAL(10, maxUsedQuotaCounter->Val());
        }

        usedQuotaMetrics.Unregister();

        {
            auto usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
            auto maxUsedQuotaCounter =
                totalCounters->FindCounter("MaxUsedQuota");

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "usedQuotaCounter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "maxUsedQuotaCounter should not be initialized");
        }

        totalCounters->ReadSnapshot();

        {
            auto usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
            auto maxUsedQuotaCounter =
                totalCounters->FindCounter("MaxUsedQuota");

            UNIT_ASSERT_C(
                !usedQuotaCounter,
                "usedQuotaCounter should not be initialized");
            UNIT_ASSERT_C(
                !maxUsedQuotaCounter,
                "maxUsedQuotaCounter should not be initialized");
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyTrackQuotaMetrics)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        TUsedQuotaMetrics usedQuotaMetrics(CreateWallClockTimer());
        usedQuotaMetrics.Register(totalCounters);

        auto usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
        auto maxUsedQuotaCounter = totalCounters->FindCounter("MaxUsedQuota");

        UNIT_ASSERT_C(
            usedQuotaCounter,
            "usedQuotaCounter should be initialized");
        UNIT_ASSERT_C(
            maxUsedQuotaCounter,
            "maxUsedQuotaCounter should not be initialized");

        usedQuotaMetrics.UpdateQuota(10);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(5);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(5, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(15);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(15, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(5);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(5, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(10);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, maxUsedQuotaCounter->Val());
    }

    Y_UNIT_TEST(ShouldCorrectlyTrackExternalMaxUsedQuotaMetrics)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        TUsedQuotaMetrics usedQuotaMetrics(CreateWallClockTimer());
        usedQuotaMetrics.Register(totalCounters);

        auto usedQuotaCounter = totalCounters->FindCounter("UsedQuota");
        auto maxUsedQuotaCounter = totalCounters->FindCounter("MaxUsedQuota");

        UNIT_ASSERT_C(
            usedQuotaCounter,
            "usedQuotaCounter should be initialized");
        UNIT_ASSERT_C(
            maxUsedQuotaCounter,
            "maxUsedQuotaCounter should not be initialized");

        usedQuotaMetrics.UpdateQuota(10);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(5);
        usedQuotaMetrics.UpdateMaxQuota(25);

        UNIT_ASSERT_VALUES_EQUAL(5, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(25, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(15);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(15, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(5);
        usedQuotaMetrics.UpdateMaxQuota(35);

        UNIT_ASSERT_VALUES_EQUAL(5, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(35, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(10);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(10, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, maxUsedQuotaCounter->Val());

        for (ui32 i = 0; i < 12; ++i) {
            usedQuotaMetrics.UpdateQuota(10);
            usedQuotaMetrics.UpdateMaxQuota();
        }

        usedQuotaMetrics.UpdateQuota(5);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(5, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(1);
        usedQuotaMetrics.UpdateMaxQuota(40);

        UNIT_ASSERT_VALUES_EQUAL(1, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(40, maxUsedQuotaCounter->Val());

        usedQuotaMetrics.UpdateQuota(7);
        usedQuotaMetrics.UpdateMaxQuota();

        UNIT_ASSERT_VALUES_EQUAL(7, usedQuotaCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, maxUsedQuotaCounter->Val());
    }

    Y_UNIT_TEST(ShouldCalculateMaxUsedQuotaOnlyForTheLatestFifteenTicks)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto totalCounters = monitoring->GetCounters();

        TUsedQuotaMetrics usedQuotaMetrics(CreateWallClockTimer());
        usedQuotaMetrics.Register(totalCounters);

        auto usedQuotaCounter = totalCounters->FindCounter("UsedQuota");

        UNIT_ASSERT_C(
            usedQuotaCounter,
            "usedQuotaCounter should be initialized");

        auto maxUsedQuotaCounter = totalCounters->FindCounter("MaxUsedQuota");

        UNIT_ASSERT_C(
            maxUsedQuotaCounter,
            "maxUsedQuotaCounter should not be initialized");

        constexpr ui32 seconds = 15;
        constexpr ui32 requestsPerSecond = 100;
        constexpr ui32 requests = seconds * requestsPerSecond;

        for (ui32 i = 1; i <= requests; ++i) {
            usedQuotaMetrics.UpdateQuota(requests - i + 1);
            if (i % requestsPerSecond == 0) {
                usedQuotaMetrics.UpdateMaxQuota();
                UNIT_ASSERT_VALUES_EQUAL(requests, maxUsedQuotaCounter->Val());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(requests, maxUsedQuotaCounter->Val());

        for (size_t i = 1; i <= requests; ++i) {
            usedQuotaMetrics.UpdateQuota(i);
            if (i % requestsPerSecond == 0) {
                usedQuotaMetrics.UpdateMaxQuota();
                UNIT_ASSERT_VALUES_EQUAL(
                    Max(i, requests - i),
                    maxUsedQuotaCounter->Val());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(requests, maxUsedQuotaCounter->Val());
    }
}

}   // namespace NCloud::NBlockStore
