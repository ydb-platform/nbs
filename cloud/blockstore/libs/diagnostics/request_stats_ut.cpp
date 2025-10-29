#include "request_stats.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/size_literals.h>
#include <util/system/sanitizers.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    size_t RequestBytes;
    TDuration RequestTime;
    TDuration PostponedTime;
};

////////////////////////////////////////////////////////////////////////////////

void AddRequestStats(
    IRequestStats& requestStats,
    NCloud::NProto::EStorageMediaKind mediaKind,
    EBlockStoreRequest requestType,
    std::initializer_list<TRequest> requests)
{
    for (const auto& request: requests) {
        auto requestStarted = requestStats.RequestStarted(
            mediaKind,
            requestType,
            request.RequestBytes);

        requestStats.RequestCompleted(
            mediaKind,
            requestType,
            requestStarted - DurationToCyclesSafe(request.RequestTime),
            request.PostponedTime,
            request.RequestBytes,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestStatsTest)
{
    Y_UNIT_TEST(ShouldTrackRequestsPerMediaKind)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {});

        auto totalCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");
        auto totalCount = totalCounters->GetCounter("Count");

        auto ssdCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd")
            ->GetSubgroup("request", "WriteBlocks");
        auto ssdCount = ssdCounters->GetCounter("Count");

        auto hddCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "hdd")
            ->GetSubgroup("request", "WriteBlocks");
        auto hddCount = hddCounters->GetCounter("Count");

        auto ssdNonreplCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd_nonrepl")
            ->GetSubgroup("request", "WriteBlocks");
        auto ssdNonreplCount = ssdNonreplCounters->GetCounter("Count");

        auto hddNonreplCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "hdd_nonrepl")
            ->GetSubgroup("request", "WriteBlocks");
        auto hddNonreplCount = hddNonreplCounters->GetCounter("Count");

        auto ssdMirror2Counters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd_mirror2")
            ->GetSubgroup("request", "WriteBlocks");
        auto ssdMirror2Count = ssdMirror2Counters->GetCounter("Count");

        auto ssdMirror3Counters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd_mirror3")
            ->GetSubgroup("request", "WriteBlocks");
        auto ssdMirror3Count = ssdMirror3Counters->GetCounter("Count");

        UNIT_ASSERT_EQUAL(totalCount->Val(), 0);
        UNIT_ASSERT_EQUAL(ssdCount->Val(), 0);
        UNIT_ASSERT_EQUAL(hddCount->Val(), 0);
        UNIT_ASSERT_EQUAL(ssdNonreplCount->Val(), 0);
        UNIT_ASSERT_EQUAL(hddNonreplCount->Val(), 0);
        UNIT_ASSERT_EQUAL(ssdMirror2Count->Val(), 0);
        UNIT_ASSERT_EQUAL(ssdMirror3Count->Val(), 0);

        {
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_SSD,
                EBlockStoreRequest::WriteBlocks,
            {
                { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() }
            });

            UNIT_ASSERT_EQUAL(totalCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddCount->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdNonreplCount->Val(), 0);
            UNIT_ASSERT_EQUAL(hddNonreplCount->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdMirror2Count->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdMirror3Count->Val(), 0);
        }

        {
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_HDD,
                EBlockStoreRequest::WriteBlocks,
            {
                { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() }
            });

            UNIT_ASSERT_EQUAL(totalCount->Val(), 2);
            UNIT_ASSERT_EQUAL(ssdCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdNonreplCount->Val(), 0);
            UNIT_ASSERT_EQUAL(hddNonreplCount->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdMirror2Count->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdMirror3Count->Val(), 0);
        }

        {
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                EBlockStoreRequest::WriteBlocks,
            {
                { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() }
            });

            UNIT_ASSERT_EQUAL(totalCount->Val(), 3);
            UNIT_ASSERT_EQUAL(ssdCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdNonreplCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddNonreplCount->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdMirror2Count->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdMirror3Count->Val(), 0);
        }

        {
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
                EBlockStoreRequest::WriteBlocks,
            {
                { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() }
            });

            UNIT_ASSERT_EQUAL(totalCount->Val(), 4);
            UNIT_ASSERT_EQUAL(ssdCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdNonreplCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddNonreplCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdMirror2Count->Val(), 0);
            UNIT_ASSERT_EQUAL(ssdMirror3Count->Val(), 0);
        }

        {
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2,
                EBlockStoreRequest::WriteBlocks,
            {
                { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() }
            });

            UNIT_ASSERT_EQUAL(totalCount->Val(), 5);
            UNIT_ASSERT_EQUAL(ssdCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdNonreplCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddNonreplCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdMirror2Count->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdMirror3Count->Val(), 0);
        }

        {
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
                EBlockStoreRequest::WriteBlocks,
            {
                { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() }
            });

            UNIT_ASSERT_EQUAL(totalCount->Val(), 6);
            UNIT_ASSERT_EQUAL(ssdCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdNonreplCount->Val(), 1);
            UNIT_ASSERT_EQUAL(hddNonreplCount->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdMirror2Count->Val(), 1);
            UNIT_ASSERT_EQUAL(ssdMirror3Count->Val(), 1);
        }
    }

    Y_UNIT_TEST(ShouldNotTrackRequestsForDefaultMediaKindAsHdd)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {});

        auto totalCounters =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        TIntrusivePtr<NMonitoring::TDynamicCounters> countersByMediaKind[]{
            monitoring->GetCounters()
                ->GetSubgroup("type", "ssd")
                ->GetSubgroup("request", "WriteBlocks"),
            monitoring->GetCounters()
                ->GetSubgroup("type", "hdd")
                ->GetSubgroup("request", "WriteBlocks"),
            monitoring->GetCounters()
                ->GetSubgroup("type", "ssd_nonrepl")
                ->GetSubgroup("request", "WriteBlocks"),
            monitoring->GetCounters()
                ->GetSubgroup("type", "hdd_nonrepl")
                ->GetSubgroup("request", "WriteBlocks"),
            monitoring->GetCounters()
                ->GetSubgroup("type", "ssd_mirror2")
                ->GetSubgroup("request", "WriteBlocks"),
            monitoring->GetCounters()
                ->GetSubgroup("type", "ssd_mirror3")
                ->GetSubgroup("request", "WriteBlocks")
        };

        // Add statistics for STORAGE_MEDIA_DEFAULT and check that statistics
        // are not taken into account for a specific type of media.
        {
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::WriteBlocks,
                {{1_MB, TDuration::MilliSeconds(100), TDuration::Zero()}});

            UNIT_ASSERT_EQUAL(1, totalCounters->GetCounter("Count")->Val());
            for (const auto& counter: countersByMediaKind) {
                UNIT_ASSERT_EQUAL(0, counter->GetCounter("Count")->Val());
            }
        }

        {
            requestStats->AddRetryStats(
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::WriteBlocks,
                EDiagnosticsErrorKind::ErrorRetriable,
                0);

            UNIT_ASSERT_EQUAL(
                1,
                totalCounters->GetCounter("Errors/Retriable")->Val());
            for (const auto& counter: countersByMediaKind) {
                UNIT_ASSERT_EQUAL(
                    0,
                    counter->GetCounter("Errors/Retriable")->Val());
            }
        }

        {
            const auto totalTime = TDuration::Seconds(15);
            requestStats->AddIncompleteStats(
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::WriteBlocks,
                TRequestTime{
                    .TotalTime = totalTime,
                    .ExecutionTime = totalTime},
                ECalcMaxTime::ENABLE);
            requestStats->UpdateStats(true);

            UNIT_ASSERT_EQUAL(
                totalTime.MicroSeconds(),
                totalCounters->GetCounter("MaxTime")->Val());
            for (const auto& counter: countersByMediaKind) {
                UNIT_ASSERT_EQUAL(0, counter->GetCounter("MaxTime")->Val());
            }
        }
    }

    Y_UNIT_TEST(ShouldFillTimePercentiles)
    {
        // Hdr histogram is no-op under Tsan, so just finish this test
        if (NSan::TSanIsOn()) {
            return;
        }

        auto monitoring = CreateMonitoringServiceStub();
        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {});

        auto totalCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto ssdCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd")
            ->GetSubgroup("request", "WriteBlocks");

        auto hddCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "hdd")
            ->GetSubgroup("request", "WriteBlocks");

        auto ssdNonreplCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd_nonrepl")
            ->GetSubgroup("request", "WriteBlocks");

        auto hddNonreplCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "hdd_nonrepl")
            ->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::WriteBlocks,
        {
            { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
            { 1_MB, TDuration::MilliSeconds(200), TDuration::Zero() },
            { 1_MB, TDuration::MilliSeconds(300), TDuration::Zero() },
        });

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            EBlockStoreRequest::WriteBlocks,
        {
            { 1_MB, TDuration::MilliSeconds(400), TDuration::Zero() },
            { 1_MB, TDuration::MilliSeconds(500), TDuration::Zero() },
            { 1_MB, TDuration::MilliSeconds(600), TDuration::Zero() },
        });

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EBlockStoreRequest::WriteBlocks,
        {
            { 1_MB, TDuration::MilliSeconds(10), TDuration::Zero() },
            { 1_MB, TDuration::MilliSeconds(20), TDuration::Zero() },
            { 1_MB, TDuration::MilliSeconds(30), TDuration::Zero() },
        });

        requestStats->UpdateStats(true);

        auto us2ms = [](const ui64 us)
        {
            return TDuration::MicroSeconds(us).MilliSeconds();
        };

        {
            auto percentiles = totalCounters->GetSubgroup("percentiles", "Time");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(600, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(200, us2ms(p50->Val()));
        }

        {
            auto percentiles = ssdCounters->GetSubgroup("percentiles", "Time");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(300, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(200, us2ms(p50->Val()));
        }

        {
            auto percentiles = hddCounters->GetSubgroup("percentiles", "Time");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(600, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(500, us2ms(p50->Val()));
        }

        {
            auto percentiles =
                ssdNonreplCounters->GetSubgroup("percentiles", "Time");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(30, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(20, us2ms(p50->Val()));
        }
    }

    Y_UNIT_TEST(ShouldFillExecuteTimePercentiles)
    {
        // Hdr histogram is no-op under Tsan, so just finish this test
        if (NSan::TSanIsOn()) {
            return;
        }

        auto monitoring = CreateMonitoringServiceStub();
        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {});

        auto totalCounters =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        auto ssdCounters = monitoring->GetCounters()
                               ->GetSubgroup("type", "ssd")
                               ->GetSubgroup("request", "WriteBlocks");

        auto hddCounters = monitoring->GetCounters()
                               ->GetSubgroup("type", "hdd")
                               ->GetSubgroup("request", "WriteBlocks");

        auto ssdNonreplCounters = monitoring->GetCounters()
                                      ->GetSubgroup("type", "ssd_nonrepl")
                                      ->GetSubgroup("request", "WriteBlocks");

        auto hddNonreplCounters = monitoring->GetCounters()
                                      ->GetSubgroup("type", "hdd_nonrepl")
                                      ->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::WriteBlocks,
            {
                {1_MB,
                 TDuration::MilliSeconds(100),
                 TDuration::MilliSeconds(50)},   // 50 ms
                {1_MB,
                 TDuration::MilliSeconds(200),
                 TDuration::MilliSeconds(100)},   // 100 ms
                {1_MB,
                 TDuration::MilliSeconds(300),
                 TDuration::MilliSeconds(100)},   // 200 ms
            });

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            EBlockStoreRequest::WriteBlocks,
            {
                {1_MB,
                 TDuration::MilliSeconds(400),
                 TDuration::MilliSeconds(100)},   // 300 ms
                {1_MB,
                 TDuration::MilliSeconds(500),
                 TDuration::MilliSeconds(100)},   // 400 ms
                {1_MB,
                 TDuration::MilliSeconds(600),
                 TDuration::MilliSeconds(250)},   // 350 ms
            });

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EBlockStoreRequest::WriteBlocks,
            {
                {1_MB,
                 TDuration::MilliSeconds(10),
                 TDuration::MilliSeconds(0)},   // 10 ms
                {1_MB,
                 TDuration::MilliSeconds(20),
                 TDuration::MilliSeconds(11)},   // 9 ms
                {1_MB,
                 TDuration::MilliSeconds(30),
                 TDuration::MilliSeconds(22)},   // 8 ms
            });

        requestStats->UpdateStats(true);

        auto us2ms = [](const ui64 us)
        {
            return TDuration::MicroSeconds(us).MilliSeconds();
        };

        {
            auto percentiles =
                totalCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(400, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(100, us2ms(p50->Val()));
        }

        {
            auto percentiles =
                ssdCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(200, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(100, us2ms(p50->Val()));
        }

        {
            auto percentiles =
                hddCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(400, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(350, us2ms(p50->Val()));
        }

        {
            auto percentiles =
                ssdNonreplCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(10, us2ms(p100->Val()));
            UNIT_ASSERT_VALUES_EQUAL(9, us2ms(p50->Val()));
        }
    }

    Y_UNIT_TEST(ShouldFillSizePercentiles)
    {
        // Hdr histogram is no-op under Tsan, so just finish this test
       if (NSan::TSanIsOn()) {
            return;
       }

        auto monitoring = CreateMonitoringServiceStub();
        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {});

        auto totalCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto ssdCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd")
            ->GetSubgroup("request", "WriteBlocks");

        auto hddCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "hdd")
            ->GetSubgroup("request", "WriteBlocks");

        auto ssdNonreplCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd_nonrepl")
            ->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::WriteBlocks,
        {
            { 1_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
            { 2_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
            { 3_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
        });

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            EBlockStoreRequest::WriteBlocks,
        {
            { 4_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
            { 5_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
            { 6_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
        });

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EBlockStoreRequest::WriteBlocks,
        {
            { 7_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
            { 8_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
            { 9_MB, TDuration::MilliSeconds(100), TDuration::Zero() },
        });

        requestStats->UpdateStats(true);

        {
            auto percentiles = totalCounters->GetSubgroup("percentiles", "Size");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(9445375, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(5246975, p50->Val());
        }

        {
            auto percentiles = ssdCounters->GetSubgroup("percentiles", "Size");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(3147775, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(2099199, p50->Val());
        }

        {
            auto percentiles = hddCounters->GetSubgroup("percentiles", "Size");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(6295551, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(5246975, p50->Val());
        }

        {
            auto percentiles =
                ssdNonreplCounters->GetSubgroup("percentiles", "Size");
            auto p100 = percentiles->GetCounter("100");
            auto p50 = percentiles->GetCounter("50");

            UNIT_ASSERT_VALUES_EQUAL(9445375, p100->Val());
            UNIT_ASSERT_VALUES_EQUAL(8396799, p50->Val());
        }
    }

    Y_UNIT_TEST(ShouldTrackSilentErrors)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {});

        auto totalCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("request", "WriteBlocks");

        auto ssdCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd")
            ->GetSubgroup("request", "WriteBlocks");

        auto hddCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "hdd")
            ->GetSubgroup("request", "WriteBlocks");

        auto ssdNonreplCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "ssd_nonrepl")
            ->GetSubgroup("request", "WriteBlocks");

        auto hddNonreplCounters = monitoring
            ->GetCounters()
            ->GetSubgroup("type", "hdd_nonrepl")
            ->GetSubgroup("request", "WriteBlocks");

        auto shoot = [&] (auto mediaKind) {
            auto requestStarted = requestStats->RequestStarted(
                mediaKind,
                EBlockStoreRequest::WriteBlocks,
                1_MB);

            requestStats->RequestCompleted(
                mediaKind,
                EBlockStoreRequest::WriteBlocks,
                requestStarted - DurationToCyclesSafe(TDuration::MilliSeconds(100)),
                TDuration::Zero(),
                1_MB,
                EDiagnosticsErrorKind::ErrorSilent,
                NCloud::NProto::EF_SILENT, // a stub at the moment
                false,
                ECalcMaxTime::ENABLE,
                0);
        };

        shoot(NCloud::NProto::STORAGE_MEDIA_SSD);
        shoot(NCloud::NProto::STORAGE_MEDIA_HDD);
        shoot(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        shoot(NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED);

        auto totalErrors = totalCounters->GetCounter("Errors/Silent");
        auto ssdErrors = ssdCounters->GetCounter("Errors/Silent");
        auto hddErrors = hddCounters->GetCounter("Errors/Silent");
        auto ssdNonreplErrors = ssdNonreplCounters->GetCounter("Errors/Silent");
        auto hddNonreplErrors = hddNonreplCounters->GetCounter("Errors/Silent");

        UNIT_ASSERT_VALUES_EQUAL(4, totalErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddNonreplErrors->Val());
    }

    Y_UNIT_TEST(ShouldTrackHwProblems)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {});

        unsigned int totalShots = 0;
        auto shoot = [&] (auto mediaKind, unsigned int count) {
            totalShots += count;
            while (count--) {
                auto requestStarted = requestStats->RequestStarted(
                    mediaKind,
                    EBlockStoreRequest::WriteBlocks,
                    1_MB);

                requestStats->RequestCompleted(
                    mediaKind,
                    EBlockStoreRequest::WriteBlocks,
                    requestStarted
                        - DurationToCyclesSafe(TDuration::MilliSeconds(100)),
                    TDuration::Zero(),
                    1_MB,
                    EDiagnosticsErrorKind::ErrorSilent,
                    NCloud::NProto::EF_HW_PROBLEMS_DETECTED,
                    false,
                    ECalcMaxTime::ENABLE,
                    0);
            }
        };

        shoot(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED, 1);
        shoot(NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED, 8);
        shoot(NCloud::NProto::STORAGE_MEDIA_SSD, 5);
        shoot(NCloud::NProto::STORAGE_MEDIA_HDD, 4);
        shoot(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL, 7);
        shoot(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2, 2);
        shoot(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3, 3);

        auto totalCounters = monitoring->GetCounters();
        auto getHwProblems = [&totalCounters] (const TString &type) {
            return totalCounters->GetSubgroup("type", type)
                ->GetCounter("HwProblems")->Val();
        };

        auto totalHwProblems = totalCounters->GetCounter("HwProblems")->Val();

        // Note: Total counter does not filter out reliable media requests
        UNIT_ASSERT_VALUES_EQUAL(totalShots, totalHwProblems);
        UNIT_ASSERT_VALUES_EQUAL(0, getHwProblems("hdd"));
        UNIT_ASSERT_VALUES_EQUAL(0, getHwProblems("ssd"));
        UNIT_ASSERT_VALUES_EQUAL(1, getHwProblems("ssd_nonrepl"));
        UNIT_ASSERT_VALUES_EQUAL(8, getHwProblems("hdd_nonrepl"));
        UNIT_ASSERT_VALUES_EQUAL(7, getHwProblems("ssd_local"));
        UNIT_ASSERT_VALUES_EQUAL(2, getHwProblems("ssd_mirror2"));
        UNIT_ASSERT_VALUES_EQUAL(3, getHwProblems("ssd_mirror3"));
    }

    Y_UNIT_TEST(ShouldTrackExecuteTimeForDifferentSubClassesSeparately)
    {
        // Hdr histogram is no-op under Tsan, so just finish this test
        if (NSan::TSanIsOn()) {
            return;
        }

        auto monitoring = CreateMonitoringServiceStub();

        auto requestStats = CreateServerRequestStats(
            monitoring->GetCounters(),
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters,
            {{4_KB, 512_KB}, {1_MB, 4_MB}});

        auto totalCounters =
            monitoring->GetCounters()->GetSubgroup("request", "WriteBlocks");

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD,
            EBlockStoreRequest::WriteBlocks,
            {
                {.RequestBytes = 4_KB,   // first size class
                 .RequestTime = TDuration::MilliSeconds(100),
                 .PostponedTime = TDuration::MilliSeconds(0)},
                {.RequestBytes = 512_KB,   // no size class
                 .RequestTime = TDuration::MilliSeconds(2000),
                 .PostponedTime = TDuration::MilliSeconds(0)},
                {.RequestBytes = 1_MB + 512_KB,   // second size class
                 .RequestTime = TDuration::MilliSeconds(300),
                 .PostponedTime = TDuration::MilliSeconds(0)},
                {.RequestBytes = 4_MB,   // no size class
                 .RequestTime = TDuration::MilliSeconds(4000),
                 .PostponedTime = TDuration::MilliSeconds(0)},
            });

        requestStats->UpdateStats(true);

        auto us2ms = [](const ui64 us)
        {
            return TDuration::MicroSeconds(us).MilliSeconds();
        };

        auto getNameForSizeClass = [](ui64 start, ui64 end)
        {
            return FormatByteSize(start) + "-" + FormatByteSize(end);
        };

        {
            auto percentiles =
                totalCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto classPercentiles = percentiles->GetSubgroup(
                "sizeclass",
                getNameForSizeClass(4_KB, 512_KB));

            auto p100 = classPercentiles->GetCounter("100");

            UNIT_ASSERT_VALUES_EQUAL(100, us2ms(p100->Val()));
        }

        {
            auto percentiles =
                totalCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto classPercentiles = percentiles->GetSubgroup(
                "sizeclass",
                getNameForSizeClass(1_MB, 4_MB));

            auto p100 = classPercentiles->GetCounter("100");

            UNIT_ASSERT_VALUES_EQUAL(300, us2ms(p100->Val()));
        }
    }
}

}   // namespace NCloud::NBlockStore
