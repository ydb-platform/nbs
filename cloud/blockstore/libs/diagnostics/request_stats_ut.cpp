#include "request_stats.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

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
    TDuration BackoffTime;
    TDuration ShapingTime;
};

////////////////////////////////////////////////////////////////////////////////

void AddRequestStats(
    IRequestStats& requestStats,
    NCloud::NProto::EStorageMediaKind mediaKind,
    EBlockStoreRequest requestType,
    std::initializer_list<TRequest> requests,
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode)
{
    for (const auto& request: requests) {
        auto requestStarted = requestStats.RequestStarted(
            mediaKind,
            requestType,
            request.RequestBytes,
            accessMode,
            mountMode);

        requestStats.RequestCompleted(
            mediaKind,
            requestType,
            requestStarted - DurationToCyclesSafe(request.RequestTime),
            request.PostponedTime,
            request.BackoffTime,
            request.ShapingTime,
            request.RequestBytes,
            EDiagnosticsErrorKind::Success,
            NCloud::NProto::EF_NONE,
            false,
            ECalcMaxTime::ENABLE,
            0,
            accessMode,
            mountMode);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TStartEndpointMode
{
    NProto::EVolumeMountMode MountMode;
    NProto::EVolumeAccessMode AccessMode;
    const char* MountLabel;
    const char* AccessLabel;
};

constexpr TStartEndpointMode StartEndpointModes[] = {
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_READ_WRITE,
     "local",
     "read_write"},
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_READ_ONLY,
     "local",
     "read_only"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_READ_WRITE,
     "remote",
     "read_write"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_READ_ONLY,
     "remote",
     "read_only"},
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_USER_READ_ONLY,
     "local",
     "read_only"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_USER_READ_ONLY,
     "remote",
     "read_only"},
    {NProto::VOLUME_MOUNT_LOCAL,
     NProto::VOLUME_ACCESS_REPAIR,
     "local",
     "read_write"},
    {NProto::VOLUME_MOUNT_REMOTE,
     NProto::VOLUME_ACCESS_REPAIR,
     "remote",
     "read_write"},
};

IRequestStatsPtr CreateStartEndpointStats(
    NMonitoring::TDynamicCountersPtr counters,
    bool isServer,
    bool useMsUnits = false)
{
    auto timer = std::make_shared<TTestTimer>();
    EHistogramCounterOptions histogramOptions =
        EHistogramCounterOption::ReportMultipleCounters;
    if (useMsUnits) {
        histogramOptions |= EHistogramCounterOption::UseMsUnitsForTimeHistogram;
    }
    if (isServer) {
        return CreateServerRequestStats(
            std::move(counters),
            std::move(timer),
            histogramOptions,
            {});
    }
    return CreateClientRequestStats(
        std::move(counters),
        std::move(timer),
        histogramOptions);
}

NMonitoring::TDynamicCountersPtr FindStartEndpointCounters(
    const NMonitoring::TDynamicCountersPtr& counters,
    const TString& mountLabel,
    const TString& accessLabel)
{
    auto mountGroup = counters->FindSubgroup("mount_mode", mountLabel);
    UNIT_ASSERT_C(mountGroup, mountLabel);
    auto accessGroup = mountGroup->FindSubgroup("access_mode", accessLabel);
    UNIT_ASSERT_C(accessGroup, accessLabel);
    auto requestGroup = accessGroup->FindSubgroup("request", "StartEndpoint");
    UNIT_ASSERT(requestGroup);
    return requestGroup;
}

NMonitoring::TDynamicCountersPtr FindStartEndpointTimePercentiles(
    const NMonitoring::TDynamicCountersPtr& counters,
    bool useMsUnits)
{
    auto percentiles = counters->FindSubgroup("percentiles", "Time");
    UNIT_ASSERT(percentiles);
    if (!useMsUnits) {
        constexpr size_t ExpectedUnitsSubgroupCount = 1;
        UNIT_ASSERT_VALUES_EQUAL(
            ExpectedUnitsSubgroupCount,
            percentiles->ReadSnapshot().size());
        percentiles = percentiles->FindSubgroup("units", "usec");
        UNIT_ASSERT(percentiles);
    }
    const auto& percentileNames = NCloud::GetDefaultPercentileNames();
    UNIT_ASSERT_VALUES_EQUAL(
        percentileNames.size(),
        percentiles->ReadSnapshot().size());
    for (const auto& name: percentileNames) {
        UNIT_ASSERT_C(percentiles->FindCounter(name), name);
    }
    return percentiles;
}

ui64 ReadCounter(
    const NMonitoring::TDynamicCountersPtr& counters,
    const TString& name)
{
    auto counter = counters->FindCounter(name);
    UNIT_ASSERT_C(counter, name);
    return counter->Val();
}

void AssertStartEndpointCounter(
    const NMonitoring::TDynamicCountersPtr& counters,
    const TStartEndpointMode& mode,
    const TString& counterName,
    ui64 expected)
{
    auto total = counters->FindSubgroup("request", "StartEndpoint");
    UNIT_ASSERT(total);
    UNIT_ASSERT_VALUES_EQUAL(expected, ReadCounter(total, counterName));

    for (const auto* mount: {"local", "remote"}) {
        for (const auto* access: {"read_write", "read_only"}) {
            const bool selected = TString(mount) == mode.MountLabel &&
                                  TString(access) == mode.AccessLabel;
            UNIT_ASSERT_VALUES_EQUAL_C(
                selected ? expected : 0,
                ReadCounter(
                    FindStartEndpointCounters(counters, mount, access),
                    counterName),
                counterName << ": " << mount << "/" << access);
        }
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
                {{.RequestBytes = 1_MB,
                  .RequestTime = TDuration::MilliSeconds(100)}},
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

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
                {{.RequestBytes = 1_MB,
                  .RequestTime = TDuration::MilliSeconds(100)}},
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

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
                {{.RequestBytes = 1_MB,
                  .RequestTime = TDuration::MilliSeconds(100)}},
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

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
                {{.RequestBytes = 1_MB,
                  .RequestTime = TDuration::MilliSeconds(100)}},
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

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
                {{.RequestBytes = 1_MB,
                  .RequestTime = TDuration::MilliSeconds(100)}},
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

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
                {{.RequestBytes = 1_MB,
                  .RequestTime = TDuration::MilliSeconds(100)}},
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

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
                {{.RequestBytes = 1_MB,
                  .RequestTime = TDuration::MilliSeconds(100),
                  .PostponedTime = TDuration::Zero(),
                  .BackoffTime = TDuration::Zero(),
                  .ShapingTime = TDuration::Zero()}},
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

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
            TMetricRequest metricRequest{EBlockStoreRequest::WriteBlocks};
            metricRequest.MediaKind = NCloud::NProto::STORAGE_MEDIA_DEFAULT;
            requestStats->AddIncompleteStats(
                metricRequest,
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
            {{.RequestBytes = 1_MB,
              .RequestTime = TDuration::MilliSeconds(100)},
             {.RequestBytes = 1_MB,
              .RequestTime = TDuration::MilliSeconds(200)},
             {.RequestBytes = 1_MB,
              .RequestTime = TDuration::MilliSeconds(300)}},
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            EBlockStoreRequest::WriteBlocks,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(400)},
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(500)},
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(600)},
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EBlockStoreRequest::WriteBlocks,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(10)},
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(20)},
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(30)},
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

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
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100),
                 .PostponedTime = TDuration::MilliSeconds(50)},   // 50 ms
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(100)},   // 100 ms
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(300),
                 .PostponedTime = TDuration::MilliSeconds(100)},   // 200 ms
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            EBlockStoreRequest::WriteBlocks,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(400),
                 .PostponedTime = TDuration::MilliSeconds(100)},   // 300 ms
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(500),
                 .PostponedTime = TDuration::MilliSeconds(100)},   // 400 ms
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(600),
                 .PostponedTime = TDuration::MilliSeconds(250)},   // 350 ms
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EBlockStoreRequest::WriteBlocks,
            {
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(10),
                 .PostponedTime = TDuration::MilliSeconds(0)},   // 10 ms
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(20),
                 .PostponedTime = TDuration::MilliSeconds(11)},   // 9 ms
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(30),
                 .PostponedTime = TDuration::MilliSeconds(22)},   // 8 ms
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

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
                {.RequestBytes = 1_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 2_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 3_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_HDD,
            EBlockStoreRequest::WriteBlocks,
            {
                {.RequestBytes = 4_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 5_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 6_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        AddRequestStats(
            *requestStats,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            EBlockStoreRequest::WriteBlocks,
            {
                {.RequestBytes = 7_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 8_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
                {.RequestBytes = 9_MB,
                 .RequestTime = TDuration::MilliSeconds(100)},
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

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

        auto shoot = [&](auto mediaKind)
        {
            auto requestStarted = requestStats->RequestStarted(
                mediaKind,
                EBlockStoreRequest::WriteBlocks,
                1_MB,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);

            requestStats->RequestCompleted(
                mediaKind,
                EBlockStoreRequest::WriteBlocks,
                requestStarted -
                    DurationToCyclesSafe(TDuration::MilliSeconds(100)),
                TDuration::Zero(),  // postponedTime
                TDuration::Zero(),  // backoffTime
                TDuration::Zero(),  // shapingTime
                1_MB,
                EDiagnosticsErrorKind::ErrorSilent,
                NCloud::NProto::EF_SILENT,   // a stub at the moment
                false,
                ECalcMaxTime::ENABLE,
                0,
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL);
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
        auto shoot = [&](auto mediaKind, unsigned int count)
        {
            totalShots += count;
            while (count--) {
                auto requestStarted = requestStats->RequestStarted(
                    mediaKind,
                    EBlockStoreRequest::WriteBlocks,
                    1_MB,
                    NProto::VOLUME_ACCESS_READ_WRITE,
                    NProto::VOLUME_MOUNT_LOCAL);

                requestStats->RequestCompleted(
                    mediaKind,
                    EBlockStoreRequest::WriteBlocks,
                    requestStarted -
                        DurationToCyclesSafe(TDuration::MilliSeconds(100)),
                    TDuration::Zero(),  // postponedTime
                    TDuration::Zero(),  // backoffTime
                    TDuration::Zero(),  // shapingTime
                    1_MB,
                    EDiagnosticsErrorKind::ErrorSilent,
                    NCloud::NProto::EF_HW_PROBLEMS_DETECTED,
                    false,
                    ECalcMaxTime::ENABLE,
                    0,
                    NProto::VOLUME_ACCESS_READ_WRITE,
                    NProto::VOLUME_MOUNT_LOCAL);
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

    Y_UNIT_TEST(ShouldTrackExecuteTimeForDifferentSizeClassesSeparately)
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
                 .RequestTime = TDuration::MilliSeconds(1100),
                 .PostponedTime = TDuration::MilliSeconds(1000)},
                {.RequestBytes = 512_KB,   // no size class
                 .RequestTime = TDuration::MilliSeconds(3000),
                 .PostponedTime = TDuration::MilliSeconds(1000)},
                {.RequestBytes = 1_MB + 512_KB,   // second size class
                 .RequestTime = TDuration::MilliSeconds(1300),
                 .PostponedTime = TDuration::MilliSeconds(1000)},
                {.RequestBytes = 4_MB,   // no size class
                 .RequestTime = TDuration::MilliSeconds(5000),
                 .PostponedTime = TDuration::MilliSeconds(1000)},
            },
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL);

        requestStats->UpdateStats(true);

        auto us2ms = [](const ui64 us)
        {
            return TDuration::MicroSeconds(us).MilliSeconds();
        };

        {
            auto percentiles =
                totalCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto classPercentiles = percentiles->GetSubgroup(
                "sizeclass",
                ToString(TSizeInterval{4_KB, 512_KB}));

            auto p100 = classPercentiles->GetCounter("100");

            UNIT_ASSERT_VALUES_EQUAL(100, us2ms(p100->Val()));
        }

        {
            auto percentiles =
                totalCounters->GetSubgroup("percentiles", "ExecutionTime");
            auto classPercentiles = percentiles->GetSubgroup(
                "sizeclass",
                ToString(TSizeInterval{1_MB, 4_MB}));

            auto p100 = classPercentiles->GetCounter("100");

            UNIT_ASSERT_VALUES_EQUAL(300, us2ms(p100->Val()));
        }
    }

    Y_UNIT_TEST(ShouldRegisterCombinedStartEndpointModesWithoutMoreCounters)
    {
        const auto checkCase = [&](bool isServer, bool useMsUnits)
        {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto requestStats =
                CreateStartEndpointStats(counters, isServer, useMsUnits);

            UNIT_ASSERT(!counters->FindSubgroup("access_mode", "read_write"));
            UNIT_ASSERT(!counters->FindSubgroup("access_mode", "read_only"));

            const auto countAccessSeries =
                [&](const NMonitoring::TDynamicCountersPtr& mountGroup,
                    const char* mount,
                    const char* access) -> ui64
            {
                auto accessGroup =
                    mountGroup->FindSubgroup("access_mode", access);
                UNIT_ASSERT(accessGroup);
                UNIT_ASSERT_VALUES_EQUAL(2, accessGroup->ReadSnapshot().size());
                UNIT_ASSERT(accessGroup->FindCounter("HwProblems"));

                auto requestGroup =
                    FindStartEndpointCounters(counters, mount, access);
                auto percentiles =
                    FindStartEndpointTimePercentiles(requestGroup, useMsUnits);
                UNIT_ASSERT(percentiles);
                // Replace the percentile subgroup with its leaf counters,
                // then include HwProblems from the parent group.
                return requestGroup->ReadSnapshot().size() - 1 +
                       percentiles->ReadSnapshot().size() + 1;
            };

            const auto countMountSeries = [&](const char* mount) -> ui64
            {
                auto mountGroup = counters->FindSubgroup("mount_mode", mount);
                UNIT_ASSERT(mountGroup);
                UNIT_ASSERT_VALUES_EQUAL(2, mountGroup->ReadSnapshot().size());
                UNIT_ASSERT(
                    !mountGroup->FindSubgroup("request", "StartEndpoint"));
                UNIT_ASSERT(!mountGroup->FindCounter("HwProblems"));

                ui64 seriesCount = 0;
                for (const auto* access: {"read_write", "read_only"}) {
                    seriesCount += countAccessSeries(mountGroup, mount, access);
                }
                return seriesCount;
            };

            ui64 seriesCount = 0;
            for (const auto* mount: {"local", "remote"}) {
                seriesCount += countMountSeries(mount);
            }
            // The old four independent mode groups also exported 80 series.
            UNIT_ASSERT_VALUES_EQUAL(80, seriesCount);
        };

        for (const bool isServer: {false, true}) {
            for (const bool useMsUnits: {false, true}) {
                checkCase(isServer, useMsUnits);
            }
        }
    }

    Y_UNIT_TEST(ShouldTrackStartEndpointByMountAndAccessMode)
    {
        const auto checkCase =
            [&](bool isServer, bool useMsUnits, const TStartEndpointMode& mode)
        {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto requestStats =
                CreateStartEndpointStats(counters, isServer, useMsUnits);

            auto start = [&]
            {
                return requestStats->RequestStarted(
                    NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                    EBlockStoreRequest::StartEndpoint,
                    0,
                    mode.AccessMode,
                    mode.MountMode);
            };
            auto complete = [&](ui64 started, EDiagnosticsErrorKind error)
            {
                requestStats->RequestCompleted(
                    NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                    EBlockStoreRequest::StartEndpoint,
                    started - DurationToCyclesSafe(TDuration::Seconds(1)),
                    TDuration::MilliSeconds(100),
                    TDuration::MilliSeconds(200),
                    TDuration::MilliSeconds(300),
                    0,
                    error,
                    NCloud::NProto::EF_HW_PROBLEMS_DETECTED,
                    false,
                    ECalcMaxTime::ENABLE,
                    0,
                    mode.AccessMode,
                    mode.MountMode);
            };

            const auto successful = start();
            const auto failed = start();
            AssertStartEndpointCounter(counters, mode, "InProgress", 2);
            AssertStartEndpointCounter(counters, mode, "Count", 0);
            AssertStartEndpointCounter(counters, mode, "Errors", 0);

            complete(successful, EDiagnosticsErrorKind::Success);
            AssertStartEndpointCounter(counters, mode, "InProgress", 1);
            AssertStartEndpointCounter(counters, mode, "Count", 1);
            AssertStartEndpointCounter(counters, mode, "Errors", 0);

            complete(failed, EDiagnosticsErrorKind::ErrorFatal);
            AssertStartEndpointCounter(counters, mode, "InProgress", 0);
            AssertStartEndpointCounter(counters, mode, "Count", 1);
            AssertStartEndpointCounter(counters, mode, "Errors", 1);
            AssertStartEndpointCounter(counters, mode, "Errors/Fatal", 1);

            requestStats->UpdateStats(false);
            AssertStartEndpointCounter(counters, mode, "MaxInProgress", 2);
            auto selected = FindStartEndpointCounters(
                counters,
                mode.MountLabel,
                mode.AccessLabel);
            auto percentiles =
                FindStartEndpointTimePercentiles(selected, useMsUnits);
            UNIT_ASSERT(percentiles);
            UNIT_ASSERT_VALUES_EQUAL(0, ReadCounter(percentiles, "100"));

            requestStats->UpdateStats(true);
            const auto checkModeMetrics =
                [&](const char* mount, const char* access)
            {
                const bool selectedMode = TString(mount) == mode.MountLabel &&
                                          TString(access) == mode.AccessLabel;
                auto group = FindStartEndpointCounters(counters, mount, access);
                auto timePercentiles =
                    FindStartEndpointTimePercentiles(group, useMsUnits);
                UNIT_ASSERT(timePercentiles);
                for (const auto* name: {"50", "90", "99", "99.9", "100"}) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        selectedMode,
                        ReadCounter(timePercentiles, name) > 0);
                }
                for (const auto* name: {"MaxTime", "MaxTotalTime", "Time"}) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        selectedMode,
                        ReadCounter(group, name) > 0);
                }
                auto modeGroup = counters->FindSubgroup("mount_mode", mount)
                                     ->FindSubgroup("access_mode", access);
                UNIT_ASSERT_VALUES_EQUAL(
                    selectedMode ? 1 : 0,
                    ReadCounter(modeGroup, "HwProblems"));
            };

            for (const auto* mount: {"local", "remote"}) {
                for (const auto* access: {"read_write", "read_only"}) {
                    checkModeMetrics(mount, access);
                }
            }
            UNIT_ASSERT(ReadCounter(selected, "MaxTime") >= 400'000);
            UNIT_ASSERT(ReadCounter(selected, "MaxTotalTime") >= 1'000'000);
            UNIT_ASSERT(ReadCounter(selected, "Time") >= 2'000'000);
            UNIT_ASSERT_VALUES_EQUAL(1, ReadCounter(counters, "HwProblems"));
            auto total = counters->FindSubgroup("request", "StartEndpoint");
            UNIT_ASSERT(total);
            UNIT_ASSERT(ReadCounter(total, "MaxTime") >= 400'000);
            UNIT_ASSERT(ReadCounter(total, "MaxTotalTime") >= 1'000'000);
            UNIT_ASSERT(ReadCounter(total, "Time") >= 2'000'000);
            UNIT_ASSERT(
                ReadCounter(
                    FindStartEndpointTimePercentiles(total, useMsUnits),
                    "100") > 0);
        };

        for (const bool isServer: {false, true}) {
            for (const bool useMsUnits: {false, true}) {
                for (const auto& mode: StartEndpointModes) {
                    checkCase(isServer, useMsUnits, mode);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldTrackIncompleteStartEndpointByMountAndAccessMode)
    {
        const auto checkCase = [&](bool isServer,
                                   const TStartEndpointMode& mode,
                                   ECalcMaxTime calcMaxTime)
        {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto requestStats = CreateStartEndpointStats(counters, isServer);
            requestStats->RequestStarted(
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::StartEndpoint,
                0,
                mode.AccessMode,
                mode.MountMode);
            TMetricRequest metricRequest{EBlockStoreRequest::StartEndpoint};
            metricRequest.MediaKind = NCloud::NProto::STORAGE_MEDIA_DEFAULT;
            metricRequest.AccessMode = mode.AccessMode;
            metricRequest.MountMode = mode.MountMode;
            requestStats->AddIncompleteStats(
                metricRequest,
                {.TotalTime = TDuration::Seconds(20),
                 .ExecutionTime = TDuration::Seconds(12)},
                calcMaxTime);

            const auto checkUpdatedStats = [&](bool updatePercentiles)
            {
                requestStats->UpdateStats(updatePercentiles);
                AssertStartEndpointCounter(
                    counters,
                    mode,
                    "MaxTime",
                    calcMaxTime == ECalcMaxTime::ENABLE
                        ? TDuration::Seconds(12).MicroSeconds()
                        : 0);
                AssertStartEndpointCounter(
                    counters,
                    mode,
                    "MaxTotalTime",
                    TDuration::Seconds(20).MicroSeconds());
                AssertStartEndpointCounter(counters, mode, "InProgress", 1);
                AssertStartEndpointCounter(counters, mode, "Count", 0);
                AssertStartEndpointCounter(counters, mode, "Errors", 0);
                AssertStartEndpointCounter(counters, mode, "Time", 0);
            };

            for (const bool updatePercentiles: {false, true}) {
                checkUpdatedStats(updatePercentiles);
            }
        };

        for (const bool isServer: {false, true}) {
            for (const auto& mode: StartEndpointModes) {
                for (const auto calcMaxTime:
                     {ECalcMaxTime::ENABLE, ECalcMaxTime::DISABLE})
                {
                    checkCase(isServer, mode, calcMaxTime);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepOtherRequestsOutOfStartEndpointModeCounters)
    {
        const auto checkCase = [&](bool isServer)
        {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto requestStats = CreateStartEndpointStats(counters, isServer);
            AddRequestStats(
                *requestStats,
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::KickEndpoint,
                {{.RequestTime = TDuration::Seconds(1)}},
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_REMOTE);
            TMetricRequest metricRequest{EBlockStoreRequest::KickEndpoint};
            metricRequest.MediaKind = NCloud::NProto::STORAGE_MEDIA_DEFAULT;
            metricRequest.AccessMode = NProto::VOLUME_ACCESS_READ_ONLY;
            metricRequest.MountMode = NProto::VOLUME_MOUNT_REMOTE;
            requestStats->AddIncompleteStats(
                metricRequest,
                {.TotalTime = TDuration::Seconds(20),
                 .ExecutionTime = TDuration::Seconds(12)},
                ECalcMaxTime::ENABLE);
            requestStats->UpdateStats(true);

            auto kickCounters =
                counters->FindSubgroup("request", "KickEndpoint");
            UNIT_ASSERT(kickCounters);
            UNIT_ASSERT_VALUES_EQUAL(1, ReadCounter(kickCounters, "Count"));
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::Seconds(20).MicroSeconds(),
                ReadCounter(kickCounters, "MaxTotalTime"));
            for (const auto* name:
                 {"Count", "Errors", "InProgress", "MaxTime", "MaxTotalTime"})
            {
                AssertStartEndpointCounter(
                    counters,
                    StartEndpointModes[0],
                    name,
                    0);
            }
        };

        for (const bool isServer: {false, true}) {
            checkCase(isServer);
        }
    }

    Y_UNIT_TEST(ShouldCountUnknownStartEndpointAccessModeAsReadWrite)
    {
        const auto unknownAccessMode =
            static_cast<NProto::EVolumeAccessMode>(1000);

        const auto checkCase =
            [&](bool isServer, NProto::EVolumeMountMode mountMode)
        {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto requestStats = CreateStartEndpointStats(counters, isServer);
            const TStartEndpointMode expectedMode{
                mountMode,
                unknownAccessMode,
                mountMode == NProto::VOLUME_MOUNT_LOCAL ? "local" : "remote",
                "read_write"};

            const auto started = requestStats->RequestStarted(
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::StartEndpoint,
                0,
                unknownAccessMode,
                mountMode);
            AssertStartEndpointCounter(counters, expectedMode, "InProgress", 1);

            TMetricRequest metricRequest{EBlockStoreRequest::StartEndpoint};
            metricRequest.MediaKind = NCloud::NProto::STORAGE_MEDIA_DEFAULT;
            metricRequest.AccessMode = unknownAccessMode;
            metricRequest.MountMode = mountMode;
            requestStats->AddIncompleteStats(
                metricRequest,
                {.TotalTime = TDuration::Seconds(20),
                 .ExecutionTime = TDuration::Seconds(12)},
                ECalcMaxTime::ENABLE);
            requestStats->UpdateStats(false);
            AssertStartEndpointCounter(
                counters,
                expectedMode,
                "MaxTime",
                TDuration::Seconds(12).MicroSeconds());
            AssertStartEndpointCounter(
                counters,
                expectedMode,
                "MaxTotalTime",
                TDuration::Seconds(20).MicroSeconds());
            AssertStartEndpointCounter(counters, expectedMode, "Count", 0);

            requestStats->RequestCompleted(
                NCloud::NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::StartEndpoint,
                started - DurationToCyclesSafe(TDuration::Seconds(1)),
                TDuration::MilliSeconds(0),
                TDuration::MilliSeconds(0),
                TDuration::MilliSeconds(0),
                0,
                EDiagnosticsErrorKind::Success,
                NCloud::NProto::EF_NONE,
                false,
                ECalcMaxTime::ENABLE,
                0,
                unknownAccessMode,
                mountMode);
            requestStats->UpdateStats(false);
            AssertStartEndpointCounter(counters, expectedMode, "InProgress", 0);
            AssertStartEndpointCounter(counters, expectedMode, "Count", 1);
            AssertStartEndpointCounter(counters, expectedMode, "Errors", 0);
        };

        for (const bool isServer: {false, true}) {
            for (const auto mountMode:
                 {NProto::VOLUME_MOUNT_LOCAL, NProto::VOLUME_MOUNT_REMOTE})
            {
                checkCase(isServer, mountMode);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore
