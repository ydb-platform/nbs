#include "server_stats.h"

#include "config.h"
#include "dumpable.h"
#include "profile_log.h"
#include "request_stats.h"
#include "volume_stats.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestDumpable
    : public IDumpable
{
    void Dump(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    };

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }
};

////////////////////////////////////////////////////////////////////////////////

auto UpdateStatsWithRequestResultedInRetriableError(
    IServerStatsPtr serverStats,
    IMonitoringServicePtr monitoring,
    bool silenceRetriableErrors,
    bool isHwProblem,
    NProto::EStorageMediaKind mediaKind)
{
    TLog log;

    TMetricRequest request {EBlockStoreRequest::WriteBlocks};
    serverStats->PrepareMetricRequest(
        request,
        "client",
        "volume",
        0,
        4096,
        false);

    auto callContext = MakeIntrusive<TCallContext>();
    callContext->SetSilenceRetriableErrors(silenceRetriableErrors);

    serverStats->RequestCompleted(
        log,
        request,
        *callContext,
        MakeError(
            E_REJECTED,
            "Volume not ready",
            isHwProblem ? NCloud::NProto::EF_HW_PROBLEMS_DETECTED : 0));

    serverStats->UpdateStats(true);

    return monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "server_volume")
        ->GetSubgroup("host", "cluster")
        ->GetSubgroup("volume", "volume")
        ->GetSubgroup("instance", "instance")
        ->GetSubgroup("cloud", "cloud")
        ->GetSubgroup("folder", "folder")
        ->GetSubgroup("type", MediaKindToString(mediaKind));
}

void CheckRetriableError(
    IServerStatsPtr serverStats,
    IMonitoringServicePtr monitoring,
    bool silenceRetriableErrors,
    ui64 expected)
{
    auto instanceCounters =
        UpdateStatsWithRequestResultedInRetriableError(
            serverStats,
            monitoring,
            silenceRetriableErrors,
            false /*not a hw problem*/,
            NProto::STORAGE_MEDIA_DEFAULT);

    UNIT_ASSERT_VALUES_EQUAL(
        expected,
        instanceCounters
        ->GetSubgroup("request", "WriteBlocks")
        ->GetCounter("Errors/Retriable", true)->Val());
}

void CheckHwProblems(
    IServerStatsPtr serverStats,
    IMonitoringServicePtr monitoring,
    bool silenceRetriableErrors,
    bool isHwProblem,
    NProto::EStorageMediaKind mediaKind,
    ui64 expected)
{
    auto instanceCounters =
        UpdateStatsWithRequestResultedInRetriableError(
            serverStats,
            monitoring,
            silenceRetriableErrors,
            isHwProblem,
            mediaKind);

    UNIT_ASSERT_VALUES_EQUAL(
        expected,
        instanceCounters->GetCounter("HwProblems")->Val());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerStatsTest)
{
    Y_UNIT_TEST(ShouldTrackIncompleteRequestsPerVolume)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto monitoring = CreateMonitoringServiceStub();

        auto serverGroup = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto serverStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            std::make_shared<TDiagnosticsConfig>(),
            monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                serverGroup,
                timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
            std::move(volumeStats));

        NProto::TVolume volume;
        volume.SetDiskId("volume");
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        serverStats->MountVolume(volume, "client", "instance");

        TMetricRequest request {EBlockStoreRequest::WriteBlocks};
        serverStats->PrepareMetricRequest(
            request,
            "client",
            "volume",
            0,
            4096,
            false);

        UNIT_ASSERT_VALUES_UNEQUAL(0, request.VolumeInfo.use_count());

        auto callContext = MakeIntrusive<TCallContext>();

        request.MediaKind = NProto::STORAGE_MEDIA_HYBRID;
        serverStats->AddIncompleteRequest(
            *callContext,
            request,
            TRequestTime{
                .TotalTime = TDuration::Hours(1),
                .ExecutionTime = TDuration::Hours(1)});

        serverStats->UpdateStats(false);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Hours(1).MicroSeconds(),
            monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "volume")
            ->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "cloud")
            ->GetSubgroup("folder", "folder")
            ->GetSubgroup(
                "type",
                MediaKindToString(volume.GetStorageMediaKind()))
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("MaxTime")->Val());
    }

    Y_UNIT_TEST(ShouldSilenceErrorsIfCallContextHasSilenceRetriable)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto monitoring = CreateMonitoringServiceStub();

        auto serverGroup = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto serverStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            std::make_shared<TDiagnosticsConfig>(),
            monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                serverGroup,
                timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
            std::move(volumeStats));

        NProto::TVolume volume;
        volume.SetBlockSize(4096);
        volume.SetDiskId("volume");
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        serverStats->MountVolume(volume, "client", "instance");

        CheckRetriableError(serverStats, monitoring, false, 1);
        CheckRetriableError(serverStats, monitoring, true, 1);
    }

    Y_UNIT_TEST(ShouldNotCountMaxTimeWhenHasUncountableRejects)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto monitoring = CreateMonitoringServiceStub();

        auto serverGroup = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto serverStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            std::make_shared<TDiagnosticsConfig>(),
            monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                serverGroup,
                timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
            std::move(volumeStats));

        NProto::TVolume volume;
        volume.SetDiskId("volume");
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        serverStats->MountVolume(volume, "client", "instance");

        TMetricRequest request {EBlockStoreRequest::WriteBlocks};
        serverStats->PrepareMetricRequest(
            request,
            "client",
            "volume",
            0,
            4096,
            false);

        UNIT_ASSERT_VALUES_UNEQUAL(0, request.VolumeInfo.use_count());

        auto callContext = MakeIntrusive<TCallContext>();
        // Set flag HasUncountableRejects
        callContext->SetHasUncountableRejects();

        request.MediaKind = NProto::STORAGE_MEDIA_HYBRID;
        serverStats->AddIncompleteRequest(
            *callContext,
            request,
            TRequestTime{
                .TotalTime = TDuration::Hours(1),
                .ExecutionTime = TDuration::Hours(1)});

        serverStats->UpdateStats(false);

        // Expect MaxTime will not be calculated for server component
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration().MicroSeconds(),
            monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server")
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("MaxTime")->Val());

        // Expect MaxTime will be calculated for server_volume component
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Hours(1).MicroSeconds(),
            monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "volume")
            ->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "cloud")
            ->GetSubgroup("folder", "folder")
            ->GetSubgroup(
                "type",
                MediaKindToString(volume.GetStorageMediaKind()))
            ->GetSubgroup("request", "WriteBlocks")
            ->GetCounter("MaxTime")->Val());
    }

    void DoTestShouldCountHwProblems(
        const NCloud::NProto::EStorageMediaKind mediaKind)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto monitoring = CreateMonitoringServiceStub();

        auto serverGroup = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto serverStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            std::make_shared<TDiagnosticsConfig>(),
            monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                serverGroup,
                timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
            std::move(volumeStats));

        NProto::TVolume volume;
        volume.SetBlockSize(4096);
        volume.SetDiskId("volume");
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        volume.SetStorageMediaKind(mediaKind);
        serverStats->MountVolume(volume, "client", "instance");

        CheckHwProblems(serverStats, monitoring, false, false, mediaKind, 0);
        CheckHwProblems(serverStats, monitoring, true, false, mediaKind, 0);
        CheckHwProblems(serverStats, monitoring, false, true, mediaKind, 1);
        CheckHwProblems(serverStats, monitoring, true, true, mediaKind, 2);
    }

    Y_UNIT_TEST(ShouldCountHwProblemsSSD)
    {
        DoTestShouldCountHwProblems(
            NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldCountHwProblemsHDD)
    {
        DoTestShouldCountHwProblems(
            NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldNotReportErrorsForCellRequests)
    {
        TLog log;

        auto timer = std::make_shared<TTestTimer>();
        auto monitoring = CreateMonitoringServiceStub();

        auto serverStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            std::make_shared<TDiagnosticsConfig>(),
            monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                monitoring->GetCounters(),
                timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
            CreateVolumeStatsStub());

        TMetricRequest request {EBlockStoreRequest::DescribeVolume};
        request.CellRequest = true;
        serverStats->PrepareMetricRequest(
            request,
            "",
            "",
            0,
            0,
            false);

        auto callContext = MakeIntrusive<TCallContext>();

        serverStats->RequestStarted(
            log,
            request,
            *callContext,
            "");

        serverStats->RequestCompleted(
            log,
            request,
            *callContext,
            MakeError(S_OK, "not found")
        );

        serverStats->UpdateStats(false);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            monitoring
            ->GetCounters()
            ->GetSubgroup("request", "DescribeVolume")
            ->GetCounter("Count", true)->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            monitoring
            ->GetCounters()
            ->GetSubgroup("request", "DescribeVolume")
            ->GetCounter("Errors/Fatal")->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            monitoring
            ->GetCounters()
            ->GetSubgroup("request", "DescribeVolume")
            ->GetCounter("Errors")->Val());
    }

    Y_UNIT_TEST(ShouldReportIncompleteStartEndpointByMountAndAccessMode)
    {
        const auto checkModeCounters =
            [](const NMonitoring::TDynamicCountersPtr& counters,
               NProto::EVolumeMountMode mountMode,
               NProto::EVolumeAccessMode accessMode,
               ui64 expectedMaxTime,
               TDuration totalTime)
        {
            const auto checkGroupCounters =
                [&](NProto::EVolumeMountMode otherMountMode,
                    NProto::EVolumeAccessMode otherAccessMode)
            {
                auto group =
                    counters
                        ->GetSubgroup(
                            "mount_mode",
                            otherMountMode == NProto::VOLUME_MOUNT_LOCAL
                                ? "local"
                                : "remote")
                        ->GetSubgroup(
                            "access_mode",
                            otherAccessMode == NProto::VOLUME_ACCESS_READ_ONLY
                                ? "read_only"
                                : "read_write")
                        ->GetSubgroup("request", "StartEndpoint");
                const bool selected = otherMountMode == mountMode &&
                                      otherAccessMode == accessMode;
                UNIT_ASSERT_VALUES_EQUAL(
                    selected ? expectedMaxTime : 0,
                    group->GetCounter("MaxTime")->Val());
                UNIT_ASSERT_VALUES_EQUAL(
                    selected ? totalTime.MicroSeconds() : 0,
                    group->GetCounter("MaxTotalTime")->Val());
                UNIT_ASSERT_VALUES_EQUAL(
                    selected ? 1 : 0,
                    group->GetCounter("InProgress")->Val());
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    group->GetCounter("Count", true)->Val());
            };

            for (const auto otherMountMode:
                 {NProto::VOLUME_MOUNT_LOCAL, NProto::VOLUME_MOUNT_REMOTE})
            {
                for (const auto otherAccessMode:
                     {NProto::VOLUME_ACCESS_READ_WRITE,
                      NProto::VOLUME_ACCESS_READ_ONLY})
                {
                    checkGroupCounters(otherMountMode, otherAccessMode);
                }
            }
        };

        const auto checkCase = [&](NProto::EVolumeMountMode mountMode,
                                   NProto::EVolumeAccessMode accessMode,
                                   bool suppressMaxTime)
        {
            auto timer = std::make_shared<TTestTimer>();
            auto monitoring = CreateMonitoringServiceStub();
            auto counters = monitoring->GetCounters();
            auto serverStats = CreateServerStats(
                std::make_shared<TTestDumpable>(),
                std::make_shared<TDiagnosticsConfig>(),
                monitoring,
                CreateProfileLogStub(),
                CreateServerRequestStats(
                    counters,
                    timer,
                    EHistogramCounterOption::ReportMultipleCounters,
                    {}),
                CreateVolumeStatsStub());

            TMetricRequest request{EBlockStoreRequest::StartEndpoint};
            request.AccessMode = accessMode;
            request.MountMode = mountMode;
            auto callContext = MakeIntrusive<TCallContext>();
            if (suppressMaxTime) {
                callContext->SetHasUncountableRejects();
            }

            TLog log;
            serverStats->RequestStarted(log, request, *callContext);

            const TRequestTime time{
                .TotalTime = TDuration::Seconds(10),
                .ExecutionTime = TDuration::Seconds(6)};
            serverStats->AddIncompleteRequest(*callContext, request, time);
            serverStats->UpdateStats(false);

            const auto expectedMaxTime =
                suppressMaxTime ? 0 : time.ExecutionTime.MicroSeconds();
            auto total = counters->GetSubgroup("request", "StartEndpoint");
            UNIT_ASSERT_VALUES_EQUAL(
                expectedMaxTime,
                total->GetCounter("MaxTime")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                time.TotalTime.MicroSeconds(),
                total->GetCounter("MaxTotalTime")->Val());

            checkModeCounters(
                counters,
                mountMode,
                accessMode,
                expectedMaxTime,
                time.TotalTime);

            serverStats->RequestCompleted(log, request, *callContext, {});
            UNIT_ASSERT_VALUES_EQUAL(0, total->GetCounter("InProgress")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                total->GetCounter("Count", true)->Val());
        };

        for (const auto mountMode:
             {NProto::VOLUME_MOUNT_LOCAL, NProto::VOLUME_MOUNT_REMOTE})
        {
            for (const auto accessMode:
                 {NProto::VOLUME_ACCESS_READ_WRITE,
                  NProto::VOLUME_ACCESS_READ_ONLY})
            {
                for (const bool suppressMaxTime: {false, true}) {
                    checkCase(mountMode, accessMode, suppressMaxTime);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldExcludeOnlyOriginalProfileQuotaDelayFromLatency)
    {
        auto timer = std::make_shared<TTestTimer>();
        auto monitoring = CreateMonitoringServiceStub();

        NProto::TDiagnosticsConfig protoConfig;
        protoConfig.SetLatencyThresholdsEnabled(true);
        auto* mediaKindThresholds = protoConfig.AddLatencyThresholds();
        mediaKindThresholds->SetMediaKind(NProto::STORAGE_MEDIA_SSD);
        auto* bucket = mediaKindThresholds->AddBuckets();
        bucket->SetMinRequestBytes(0);
        bucket->SetReadThresholdMs(10);
        bucket->SetWriteThresholdMs(10);
        auto diagnosticsConfig =
            std::make_shared<TDiagnosticsConfig>(protoConfig);

        auto volumeStats = CreateVolumeStats(
            monitoring,
            diagnosticsConfig,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto serverStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            diagnosticsConfig,
            monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                monitoring->GetCounters(),
                timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
            std::move(volumeStats));

        NProto::TVolume volume;
        volume.SetBlockSize(4096);
        volume.SetDiskId("volume");
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
        serverStats->MountVolume(volume, "client", "instance");

        TMetricRequest request{EBlockStoreRequest::WriteBlocks};
        serverStats->PrepareMetricRequest(
            request,
            "client",
            "volume",
            0,
            4096,
            false);
        UNIT_ASSERT(request.VolumeInfo);

        auto counters = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "sli_volume")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "volume")
            ->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "cloud")
            ->GetSubgroup("folder", "folder")
            ->GetSubgroup("type", "network-ssd");
        auto total = counters->GetCounter("LatencyTotalOps");
        auto good = counters->GetCounter("LatencyGoodOps");
        auto skipped =
            counters->GetCounter("LatencyThresholdsSkippedOps");

        const auto makeContext = [](
            TDuration elapsed,
            TDuration throttlerDelay,
            TMaybe<TDuration> quotaDelay,
            bool quotaRejected = false)
        {
            auto callContext = MakeIntrusive<TCallContext>();
            callContext->SetRequestStartedCycles(1);
            callContext->SetResponseSentCycles(
                1 + DurationToCyclesSafe(elapsed));
            callContext->AddTime(EProcessingStage::Postponed, throttlerDelay);
            callContext->AccountThrottlerQuota(
                quotaDelay,
                throttlerDelay,
                quotaRejected);
            return callContext;
        };

        const auto record = [&](TCallContext& callContext, NProto::TError error)
        {
            serverStats->RecordLatencyCompletion(
                request,
                callContext,
                4096,
                error);
        };

        // The whole throttler wait was caused by the original profile:
        // 100ms - 95ms leaves a good 5ms.
        auto quotaContext = makeContext(
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(95),
            TDuration::MilliSeconds(95));
        record(*quotaContext, {});
        UNIT_ASSERT_VALUES_EQUAL(1, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, skipped->Val());

        // Backpressure throttling, shaping and retry backoff are service
        // latency and are not subtracted.
        auto serviceWaitContext = makeContext(
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(95),
            TDuration::Zero());
        serviceWaitContext->AddTime(
            EProcessingStage::Shaping,
            TDuration::MilliSeconds(95));
        serviceWaitContext->AddTime(
            EProcessingStage::Backoff,
            TDuration::MilliSeconds(95));
        record(*serviceWaitContext, {});
        UNIT_ASSERT_VALUES_EQUAL(2, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, skipped->Val());

        // The volume did not report the cause of the throttler wait.
        auto unknownContext = makeContext(
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(50),
            Nothing());
        record(*unknownContext, {});
        UNIT_ASSERT_VALUES_EQUAL(2, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, skipped->Val());

        // An attempt was rejected because the client exceeded its profile.
        auto rejectedContext = makeContext(
            TDuration::MilliSeconds(1),
            TDuration::Zero(),
            TDuration::Zero(),
            true);
        record(*rejectedContext, MakeError(E_BS_THROTTLED));
        UNIT_ASSERT_VALUES_EQUAL(2, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, skipped->Val());

        // A final service failure does not need a latency measurement. It
        // must keep the ordinary classifier semantics: one bad operation,
        // not a skipped operation.
        record(*quotaContext, MakeError(E_FAIL));
        UNIT_ASSERT_VALUES_EQUAL(3, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, skipped->Val());
    }
}

}   // namespace NCloud::NBlockStore
