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
    serverStats->RequestStarted(log, request, *callContext);

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
    Y_UNIT_TEST(ShouldMeasurePendingIoDepthBeforeCompletion)
    {
        ui64 nowNs = 0;
        auto timer = std::make_shared<TTestTimer>();
        auto monitoring = CreateMonitoringServiceStub();
        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            timer,
            [&] { return nowNs; });
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
            volumeStats);
        NProto::TVolume volume;
        volume.SetDiskId("volume");
        volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
        volume.SetBlockSize(DefaultBlockSize);
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        serverStats->MountVolume(volume, "client", "instance");

        TMetricRequest request{EBlockStoreRequest::ReadBlocks};
        serverStats
            ->PrepareMetricRequest(request, "client", "volume", 0, 4096, false);
        auto context = MakeIntrusive<TCallContext>();
        TLog log;
        serverStats->RequestStarted(log, request, *context);
        nowNs = 60'000'000'000ULL;
        serverStats->UpdateStats(false);

        const auto snapshot = request.VolumeInfo->GetIoDepthSnapshot();
        const auto lane = static_cast<ui32>(EBlockStoreRequest::ReadBlocks);
        UNIT_ASSERT(snapshot);
        UNIT_ASSERT(snapshot->Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->Lanes[lane].Current, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot->Lanes[lane].IntegralUs, 60'000'000);
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume")
                            ->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "volume")
                            ->GetSubgroup("instance", "instance")
                            ->GetSubgroup("cloud", "cloud")
                            ->GetSubgroup("folder", "folder")
                            ->GetSubgroup("type", "ssd")
                            ->GetSubgroup("request", "ReadBlocks");
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Count", true)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetCounter("IoDepthCurrent")->Val(),
            1);

        serverStats->RequestCompleted(
            log,
            request,
            *context,
            MakeError(E_REJECTED, "final failure"));
        const auto completed = request.VolumeInfo->GetIoDepthSnapshot();
        UNIT_ASSERT(completed->Continuous);
        UNIT_ASSERT_VALUES_EQUAL(completed->Lanes[lane].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(completed->Lanes[lane].IntegralUs, 60'000'000);
    }

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
}

}   // namespace NCloud::NBlockStore
