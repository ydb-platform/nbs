#include "server_stats.h"

#include "config.h"
#include "dumpable.h"
#include "profile_log.h"
#include "request_stats.h"
#include "volume_stats.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/json/json_reader.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCapturingProfileLog final: IProfileLog
{
    TVector<TRecord> Records;
    void Start() override {}
    void Stop() override {}
    void Write(TRecord record) override
    {
        Records.push_back(std::move(record));
    }
    bool Flush() override { return true; }
};

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
    bool isHwProblem)
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
        ->GetSubgroup("folder", "folder");
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
            false /*not a hw problem*/);

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
    ui64 expected)
{
    auto instanceCounters =
        UpdateStatsWithRequestResultedInRetriableError(
            serverStats,
            monitoring,
            silenceRetriableErrors,
            isHwProblem);

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

        serverStats->AddIncompleteRequest(
            *callContext,
            request.VolumeInfo,
            NProto::STORAGE_MEDIA_HYBRID,
            EBlockStoreRequest::WriteBlocks,
            TRequestTime{
                .TotalTime = TDuration::Hours(1),
                .ExecutionTime = TDuration::Hours(1)
            }
        );

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

        serverStats->AddIncompleteRequest(
            *callContext,
            request.VolumeInfo,
            NProto::STORAGE_MEDIA_HYBRID,
            EBlockStoreRequest::WriteBlocks,
            TRequestTime{
                .TotalTime = TDuration::Hours(1),
                .ExecutionTime = TDuration::Hours(1)
            }
        );

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

        CheckHwProblems(serverStats, monitoring, false, false, 0);
        CheckHwProblems(serverStats, monitoring, true, false, 0);
        CheckHwProblems(serverStats, monitoring, false, true, 1);
        CheckHwProblems(serverStats, monitoring, true, true, 2);
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
    Y_UNIT_TEST(ShouldPublishSeparateTimingForSuccessAndError)
    {
        for (const bool fail: {false, true}) {
            auto timer = std::make_shared<TTestTimer>();
            auto monitoring = CreateMonitoringServiceStub();
            auto profileLog = std::make_shared<TCapturingProfileLog>();
            auto serverGroup = monitoring->GetCounters()
                                   ->GetSubgroup("counters", "blockstore");
            auto volumeStats = CreateVolumeStats(
                monitoring, {}, EVolumeStatsType::EServerStats,
                CreateWallClockTimer());
            auto serverStats = CreateServerStats(
                std::make_shared<TTestDumpable>(),
                std::make_shared<TDiagnosticsConfig>(),
                monitoring, profileLog,
                CreateServerRequestStats(
                    serverGroup, timer,
                    EHistogramCounterOption::ReportMultipleCounters, {}),
                volumeStats);
            TMetricRequest request{EBlockStoreRequest::WriteBlocks};
            serverStats->PrepareMetricRequest(
                request, "client", "volume", 0, 4096, false);
            auto context = CreateCallContext(7772);
            TLog log;
            serverStats->RequestStarted(log, request, *context, "test");
            context->AddTime(
                EProcessingStage::Postponed, TDuration::MicroSeconds(1));
            serverStats->RequestCompleted(
                log, request, *context,
                fail ? MakeError(E_REJECTED, "test") : MakeError(S_OK));
            UNIT_ASSERT_VALUES_EQUAL(profileLog->Records.size(), 1);
            const auto& record = std::get<IProfileLog::TReadWriteRequest>(
                profileLog->Records[0].Request);
            UNIT_ASSERT_VALUES_EQUAL(
                record.PostponedTime, TDuration::MicroSeconds(1));
            UNIT_ASSERT(record.RequestTimingJson.empty());
            UNIT_ASSERT(record.RequestTiming.HasData());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(
                record.RequestTiming.Serialize(), &json, true));
            UNIT_ASSERT(!json["complete"].GetBoolean());
            UNIT_ASSERT(json["without_waits_us"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(
                json["total_us"].GetUInteger(), record.Duration.MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(json["request_id"].GetUInteger(), 7772);
        }
    }

}

}   // namespace NCloud::NBlockStore
