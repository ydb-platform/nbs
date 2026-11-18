#include "latency_sli.h"

#include "config.h"
#include "dumpable.h"
#include "request_stats.h"
#include "server_stats.h"
#include "volume_stats.h"

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/cputimer.h>

#include <optional>
#include <thread>

namespace NCloud::NBlockStore {
namespace {

struct TDumpable final: IDumpable
{
    void Dump(IOutputStream&) const override {}
    void DumpHtml(IOutputStream&) const override {}
};

// Forward the production counters, but control the SLI completion duration.
struct TControlledRequestStats final: IRequestStats
{
    IRequestStatsPtr Delegate;
    TDuration Elapsed;

    explicit TControlledRequestStats(IRequestStatsPtr delegate)
        : Delegate(std::move(delegate))
    {}

    ui64 RequestStarted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestBytes,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode) override
    {
        return Delegate->RequestStarted(
            mediaKind, requestType, requestBytes, accessMode, mountMode);
    }

    TDuration RequestCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        TDuration backoffTime,
        TDuration shapingTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calcMaxTime,
        ui64 responseSent,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode) override
    {
        Delegate->RequestCompleted(
            mediaKind,
            requestType,
            requestStarted,
            postponedTime,
            backoffTime,
            shapingTime,
            requestBytes,
            errorKind,
            errorFlags,
            unaligned, calcMaxTime, responseSent, accessMode, mountMode);
        return Elapsed;
    }

    void AddIncompleteStats(
        const TMetricRequest& metricRequest,
        TRequestTime requestTime, ECalcMaxTime calcMaxTime) override
    {
        Delegate->AddIncompleteStats(metricRequest, requestTime, calcMaxTime);
    }

    void AddRetryStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind, ui32 errorFlags) override
    {
        Delegate->AddRetryStats(mediaKind, requestType, errorKind, errorFlags);
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        Delegate->RequestPostponed(requestType);
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        Delegate->RequestPostponedServer(requestType);
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        Delegate->RequestAdvanced(requestType);
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        Delegate->RequestAdvancedServer(requestType);
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        Delegate->RequestFastPathHit(requestType);
    }

    void BatchCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        Delegate->BatchCompleted(
            mediaKind, requestType, count, bytes, errors, timeHist, sizeHist);
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        Delegate->UpdateStats(updateIntervalFinished);
    }
};

struct TCompletion
{
    ui64 PostponedUs = 0;
    ui32 Error = S_OK;
    bool Write = false;
    ui64 Bytes = 4096;
    bool Forwarded = false;
    ui64 BackoffUs = 0;
    ui64 OriginalBytes = 0;
    ui64 SendDelayUs = 0;
    std::optional<ui64> ThrottleUs = 0;
};

struct TFixture
{
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    IVolumeStatsPtr Volumes;
    IServerStatsPtr Server;
    std::shared_ptr<TControlledRequestStats> RequestStats;
    NMonitoring::TDynamicCountersPtr Sli;

    explicit TFixture(
        std::optional<bool> enabled = true,
        NProto::EStorageMediaKind mediaKind =
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
        bool isSystem = false)
    {
        NProto::TDiagnosticsConfig proto;
        if (enabled) {
            proto.SetEnableLatencySli(*enabled);
        }
        for (bool write: {false, true}) {
            auto* row = proto.AddLatencySliThresholds();
            row->SetMediaKind(mediaKind);
            row->SetWrite(write);
            row->SetStartBytes(1);
            row->SetEndBytes(8193);
            row->SetThresholdUs(write ? 2000 : 1000);
        }
        auto config = std::make_shared<TDiagnosticsConfig>(proto);
        Volumes = CreateVolumeStats(Monitoring, config, TDuration::Minutes(15),
            EVolumeStatsType::EServerStats, Timer);
        RequestStats =
            std::make_shared<TControlledRequestStats>(CreateServerRequestStats(
                Monitoring->GetCounters(),
                Timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}));
        Server = CreateServerStats(
            std::make_shared<TDumpable>(),
            config, Monitoring, nullptr, RequestStats, Volumes);
        NProto::TVolume volume;
        volume.SetDiskId("disk");
        volume.SetBlockSize(4096);
        volume.SetStorageMediaKind(mediaKind);
        volume.SetIsSystem(isSystem);
        Server->MountVolume(volume, "client", "instance");
        auto group = Monitoring->GetCounters()->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "sli_volume")->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "disk")->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "")->GetSubgroup("folder", "")
            ->GetSubgroup("type", MediaKindToComputeType(mediaKind));
        Sli = group;
    }

    NMonitoring::TDynamicCountersPtr Reads() const
    {
        return Sli->FindSubgroup("request", "ReadBlocks");
    }

    NMonitoring::TDynamicCountersPtr Writes() const
    {
        return Sli->FindSubgroup("request", "WriteBlocks");
    }

    void Complete(ui64 elapsedUs, TCompletion op = {})
    {
        TMetricRequest metric{
            op.Write ? EBlockStoreRequest::WriteBlocksLocal
                     : EBlockStoreRequest::ReadBlocksLocal};
        Server->PrepareMetricRequest(metric, "client", "disk", 0, op.Bytes,
                                     false);
        metric.ForwardedRequest = op.Forwarded;
        if (op.OriginalBytes) {
            metric.OriginalRequestBytes = op.OriginalBytes;
        }
        auto ctx = CreateCallContext();
        TLog log;
        Server->RequestStarted(log, metric, *ctx);
        const auto start = GetCycleCount() - DurationToCyclesSafe(TDuration::Seconds(10));
        ctx->SetRequestStartedCycles(start);
        ctx->SetResponseSentCycles(start + DurationToCyclesSafe(
            TDuration::MicroSeconds(elapsedUs)));
        ctx->AddTime(EProcessingStage::Postponed,
                     TDuration::MicroSeconds(op.PostponedUs));
        ctx->AddTime(EProcessingStage::Backoff,
                     TDuration::MicroSeconds(op.BackoffUs));
        if (op.ThrottleUs) {
            ctx->AddLatencyThrottleDelay(
                TDuration::MicroSeconds(*op.ThrottleUs));
        } else {
            ctx->MarkLatencyTimingIncomplete();
        }
        ctx->SetSilenceRetriableErrors(true);
        RequestStats->Elapsed =
            TDuration::MicroSeconds(elapsedUs + op.SendDelayUs);
        Server->RequestCompleted(log, metric, *ctx, MakeError(op.Error));
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TLatencySliTest)
{
    Y_UNIT_TEST(ShouldPublishLatencyCountersOnlyInSliVolume)
    {
        TFixture f;
        f.Complete(500);
        f.Server->UpdateStats(true);
        auto legacy = f.Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "disk")
            ->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "")->GetSubgroup("folder", "")
            ->GetSubgroup("type", "ssd_nonrepl");
        for (const auto* name:
             {"LatencyGoodOps", "LatencyBadOps", "LatencyTotalOps", "LatencyUnknownOps"})
        {
            UNIT_ASSERT(f.Reads()->FindCounter(name));
            UNIT_ASSERT(f.Writes()->FindCounter(name));
            UNIT_ASSERT(!legacy->FindCounter(name));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->FindCounter("LatencyGoodOps")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, legacy->GetSubgroup("request", "ReadBlocks")
                   ->GetCounter("Count", true)->Val());
    }

    Y_UNIT_TEST(ShouldRelabelAndTrimLatencyCountersWithTheirVolume)
    {
        TFixture f;
        TMetricRequest pending{EBlockStoreRequest::ReadBlocks};
        auto context = CreateCallContext();
        TLog log;
        f.Server->PrepareMetricRequest(
            pending, "client", "disk", 0, 4096, false);
        f.Server->RequestStarted(log, pending, *context);
        auto root = f.Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "sli_volume")
            ->GetSubgroup("host", "cluster");

        f.Volumes->AlterVolume("disk", "cloud", "folder");
        auto volumeGroup = root->FindSubgroup("volume", "disk");
        UNIT_ASSERT(volumeGroup);
        auto instance = volumeGroup->FindSubgroup("instance", "instance");
        UNIT_ASSERT(instance);
        UNIT_ASSERT(!instance->FindSubgroup("cloud", ""));
        auto group = instance->GetSubgroup("cloud", "cloud")
                         ->GetSubgroup("folder", "folder")
                         ->GetSubgroup("type", "network-ssd-nonreplicated")
                         ->GetSubgroup("request", "ReadBlocks");
        UNIT_ASSERT(group->FindCounter("LatencyTotalOps"));
        f.Server->RequestCompleted(log, pending, *context, MakeError(E_IO));
        UNIT_ASSERT_VALUES_EQUAL(1, group->FindCounter("LatencyBadOps")->Val());
        f.Complete(500);
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(
            2, group->FindCounter("LatencyTotalOps")->Val());

        f.Timer->AdvanceTime(TDuration::Minutes(8));
        auto volume = f.Volumes->GetVolumeInfo("disk", "client")->GetInfo();
        f.Volumes->MountVolume(volume, "client2", "instance2");
        f.Timer->AdvanceTime(TDuration::Minutes(8));
        f.Volumes->TrimVolumes();
        UNIT_ASSERT(!volumeGroup->FindSubgroup("instance", "instance"));
        UNIT_ASSERT(volumeGroup->FindSubgroup("instance", "instance2"));

        f.Timer->AdvanceTime(TDuration::Minutes(8));
        f.Volumes->TrimVolumes();
        UNIT_ASSERT(!root->FindSubgroup("volume", "disk"));
    }

    Y_UNIT_TEST(ShouldShareDiskTypeLabelsWithAvailability)
    {
        for (int value = NCloud::NProto::EStorageMediaKind_MIN;
             value <= NCloud::NProto::EStorageMediaKind_MAX;
             ++value)
        {
            if (!NCloud::NProto::EStorageMediaKind_IsValid(value)) {
                continue;
            }
            const auto mediaKind = static_cast<NProto::EStorageMediaKind>(value);
            for (bool isSystem: {false, true}) {
                TFixture f(true, mediaKind, isSystem);
                f.Complete(500);
                f.Timer->AdvanceTime(TDuration::Seconds(15));
                f.Server->UpdateStats(true);
                for (const auto* name:
                     {"ObservedSeconds", "AvailableSeconds", "HealthySeconds"})
                {
                    UNIT_ASSERT_C(f.Sli->FindCounter(name), name);
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    15, f.Sli->FindCounter("ObservedSeconds")->Val());
                UNIT_ASSERT_VALUES_EQUAL(
                    1, f.Reads()->FindCounter("LatencyGoodOps")->Val());

                auto folder = f.Monitoring->GetCounters()
                    ->GetSubgroup("counters", "blockstore")
                    ->GetSubgroup("component", "sli_volume")
                    ->GetSubgroup("host", "cluster")
                    ->GetSubgroup("volume", "disk")
                    ->GetSubgroup("instance", "instance")
                    ->GetSubgroup("cloud", "")->GetSubgroup("folder", "");
                const auto legacyType =
                    MediaKindToStatsString(isSystem, mediaKind);
                if (legacyType != MediaKindToComputeType(mediaKind)) {
                    UNIT_ASSERT(!folder->FindSubgroup("type", legacyType));
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldUseManualSizeBoundariesAndInclusiveThreshold)
    {
        auto config = TLatencySliConfig::TryParse("1;0:1:4097:100;0:4097:8193:200;1:1:8193:300").value();
        UNIT_ASSERT(config.Classify(false, 4096, 100, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 4096, 101, false) == ELatencySliResult::Bad);
        UNIT_ASSERT(config.Classify(false, 4097, 200, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 8193, 1, false) == ELatencySliResult::Unknown);
        UNIT_ASSERT(config.Classify(true, 4096, 250, false) == ELatencySliResult::Good);
        UNIT_ASSERT_VALUES_EQUAL(config.Serialize(), TLatencySliConfig::TryParse(config.Serialize()).value().Serialize());
    }

    Y_UNIT_TEST(ShouldRejectInvalidOrOverlappingTables)
    {
        for (const TString value: {"1;0:5:5:100", "1;0:1:9:0", "1;0:1:9:100;0:8:10:200"}) {
            UNIT_ASSERT_C(!TLatencySliConfig::TryParse(value), value);
        }
        NProto::TDiagnosticsConfig proto;
        proto.SetEnableLatencySli(true);
        auto* row = proto.AddLatencySliThresholds();
        row->SetMediaKind(NProto::STORAGE_MEDIA_SSD);
        row->SetStartBytes(1);
        row->SetEndBytes(8193);
        row->SetThresholdUs(100);
        TDiagnosticsConfig config(proto);
        UNIT_ASSERT(config.GetLatencySliConfig(NProto::STORAGE_MEDIA_HDD).Thresholds[0].empty());
    }

    Y_UNIT_TEST(ShouldRejectMalformedChildConfigWithoutThrowing)
    {
        for (const TString value: {
                 ";", ";;", ";1", "1;", "1;;0:1:9:100", "2;0:1:9:100",
                 "1;0:1:9", "1;0:1:9:100:200", "1;0::9:100",
                 "1;0:1:9:", "1;2:1:9:100", "1;-1:1:9:100",
                 "1;0:-1:9:100", "1;0:1:9:-1", "1;0:1:9:abc",
                 "1;0:1:9:18446744073709551616"})
        {
            UNIT_ASSERT_C(!TLatencySliConfig::TryParse(value), value);
        }
        const auto disabled = TLatencySliConfig::TryParse("");
        UNIT_ASSERT(disabled);
        UNIT_ASSERT(!disabled->Enabled);
        const auto empty = TLatencySliConfig::TryParse("1");
        UNIT_ASSERT(empty);
        UNIT_ASSERT(empty->Enabled);
        UNIT_ASSERT_VALUES_EQUAL("1", empty->Serialize());
    }

    Y_UNIT_TEST(ShouldLeaveRequestsWithoutThresholdsUnjudged)
    {
        auto config = TLatencySliConfig::TryParse("1;0:1:8193:100").value();
        for (bool failed: {false, true}) {
            const auto expected = ELatencySliResult::Unknown;
            UNIT_ASSERT(config.Classify(true, 4096, 100, failed) == expected);
        }
        UNIT_ASSERT(config.Classify(false, 4096, 1000, false) == ELatencySliResult::Bad);
        UNIT_ASSERT(config.Classify(false, 4096, 1, true) == ELatencySliResult::Bad);
    }

    Y_UNIT_TEST(ShouldExcludeOnlyVerifiedThrottleDelayAtOriginalCompletion)
    {
        TFixture f;
        // The 600us backoff and 600us dependency wait remain service time.
        f.Complete(1500, {.PostponedUs = 600, .BackoffUs = 600});
        // 85ms observed, of which 84ms was throttling. Exactly on the read
        // limit.
        f.Complete(85000, {.PostponedUs = 84000, .ThrottleUs = 84000});
        f.Complete(1500, {.Write = true});
        f.Complete(500, {.Forwarded = true});
        f.Complete(500, {.ThrottleUs = 501});
        f.Complete(500, {.Error = E_IO, .ThrottleUs = std::nullopt});
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->FindCounter("LatencyGoodOps")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->FindCounter("LatencyBadOps")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Reads()->FindCounter("LatencyTotalOps")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Reads()->FindCounter("LatencyUnknownOps")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Writes()->FindCounter("LatencyGoodOps")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Writes()->FindCounter("LatencyTotalOps")->Val());
    }

    Y_UNIT_TEST(ShouldFinishPendingRequestUnderItsOriginalDiskType)
    {
        TFixture f;
        TMetricRequest pending{EBlockStoreRequest::ReadBlocks};
        auto context = CreateCallContext();
        TLog log;
        f.Server->PrepareMetricRequest(pending, "client", "disk", 0, 4096,
                                       false);
        f.Server->RequestStarted(log, pending, *context);

        auto volume = pending.VolumeInfo->GetInfo();
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_HDD);
        f.Server->MountVolume(volume, "client", "instance");
        f.Server->RequestCompleted(log, pending, *context, MakeError(E_IO));
        f.Complete(500);

        auto folder = f.Monitoring->GetCounters()
                          ->GetSubgroup("counters", "blockstore")
                          ->GetSubgroup("component", "sli_volume")
                          ->GetSubgroup("host", "cluster")
                          ->GetSubgroup("volume", "disk")
                          ->GetSubgroup("instance", "instance")
                          ->GetSubgroup("cloud", "")
                          ->GetSubgroup("folder", "");
        const auto oldType = folder->FindSubgroup(
            "type",
            MediaKindToComputeType(NProto::STORAGE_MEDIA_SSD_NONREPLICATED));
        const auto newType = folder->FindSubgroup(
            "type", MediaKindToComputeType(NProto::STORAGE_MEDIA_HDD));
        UNIT_ASSERT(oldType && newType);
        UNIT_ASSERT(!oldType->FindCounter("ObservedSeconds"));
        UNIT_ASSERT(newType->FindCounter("ObservedSeconds"));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            oldType->FindSubgroup("request", "ReadBlocks")
                ->FindCounter("LatencyBadOps")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            newType->FindSubgroup("request", "ReadBlocks")
                ->FindCounter("LatencyUnknownOps")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            newType->FindSubgroup("request", "ReadBlocks")
                ->FindCounter("LatencyTotalOps")
                ->Val());
    }

    Y_UNIT_TEST(ShouldUseOriginalUnalignedLengthWithoutChangingLegacyBytes)
    {
        TFixture f;
        f.Complete(500, {.Bytes = 12288, .OriginalBytes = 8192});
        f.Complete(500, {.Bytes = 12288});
        f.Server->UpdateStats(true);
        auto legacy = f.Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "disk")
            ->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "")->GetSubgroup("folder", "")
            ->GetSubgroup("type", "ssd_nonrepl")
            ->GetSubgroup("request", "ReadBlocks");
        UNIT_ASSERT_VALUES_EQUAL(24576, legacy->GetCounter("RequestBytes", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, legacy->GetCounter("Count", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldJudgeServiceErrorsOnlyForConfiguredSizes)
    {
        TFixture f;
        for (ui32 error: {E_REJECTED, E_CANCELLED, E_ARGUMENT, E_TIMEOUT}) {
            f.Complete(500, {.Error = error});
        }
        f.Complete(500, {.PostponedUs = 600});
        f.Complete(500, {.Error = E_REJECTED, .Bytes = 99999});
        f.Complete(500, {.Bytes = 99999});
        f.Complete(2000, {.PostponedUs = 1500, .Error = E_REJECTED});
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            5, f.Reads()->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            6, f.Reads()->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Reads()->GetCounter("LatencyUnknownOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyGoodOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldIncludeResponseDeliveryTime)
    {
        TFixture f;
        f.Complete(500, {.SendDelayUs = 1500});
        UNIT_ASSERT_VALUES_EQUAL(
            0, f.Reads()->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyTotalOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldPublishAllOutcomesImmediately)
    {
        TFixture f;
        f.Complete(500);
        f.Complete(1500);
        f.Complete(500, {.Bytes = 16384});
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Reads()->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Reads()->GetCounter("LatencyUnknownOps", true)->Val());
        f.Complete(500);
        f.Server->UpdateStats(true);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Reads()->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            3, f.Reads()->GetCounter("LatencyTotalOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldPreserveReusedCounters)
    {
        auto group = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const auto config = TLatencySliConfig::TryParse("1;0:1:8193:100").value();
        {
            TLatencySliCounters counters(config, group);
            counters.Add(false, 5, 3, 2);
            counters.Add(false, 7, 0, 1);
        }
        {
            TLatencySliCounters counters(config, group);
            counters.Add(false, 2, 4, 8);
        }
        const auto reads = group->FindSubgroup("request", "ReadBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            14, reads->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            7, reads->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            21, reads->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            11, reads->GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldNotLoseConcurrentCompletions)
    {
        auto group = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TLatencySliCounters counters({}, group);
        constexpr ui64 iterations = 10000;
        std::thread good(
            [&]
            {
                for (ui64 i = 0; i < iterations; ++i) {
                    counters.Add(false, 1, 0, 0);
                }
            });
        std::thread bad(
            [&]
            {
                for (ui64 i = 0; i < iterations; ++i) {
                    counters.Add(false, 0, 1, 0);
                }
            });
        std::thread unknown(
            [&]
            {
                for (ui64 i = 0; i < iterations; ++i) {
                    counters.Add(false, 0, 0, 1);
                }
            });
        good.join();
        bad.join();
        unknown.join();
        const auto reads = group->FindSubgroup("request", "ReadBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            iterations, reads->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            iterations, reads->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            iterations * 2, reads->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            iterations, reads->GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldBeDisabledByDefaultAndLeaveClientStatsUsable)
    {
        for (const auto enabled: {std::optional<bool>{}, std::optional<bool>{false}}) {
            TFixture f(enabled);
            f.Complete(500);
            f.Server->UpdateStats(true);
            UNIT_ASSERT(!f.Reads());
            UNIT_ASSERT(!f.Writes());
        }
        auto client = CreateClientStats(
            std::make_shared<TDumpable>(),
            CreateMonitoringServiceStub(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            "client");
        UNIT_ASSERT(!client->GetLatencySliConfig(0).Enabled);
    }
}

}   // namespace NCloud::NBlockStore
