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

struct TFixture
{
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    IVolumeStatsPtr Volumes;
    IServerStatsPtr Server;
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
        Server = CreateServerStats(std::make_shared<TDumpable>(), config,
            Monitoring, nullptr, CreateServerRequestStats(Monitoring->GetCounters(),
                Timer, EHistogramCounterOption::ReportMultipleCounters, {}), Volumes);
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
        // Intentionally aggregate both operations while keeping separate thresholds.
        Sli = group;
    }

    void Complete(ui64 elapsedUs, ui64 postponedUs = 0, ui32 error = S_OK,
                  bool write = false, ui64 bytes = 4096, bool forwarded = false,
                  bool valid = true, ui64 backoff = 0, ui64 shaping = 0,
                  ui64 originalBytes = 0)
    {
        TMetricRequest metric{write ? EBlockStoreRequest::WriteBlocksLocal
                                    : EBlockStoreRequest::ReadBlocksLocal};
        Server->PrepareMetricRequest(metric, "client", "disk", 0, bytes, false);
        metric.ForwardedRequest = forwarded;
        if (originalBytes) {
            metric.OriginalRequestBytes = originalBytes;
        }
        auto ctx = CreateCallContext();
        TLog log;
        Server->RequestStarted(log, metric, *ctx);
        const auto start = GetCycleCount() - DurationToCyclesSafe(TDuration::Seconds(10));
        ctx->SetRequestStartedCycles(valid ? start : 0);
        ctx->SetResponseSentCycles(valid ? start + DurationToCyclesSafe(
            TDuration::MicroSeconds(elapsedUs)) : 0);
        ctx->AddTime(EProcessingStage::Postponed, TDuration::MicroSeconds(postponedUs));
        ctx->AddTime(EProcessingStage::Backoff, TDuration::MicroSeconds(backoff));
        ctx->AddTime(EProcessingStage::Shaping, TDuration::MicroSeconds(shaping));
        ctx->SetSilenceRetriableErrors(true);
        Server->RequestCompleted(log, metric, *ctx, MakeError(error));
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
            UNIT_ASSERT(f.Sli->FindCounter(name));
            UNIT_ASSERT(!legacy->FindCounter(name));
        }
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->FindCounter("LatencyGoodOps")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, legacy->GetSubgroup("request", "ReadBlocks")
                   ->GetCounter("Count", true)->Val());
    }

    Y_UNIT_TEST(ShouldRelabelAndTrimLatencyCountersWithTheirVolume)
    {
        TFixture f;
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
            ->GetSubgroup("type", "network-ssd-nonreplicated");
        UNIT_ASSERT(group->FindCounter("LatencyTotalOps"));
        f.Complete(500);
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(1, group->FindCounter("LatencyTotalOps")->Val());

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
                     {"ObservedSeconds",
                      "AvailableSeconds",
                      "HealthySeconds",
                      "LatencyGoodOps",
                      "LatencyBadOps",
                      "LatencyTotalOps",
                      "LatencyUnknownOps"})
                {
                    UNIT_ASSERT_C(f.Sli->FindCounter(name), name);
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    15, f.Sli->FindCounter("ObservedSeconds")->Val());
                UNIT_ASSERT_VALUES_EQUAL(
                    1, f.Sli->FindCounter("LatencyGoodOps")->Val());

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

    Y_UNIT_TEST(ShouldMergeReadAndWriteOutcomesUsingTheirOwnThresholds)
    {
        TFixture f;
        f.Complete(1500); // above the 1000us read threshold
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(0, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyTotalOps", true)->Val());
        f.Complete(1500, 0, S_OK, true); // below the 2000us write threshold
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, f.Sli->GetCounter("LatencyTotalOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldUseManualSizeBoundariesAndInclusiveThreshold)
    {
        auto config = TLatencySliConfig::Parse("1;0:1:4097:100;0:4097:8193:200;1:1:8193:300");
        UNIT_ASSERT(config.Classify(false, 4096, 100, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 4096, 101, false) == ELatencySliResult::Bad);
        UNIT_ASSERT(config.Classify(false, 4097, 200, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 8193, 1, false) == ELatencySliResult::Unknown);
        UNIT_ASSERT(config.Classify(true, 4096, 250, false) == ELatencySliResult::Good);
        UNIT_ASSERT_VALUES_EQUAL(config.Serialize(), TLatencySliConfig::Parse(config.Serialize()).Serialize());
    }

    Y_UNIT_TEST(ShouldRejectInvalidOrOverlappingTables)
    {
        for (const TString value: {"1;0:5:5:100", "1;0:1:9:0", "1;0:1:9:100;0:8:10:200"}) {
            const auto config = TLatencySliConfig::Parse(value);
            UNIT_ASSERT(config.Thresholds[0].empty());
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

    Y_UNIT_TEST(ShouldCountFailuresWithoutCompleteTimingOrThresholds)
    {
        auto config = TLatencySliConfig::Parse("1;0:1:8193:100");
        for (bool failed: {false, true}) {
            const auto expected = failed ? ELatencySliResult::Bad
                                         : ELatencySliResult::Unknown;
            UNIT_ASSERT(config.Classify(false, 4096, 100, failed, false) == expected);
            UNIT_ASSERT(config.Classify(true, 4096, 100, failed) == expected);
        }
        UNIT_ASSERT(config.Classify(false, 4096, 1000, false) == ELatencySliResult::Bad);
        UNIT_ASSERT(config.Classify(false, 4096, 1, true) == ELatencySliResult::Bad);
    }

    Y_UNIT_TEST(ShouldCountAtOriginalCompletionAndRetainAllWaits)
    {
        TFixture f;
        f.Complete(500);
        f.Complete(1500);
        f.Complete(1500, 1000);
        f.Complete(1500, 0, S_OK, false, 4096, false, true, 600, 600);
        f.Complete(1500, 0, S_OK, true);
        f.Complete(500, 0, S_OK, false, 4096, true); // forwarded cell
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(2, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, f.Sli->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(5, f.Sli->GetCounter("LatencyTotalOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldUseOriginalUnalignedLengthWithoutChangingLegacyBytes)
    {
        TFixture f;
        f.Complete(500, 0, S_OK, false, 12288, false, true, 0, 0, 8192);
        f.Complete(500, 0, S_OK, false, 12288);
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
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldCountErrorsWithoutExclusionsAndNeverGuessUnknownTiming)
    {
        TFixture f;
        for (ui32 error: {E_REJECTED, E_CANCELLED, E_ARGUMENT, E_TIMEOUT}) {
            f.Complete(500, 0, error);
        }
        f.Complete(500, 600);
        f.Complete(500, 0, E_REJECTED, false, 99999);
        f.Complete(500, 0, S_OK, false, 4096, false, false);
        f.Complete(500, 0, E_REJECTED, false, 4096, false, false);
        f.Complete(2000, 1500, E_REJECTED);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(7, f.Sli->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(8, f.Sli->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyUnknownOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldIncludeAllThrottlerWaitsInLatency)
    {
        TFixture f;
        f.Complete(2000, 1500);
        f.Complete(2000, 1500, S_OK, true);
        f.Complete(2000);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, f.Sli->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, f.Sli->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, f.Sli->GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldPublishAllOutcomesOnlyOnStatsUpdate)
    {
        TFixture f;
        f.Complete(500);
        f.Complete(1500);
        f.Complete(500, 0, S_OK, false, 16384);
        for (const auto* name:
             {"LatencyGoodOps",
              "LatencyBadOps", "LatencyTotalOps", "LatencyUnknownOps"})
        {
            UNIT_ASSERT_VALUES_EQUAL(0, f.Sli->GetCounter(name, true)->Val());
        }
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Sli->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Sli->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Sli->GetCounter("LatencyUnknownOps", true)->Val());
        f.Complete(500);
        f.Server->UpdateStats(true);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Sli->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            3, f.Sli->GetCounter("LatencyTotalOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldFlushBatchesAndPreserveReusedCounters)
    {
        NMonitoring::TDynamicCounters group;
        const auto config = TLatencySliConfig::Parse("1;0:1:8193:100");
        {
            TLatencySliCounters counters(config, group);
            counters.Add(5, 3, 2);
            counters.Publish();
            counters.Add(7, 0, 1);
        }
        {
            TLatencySliCounters counters(config, group);
            counters.Add(2, 4, 8);
            counters.Publish();
            counters.Publish();
        }
        UNIT_ASSERT_VALUES_EQUAL(
            14, group.GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            7, group.GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            21, group.GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            11, group.GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldNotLoseConcurrentCompletionsOrPublications)
    {
        NMonitoring::TDynamicCounters group;
        TLatencySliCounters counters({}, group);
        constexpr ui64 iterations = 10000;
        std::atomic<bool> done = false;
        auto publish = [&]
        {
            while (!done.load(std::memory_order_relaxed)) {
                counters.Publish();
                std::this_thread::yield();
            }
        };
        std::thread collector1(publish);
        std::thread collector2(publish);
        std::thread good(
            [&]
            {
                for (ui64 i = 0; i < iterations; ++i) {
                    counters.Add(1, 0, 0);
                }
            });
        std::thread bad(
            [&]
            {
                for (ui64 i = 0; i < iterations; ++i) {
                    counters.Add(0, 1, 0);
                }
            });
        std::thread unknown(
            [&]
            {
                for (ui64 i = 0; i < iterations; ++i) {
                    counters.Add(0, 0, 1);
                }
            });
        good.join();
        bad.join();
        unknown.join();
        done.store(true, std::memory_order_relaxed);
        collector1.join();
        collector2.join();
        counters.Publish();
        UNIT_ASSERT_VALUES_EQUAL(
            iterations, group.GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            iterations, group.GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            iterations * 2, group.GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            iterations, group.GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldBeDisabledByDefaultAndLeaveClientStatsUsable)
    {
        TFixture f(std::nullopt);
        f.Complete(500);
        f.Server->UpdateStats(true);
        UNIT_ASSERT(!f.Sli->FindCounter("LatencyTotalOps"));
        auto client = CreateClientStats(std::make_shared<TDumpable>(), f.Monitoring,
            CreateRequestStatsStub(), CreateVolumeStatsStub(), "client");
        UNIT_ASSERT(!client->GetLatencySliConfig(0).Enabled);
    }

    Y_UNIT_TEST(ShouldRespectExplicitDisable)
    {
        TFixture f(false);
        f.Complete(500);
        f.Server->UpdateStats(true);
        UNIT_ASSERT(!f.Sli->FindCounter("LatencyTotalOps"));
    }
}

}   // namespace NCloud::NBlockStore
