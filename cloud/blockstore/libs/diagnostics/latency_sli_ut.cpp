#include "latency_sli.h"

#include "config.h"
#include "dumpable.h"
#include "request_stats.h"
#include "server_stats.h"
#include "volume_stats.h"

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/cputimer.h>

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
    IServerStatsPtr Server;
    NMonitoring::TDynamicCountersPtr Read;
    NMonitoring::TDynamicCountersPtr Write;

    explicit TFixture(bool enabled = true)
    {
        NProto::TDiagnosticsConfig proto;
        proto.SetEnableLatencySli(enabled);
        for (bool write: {false, true}) {
            auto* row = proto.AddLatencySliThresholds();
            row->SetMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
            row->SetWrite(write);
            row->SetStartBytes(1);
            row->SetEndBytes(8193);
            row->SetThresholdUs(write ? 2000 : 1000);
        }
        auto config = std::make_shared<TDiagnosticsConfig>(proto);
        auto timer = CreateWallClockTimer();
        auto volumes = CreateVolumeStats(Monitoring, config, TDuration::Minutes(15),
            EVolumeStatsType::EServerStats, timer);
        Server = CreateServerStats(std::make_shared<TDumpable>(), config,
            Monitoring, nullptr, CreateServerRequestStats(Monitoring->GetCounters(),
                timer, EHistogramCounterOption::ReportMultipleCounters, {}), volumes);
        NProto::TVolume volume;
        volume.SetDiskId("disk");
        volume.SetBlockSize(4096);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        Server->MountVolume(volume, "client", "instance");
        auto group = Monitoring->GetCounters()->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume")->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "disk")->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "")->GetSubgroup("folder", "")
            ->GetSubgroup("type", "ssd_nonrepl");
        Read = group;
        Write = group;
    }

    void Complete(ui64 elapsedUs, ui64 postponedUs = 0, ui32 error = S_OK,
                  bool write = false, ui64 bytes = 4096, bool cell = false,
                  bool valid = true, ui64 backoff = 0, ui64 shaping = 0,
                  ui64 originalBytes = 0, TMaybe<ui64> quotaUs = Nothing(),
                  bool quotaKnown = true)
    {
        TMetricRequest metric{write ? EBlockStoreRequest::WriteBlocksLocal
                                    : EBlockStoreRequest::ReadBlocksLocal};
        Server->PrepareMetricRequest(metric, "client", "disk", 0, bytes, false);
        metric.CellRequest = cell;
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
        ctx->AccountThrottlerQuota(
            quotaKnown ? TMaybe<TDuration>(TDuration::MicroSeconds(
                quotaUs.GetOrElse(postponedUs))) : Nothing(),
            TDuration::MicroSeconds(postponedUs));
        ctx->AddTime(EProcessingStage::Backoff, TDuration::MicroSeconds(backoff));
        ctx->AddTime(EProcessingStage::Shaping, TDuration::MicroSeconds(shaping));
        ctx->SetSilenceRetriableErrors(true);
        Server->RequestCompleted(log, metric, *ctx, MakeError(error));
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TLatencySliTest)
{
    Y_UNIT_TEST(ShouldUseManualSizeBoundariesAndInclusiveThreshold)
    {
        auto config = TLatencySliConfig::Parse("1;0:1:4097:100;0:4097:8193:200;1:1:8193:300");
        UNIT_ASSERT(config.Classify(false, 4096, 100, 0, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 4096, 101, 0, false) == ELatencySliResult::Bad);
        UNIT_ASSERT(config.Classify(false, 4097, 200, 0, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 8193, 1, 0, false) == ELatencySliResult::Unknown);
        UNIT_ASSERT(config.Classify(true, 4096, 250, 0, false) == ELatencySliResult::Good);
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
            UNIT_ASSERT(config.Classify(false, 4096, 100, 101, failed) == expected);
            UNIT_ASSERT(config.Classify(false, 4096, 100, 0, failed, false) == expected);
            UNIT_ASSERT(config.Classify(true, 4096, 100, 0, failed) == expected);
        }
        UNIT_ASSERT(config.Classify(false, 4096, 1000, 900, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 4096, 1, 0, true) == ELatencySliResult::Bad);
    }

    Y_UNIT_TEST(ShouldCountAtOriginalCompletionAndRetainOtherWaits)
    {
        TFixture f;
        f.Complete(500);
        f.Complete(1500);
        f.Complete(1500, 1000);
        f.Complete(1500, 0, S_OK, false, 4096, false, true, 600, 600);
        f.Complete(1500, 0, S_OK, true);
        f.Complete(500, 0, S_OK, false, 4096, true); // forwarded cell
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(3, f.Read->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, f.Read->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(5, f.Read->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, f.Write->GetCounter("LatencyGoodOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldUseOriginalUnalignedLengthWithoutChangingLegacyBytes)
    {
        TFixture f;
        f.Complete(500, 0, S_OK, false, 12288, false, true, 0, 0, 8192);
        f.Complete(500, 0, S_OK, false, 12288);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Read->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Read->GetCounter("LatencyUnknownOps", true)->Val());
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
        f.Complete(2000, 1500, E_REJECTED, false, 4096, false, true, 0, 0, 0, Nothing(), false);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(7, f.Read->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(7, f.Read->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, f.Read->GetCounter("LatencyUnknownOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, f.Read->GetCounter("LatencyGoodOps", true)->Val());
    }

    Y_UNIT_TEST(ShouldSubtractQuotaOnlyAndReportMissingAttribution)
    {
        TFixture f;
        f.Complete(2000, 1500, S_OK, false, 4096, false, true, 0, 0, 0, 500);
        f.Complete(2000, 1500, S_OK, true, 4096, false, true, 0, 0, 0, 1500);
        f.Complete(2000, 1500, S_OK, false, 4096, false, true, 0, 0, 0, Nothing(), false);
        // Known zero quota wait must not be confused with missing attribution.
        f.Complete(2000, 1500, S_OK, false, 4096, false, true, 0, 0, 0, 0);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, f.Read->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, f.Read->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, f.Read->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, f.Read->GetCounter("LatencyUnknownOps", true)->Val());
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
            UNIT_ASSERT_VALUES_EQUAL(0, f.Read->GetCounter(name, true)->Val());
        }
        f.Server->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Read->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Read->GetCounter("LatencyBadOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Read->GetCounter("LatencyTotalOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1, f.Read->GetCounter("LatencyUnknownOps", true)->Val());
        f.Complete(500);
        f.Server->UpdateStats(true);
        f.Server->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            2, f.Read->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            3, f.Read->GetCounter("LatencyTotalOps", true)->Val());
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
        TFixture f(false);
        f.Complete(500);
        UNIT_ASSERT(!f.Read->FindCounter("LatencyTotalOps"));
        auto client = CreateClientStats(std::make_shared<TDumpable>(), f.Monitoring,
            CreateRequestStatsStub(), CreateVolumeStatsStub(), "client");
        UNIT_ASSERT(!client->GetLatencySliConfig(0).Enabled);
    }
}

}   // namespace NCloud::NBlockStore
