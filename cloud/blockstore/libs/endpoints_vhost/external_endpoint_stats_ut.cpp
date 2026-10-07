#include "external_endpoint_stats.cpp"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/dumpable.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>

#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <chrono>

namespace NCloud::NBlockStore::NServer {

using namespace std::chrono_literals;

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

struct TReqStats
{
    ui64 Count = 0;
    ui64 Bytes = 0;
    ui64 Errors = 0;

    TVector<std::pair<ui64, ui64>> Times;
    TVector<std::pair<ui64, ui64>> Sizes;
};

struct TVolumeStats
{
    TReqStats Read;
    TReqStats Write;
};

auto Dump(const TReqStats& stats)
{
    auto value = NJson::TJsonMap {
        {"count", stats.Count}, {"bytes", stats.Bytes}
    };

    auto hist = [] (auto& h) {
        NJson::TJsonArray hist;
        for (auto [value, count]: h) {
            hist.AppendValue(NJson::TJsonArray {{value, count}});
        }
        return hist;
    };

    if (stats.Sizes) {
        value["sizes"] = hist(stats.Sizes);
    }

    if (stats.Times) {
        value["times"] = hist(stats.Times);
    }

    return value;
}

auto Dump(TDuration elapsed, const TVolumeStats& stats)
{
    NJson::TJsonMap value {
        {"elapsed_ms", elapsed.MilliSeconds()},
        {"read", Dump(stats.Read)},
        {"write", Dump(stats.Write)}
    };

    return value;
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    IServerStatsPtr ServerStats;

    TString ClientId = "client";
    TString DiskId = "volume";

    TFixture(bool latencySli = false)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto serverGroup = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server");

        NProto::TDiagnosticsConfig proto;
        proto.SetEnableLatencySli(latencySli);
        auto* row = proto.AddLatencySliThresholds();
        row->SetMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
        row->SetStartBytes(1);
        row->SetEndBytes(8193);
        row->SetThresholdUs(1000);
        auto config = std::make_shared<TDiagnosticsConfig>(proto);
        auto volumeStats = CreateVolumeStats(
            Monitoring,
            config,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        ServerStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            config,
            Monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                serverGroup,
                Timer,
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
            std::move(volumeStats));

        NProto::TVolume volume;
        volume.SetDiskId(DiskId);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);

        ServerStats->MountVolume(volume, ClientId, "instance");
    }

    void UpdateStats(
        TEndpointStats& stats,
        const TVolumeStats& volumeStats)
    {
        Timer->AdvanceTime(1s);

        stats.Update(Dump(1s, volumeStats));
        ServerStats->UpdateStats(true);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEndpointStatsTest)
{
    Y_UNIT_TEST(ShouldProtectLatencyBatches)
    {
        TFixture f(true);
        TEndpointStats reader{f.ClientId, f.DiskId, f.ServerStats};
        auto group = f.Monitoring->GetCounters()->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume")->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", f.DiskId)->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "")->GetSubgroup("folder", "")
            ->GetSubgroup("type", "ssd_local")->GetSubgroup("request", "ReadBlocks");
        auto snapshot = [&](ui64 sequence, ui64 good, bool fresh = true) {
            return NJson::TJsonMap{
                {"latency_sli", NJson::TJsonMap{
                    {"version", ui64(1)}, {"epoch", "process-1"},
                    {"sequence", sequence}, {"fresh", fresh},
                    {"config", "1;0:1:8193:1000"}}},
                {"read", NJson::TJsonMap{
                    {"count", ui64(1)}, {"latency_good", good},
                    {"latency_bad", ui64(0)}, {"latency_unknown", ui64(0)}}},
                {"write", NJson::TJsonMap{
                    {"latency_good", ui64(0)}, {"latency_bad", ui64(0)},
                    {"latency_unknown", ui64(0)}}}};
        };
        reader.Update(snapshot(1, 5));
        reader.Update(snapshot(1, 5)); // duplicate
        reader.Update(snapshot(2, 7));
        reader.Update(snapshot(1, 5)); // out of order
        UNIT_ASSERT_VALUES_EQUAL(7, group->GetCounter("LatencyGoodOps", true)->Val());
        reader.Update(snapshot(3, 9, false)); // stale completion snapshot
        reader.Update(snapshot(4, 11)); // gap: four operations become Unknown
        UNIT_ASSERT_VALUES_EQUAL(4, group->GetCounter("LatencyUnknownOps", true)->Val());
        auto mismatched = snapshot(5, 12);
        mismatched["latency_sli"]["config"] = "1;0:1:8193:2000";
        reader.Update(mismatched);
        reader.Update(snapshot(6, 1)); // reset: no unsigned subtraction
        reader.Update(snapshot(7, 2));
        UNIT_ASSERT_VALUES_EQUAL(8, group->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(6, group->GetCounter("LatencyUnknownOps", true)->Val());
        auto otherEpoch = snapshot(8, 999);
        otherEpoch["latency_sli"]["epoch"] = "another-process";
        reader.Update(otherEpoch);
        UNIT_ASSERT_VALUES_EQUAL(8, group->GetCounter("LatencyTotalOps", true)->Val());
        // A restarted child owns a fresh stats pipe and a new reader.
        TEndpointStats restarted{f.ClientId, f.DiskId, f.ServerStats};
        restarted.Update(snapshot(1, 3));
        UNIT_ASSERT_VALUES_EQUAL(11, group->GetCounter("LatencyGoodOps", true)->Val());
        auto legacy = NJson::TJsonMap{{"read", NJson::TJsonMap{
            {"count", ui64(2)}, {"errors", ui64(1)}}}};
        restarted.Update(legacy);
        UNIT_ASSERT_VALUES_EQUAL(9, group->GetCounter("LatencyUnknownOps", true)->Val());
        auto missing = snapshot(2, 4);
        missing["read"].EraseValue("latency_good");
        restarted.Update(missing);
        restarted.Update(snapshot(3, 5)); // rebaseline, do not replay old Good
        restarted.Update(snapshot(4, 6));
        UNIT_ASSERT_VALUES_EQUAL(12, group->GetCounter("LatencyGoodOps", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(11, group->GetCounter("LatencyUnknownOps", true)->Val());
    }

    Y_UNIT_TEST_F(ShouldCalcMaxValues, TFixture)
    {
        TEndpointStats stats {ClientId, DiskId, ServerStats};

        auto writeBlocks = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server")
            ->GetSubgroup("type", "ssd_local")
            ->GetSubgroup("request", "WriteBlocks");

        auto count = writeBlocks->GetCounter("Count");
        auto maxCount = writeBlocks->GetCounter("MaxCount");

        auto requestBytes = writeBlocks->GetCounter("RequestBytes");
        auto maxRequestBytes = writeBlocks->GetCounter("MaxRequestBytes");

        {
            TVolumeStats volumeStats {
                .Write = {
                    .Count = 42,
                    .Bytes = 1_MB,
                }
            };

            UpdateStats(stats, volumeStats);

            UNIT_ASSERT_VALUES_EQUAL(42, count->Val());
            UNIT_ASSERT_VALUES_EQUAL(42, maxCount->Val());

            UNIT_ASSERT_VALUES_EQUAL(1_MB, requestBytes->Val());
            UNIT_ASSERT_VALUES_EQUAL(1_MB, maxRequestBytes->Val());
        }

        {
            TVolumeStats volumeStats {
                .Write = {
                    .Count = 20,
                    .Bytes = 5_MB,
                }
            };

            UpdateStats(stats, volumeStats);

            UNIT_ASSERT_VALUES_EQUAL(62, count->Val());
            UNIT_ASSERT_VALUES_EQUAL(42, maxCount->Val());

            UNIT_ASSERT_VALUES_EQUAL(6_MB, requestBytes->Val());
            UNIT_ASSERT_VALUES_EQUAL(5_MB, maxRequestBytes->Val());
        }

        {
            TVolumeStats volumeStats {
                .Write = {
                    .Count = 1,
                    .Bytes = 5_KB,
                }
            };

            UpdateStats(stats, volumeStats);

            UNIT_ASSERT_VALUES_EQUAL(63, count->Val());
            UNIT_ASSERT_VALUES_EQUAL(42, maxCount->Val());

            UNIT_ASSERT_VALUES_EQUAL(6_MB + 5_KB, requestBytes->Val());
            UNIT_ASSERT_VALUES_EQUAL(5_MB, maxRequestBytes->Val());
        }

        for (int i = 0; i != 12; ++i) {
            const ui64 prevСount = count->Val();
            const ui64 prevRequestBytes = requestBytes->Val();

            UpdateStats(stats, {});

            UNIT_ASSERT_VALUES_EQUAL(prevСount, count->Val());
            UNIT_ASSERT_VALUES_EQUAL(42, maxCount->Val());

            UNIT_ASSERT_VALUES_EQUAL(prevRequestBytes, requestBytes->Val());
            UNIT_ASSERT_VALUES_EQUAL(5_MB, maxRequestBytes->Val());
        }

        {
            const ui64 prevСount = count->Val();
            const ui64 prevRequestBytes = requestBytes->Val();

            UpdateStats(stats, {});

            UNIT_ASSERT_VALUES_EQUAL(prevСount, count->Val());
            UNIT_ASSERT_VALUES_EQUAL(20, maxCount->Val());

            UNIT_ASSERT_VALUES_EQUAL(prevRequestBytes, requestBytes->Val());
            UNIT_ASSERT_VALUES_EQUAL(5_MB, maxRequestBytes->Val());
        }

        {
            const ui64 prevСount = count->Val();
            const ui64 prevRequestBytes = requestBytes->Val();

            UpdateStats(stats, {});

            UNIT_ASSERT_VALUES_EQUAL(prevСount, count->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, maxCount->Val());

            UNIT_ASSERT_VALUES_EQUAL(prevRequestBytes, requestBytes->Val());
            UNIT_ASSERT_VALUES_EQUAL(5_KB, maxRequestBytes->Val());
        }

        {
            const ui64 prevСount = count->Val();
            const ui64 prevRequestBytes = requestBytes->Val();

            UpdateStats(stats, {});

            UNIT_ASSERT_VALUES_EQUAL(prevСount, count->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, maxCount->Val());

            UNIT_ASSERT_VALUES_EQUAL(prevRequestBytes, requestBytes->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, maxRequestBytes->Val());
        }
    }

    Y_UNIT_TEST_F(ShouldCalcHists, TFixture)
    {
        TEndpointStats stats {ClientId, DiskId, ServerStats};

        auto writeBlocks = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server")
            ->GetSubgroup("type", "ssd_local")
            ->GetSubgroup("request", "WriteBlocks");

        auto sizes = writeBlocks->FindSubgroup("histogram", "Size");
        UNIT_ASSERT(sizes);
        sizes = sizes->FindSubgroup("units", "KB");
        UNIT_ASSERT(sizes);

        auto times = writeBlocks->FindSubgroup("histogram", "Time");
        times = times->FindSubgroup("units", "usec");
        UNIT_ASSERT(times);

        {
            TVolumeStats volumeStats {
                .Write = {
                    .Count = 10,
                    .Bytes = 40_KB,
                    .Times = {{30, 7}, {100, 2}, {200, 1}},
                    .Sizes = {{4_KB, 10}},
                }
            };

            UpdateStats(stats, volumeStats);

            UNIT_ASSERT_VALUES_EQUAL(10, sizes->GetCounter("4KB")->Val());
            UNIT_ASSERT_VALUES_EQUAL(9, times->GetCounter("100")->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, times->GetCounter("200")->Val());
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
