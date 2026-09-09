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
#include <limits>

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

    TFixture()
    {
        auto monitoring = CreateMonitoringServiceStub();

        NProto::TDiagnosticsConfig protoConfig;
        protoConfig.SetLatencyThresholdsEnabled(true);
        auto* media = protoConfig.AddLatencyThresholds();
        media->SetMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
        auto* bucket = media->AddBuckets();
        bucket->SetMinRequestBytes(0);
        bucket->SetReadThresholdMs(10);
        bucket->SetWriteThresholdMs(10);
        auto diagnosticsConfig =
            std::make_shared<TDiagnosticsConfig>(std::move(protoConfig));

        auto serverGroup = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server");

        auto volumeStats = CreateVolumeStats(
            Monitoring,
            diagnosticsConfig,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        ServerStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            diagnosticsConfig,
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

    auto GetLatencyCounters()
    {
        return Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "sli_volume")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", DiskId)
            ->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "")
            ->GetSubgroup("folder", "")
            ->GetSubgroup("type", "unknown");
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
    Y_UNIT_TEST(ShouldValidateVersionedLatencyCountersPayloadAtomically)
    {
        auto makeCounters = [] (ui64 good, ui64 bad, ui64 skipped) {
            return NJson::TJsonMap{
                {"good", good},
                {"bad", bad},
                {"skipped", skipped},
            };
        };
        auto makePayload = [&] (ui64 version) {
            return NJson::TJsonMap{
                {"latency_counters", NJson::TJsonMap{
                    {"version", version},
                    {"read", makeCounters(3, 2, 1)},
                    {"write", makeCounters(5, 4, 3)},
                }},
            };
        };

        UNIT_ASSERT(!TryReadLatencyCountersBatch(NJson::TJsonMap{}));
        UNIT_ASSERT(!TryReadLatencyCountersBatch(makePayload(2)));

        auto valid = TryReadLatencyCountersBatch(makePayload(1));
        UNIT_ASSERT(valid);
        UNIT_ASSERT_VALUES_EQUAL(3, valid->Read.Good);
        UNIT_ASSERT_VALUES_EQUAL(2, valid->Read.Bad);
        UNIT_ASSERT_VALUES_EQUAL(1, valid->Read.Skipped);
        UNIT_ASSERT_VALUES_EQUAL(5, valid->Write.Good);
        UNIT_ASSERT_VALUES_EQUAL(4, valid->Write.Bad);
        UNIT_ASSERT_VALUES_EQUAL(3, valid->Write.Skipped);

        auto malformed = makePayload(1);
        malformed["latency_counters"]["write"]["bad"] = "not-a-counter";
        UNIT_ASSERT(!TryReadLatencyCountersBatch(malformed));

        auto overflow = makePayload(1);
        overflow["latency_counters"]["read"]["good"] =
            static_cast<unsigned long long>(std::numeric_limits<ui64>::max());
        overflow["latency_counters"]["read"]["bad"] = 1;
        UNIT_ASSERT(!TryReadLatencyCountersBatch(overflow));

        auto signedOverflow = makePayload(1);
        signedOverflow["latency_counters"]["read"]["good"] =
            static_cast<unsigned long long>(
                std::numeric_limits<TAtomicBase>::max());
        signedOverflow["latency_counters"]["read"]["bad"] = 0;
        signedOverflow["latency_counters"]["read"]["skipped"] = 0;
        signedOverflow["latency_counters"]["write"]["good"] = 1;
        signedOverflow["latency_counters"]["write"]["bad"] = 0;
        signedOverflow["latency_counters"]["write"]["skipped"] = 0;
        UNIT_ASSERT(!TryReadLatencyCountersBatch(signedOverflow));
    }

    Y_UNIT_TEST_F(ShouldConsumeOnlyExplicitLatencyCountersPayload, TFixture)
    {
        TEndpointStats stats{ClientId, DiskId, ServerStats};
        auto counters = GetLatencyCounters();
        auto total = counters->GetCounter("LatencyTotalOps");
        auto good = counters->GetCounter("LatencyGoodOps");
        auto skipped = counters->GetCounter("LatencyThresholdsSkippedOps");

        // A legacy-only batch must not be guessed from count/errors or the
        // independent time/size histograms.
        UpdateStats(stats, TVolumeStats{.Read = {.Count = 100}});
        UNIT_ASSERT_VALUES_EQUAL(0, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, skipped->Val());

        auto value = Dump(1s, {});
        value["latency_counters"] = NJson::TJsonMap{
            {"version", 1},
            {"read", NJson::TJsonMap{
                {"good", 3}, {"bad", 2}, {"skipped", 1}}},
            {"write", NJson::TJsonMap{
                {"good", 5}, {"bad", 4}, {"skipped", 3}}},
        };
        stats.Update(value);

        UNIT_ASSERT_VALUES_EQUAL(14, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(8, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(4, skipped->Val());

        value["latency_counters"]["version"] = 2;
        stats.Update(value);
        UNIT_ASSERT_VALUES_EQUAL(14, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(8, good->Val());
        UNIT_ASSERT_VALUES_EQUAL(4, skipped->Val());
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
