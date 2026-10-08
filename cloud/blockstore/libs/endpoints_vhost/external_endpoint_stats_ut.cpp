#include "external_endpoint_stats.cpp"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/dumpable.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/vhost-server/stats.h>

#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/stream/str.h>

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
        {"count", stats.Count}, {"bytes", stats.Bytes}, {"errors", stats.Errors}
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

        auto serverGroup = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server");

        auto volumeStats = CreateVolumeStats(
            Monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        ServerStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            std::make_shared<TDiagnosticsConfig>(),
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
    Y_UNIT_TEST_F(ShouldConsumeDumpedErrorsOncePerDirection, TFixture)
    {
        TEndpointStats stats{ClientId, DiskId, ServerStats};

        auto serverCounters = Monitoring->GetCounters()
                                  ->GetSubgroup("counters", "blockstore")
                                  ->GetSubgroup("component", "server")
                                  ->GetSubgroup("type", "ssd_local");
        auto volumeCounters = Monitoring->GetCounters()
                                  ->GetSubgroup("counters", "blockstore")
                                  ->GetSubgroup("component", "server_volume")
                                  ->GetSubgroup("host", "cluster")
                                  ->GetSubgroup("volume", DiskId)
                                  ->GetSubgroup("instance", "instance")
                                  ->GetSubgroup("cloud", "")
                                  ->GetSubgroup("folder", "")
                                  ->GetSubgroup("type", "ssd_local");

        NVHostServer::TCompleteStats completeStats;
        NVHostServer::TSimpleStats previousStats;
        auto& current = completeStats.SimpleStats;

        // The directional errors include disk and crypto failures. The
        // aggregate encryptor_errors field repeats only the crypto subset.
        current.Requests[0].Count = 4;
        current.Requests[0].Bytes = 16_KB;
        current.Requests[0].Errors = 1 + 2;   // disk + decrypt
        current.Requests[1].Count = 3;
        current.Requests[1].Bytes = 24_KB;
        current.Requests[1].Errors = 1 + 1;   // disk + encrypt
        current.EncryptorErrors = 2 + 1;

        auto dump = [&]
        {
            TStringStream stream;
            NVHostServer::DumpStats(
                completeStats,
                previousStats,
                1s,
                stream,
                1'000'000);
            NJson::TJsonValue value;
            NJson::ReadJsonTree(stream.Str(), &value, true);
            UNIT_ASSERT(value.Has("encryptor_errors"));
            UNIT_ASSERT(!value["read"].Has("encryptor_errors"));
            UNIT_ASSERT(!value["write"].Has("encryptor_errors"));
            return value;
        };

        auto consume = [&](const NJson::TJsonValue& value)
        {
            Timer->AdvanceTime(1s);
            stats.Update(value);
            ServerStats->UpdateStats(true);

            for (const auto& counters: {serverCounters, volumeCounters}) {
                for (ui32 kind = 0; kind != 2; ++kind) {
                    auto request = counters->GetSubgroup(
                        "request", kind == 0 ? "ReadBlocks" : "WriteBlocks");
                    UNIT_ASSERT_VALUES_EQUAL(
                        current.Requests[kind].Count,
                        request->GetCounter("Count", true)->Val());
                    UNIT_ASSERT_VALUES_EQUAL(
                        current.Requests[kind].Bytes,
                        request->GetCounter("RequestBytes", true)->Val());
                    UNIT_ASSERT_VALUES_EQUAL(
                        current.Requests[kind].Errors,
                        request->GetCounter("Errors", true)->Val());
                }
            }
        };

        {
            const auto value = dump();
            UNIT_ASSERT_VALUES_EQUAL(3, value["read"]["errors"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL(2, value["write"]["errors"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                value["encryptor_errors"].GetUInteger());
            consume(value);
        }

        {
            // DumpStats emits deltas: an unchanged producer must not add
            // any errors or successful requests to either consumer again.
            const auto value = dump();
            for (const auto* kind: {"read", "write"}) {
                UNIT_ASSERT_VALUES_EQUAL(0, value[kind]["count"].GetUInteger());
                UNIT_ASSERT_VALUES_EQUAL(0, value[kind]["bytes"].GetUInteger());
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    value[kind]["errors"].GetUInteger());
            }
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                value["encryptor_errors"].GetUInteger());
            consume(value);
        }

        {
            ++current.Requests[0].Errors;
            ++current.EncryptorErrors;
            const auto value = dump();
            UNIT_ASSERT_VALUES_EQUAL(1, value["read"]["errors"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, value["write"]["errors"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                value["encryptor_errors"].GetUInteger());
            consume(value);
        }

        {
            ++current.Requests[1].Errors;
            ++current.EncryptorErrors;
            const auto value = dump();
            UNIT_ASSERT_VALUES_EQUAL(0, value["read"]["errors"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL(1, value["write"]["errors"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                value["encryptor_errors"].GetUInteger());
            consume(value);
        }
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
