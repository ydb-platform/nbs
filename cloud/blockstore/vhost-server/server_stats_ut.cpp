#include "backend.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NVHostServer {
namespace {
vhd_bdev_info RegisteredInfo{};
bool CancelOnUnregister = false;
const auto vhd_start_vhost_server = [](auto)
{
    return 0;
};
const auto vhd_stop_vhost_server = [] {
};
const auto vhd_register_blockdev =
    [](const vhd_bdev_info* info, auto, auto, auto)
{
    RegisteredInfo = *info;
    return reinterpret_cast<vhd_vdev*>(&RegisteredInfo);
};
const auto vhd_unregister_blockdev = [](auto, auto complete, void* opaque)
{
    if (CancelOnUnregister) {
        RegisteredInfo.io_completed_cb(
            RegisteredInfo.io_completed_opaque, false, 4096, true, 0);
    }
    complete(opaque);
};
}   // namespace
}   // namespace NCloud::NBlockStore::NVHostServer

#include "server.cpp"

namespace NCloud::NBlockStore::NVHostServer {
namespace {

struct TStatsBackend final: IBackend
{
    std::optional<TSimpleStats> Snapshot;
    TDuration RequestedTimeout;

    vhd_bdev_info Init(const TOptions&) override { return {}; }
    void Start() override {}
    void Stop() override {}
    void ProcessQueue(ui32, vhd_request_queue*, TSimpleStats&) override {}

    std::optional<TSimpleStats> GetCompletionStats(TDuration timeout) override
    {
        RequestedTimeout = timeout;
        return Snapshot;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TServerStatsTest)
{
    Y_UNIT_TEST(ShouldAccountFinalOutcomesAndPublishTheDrainedSnapshot)
    {
        for (bool enabled: {false, true}) {
            auto backend = std::make_shared<TStatsBackend>();
            auto server =
                CreateServer(CreateLoggingService("console"), backend);
            TOptions options;
            options.QueueCount = 0;
            options.NoChmod = true;
            // Only the 80 KiB parent is covered, never its 8/64/8 KiB parts.
            options.LatencySli = std::make_shared<const TLatencySliConfig>(
                TLatencySliConfig::TryParse(
                    enabled ? "1;1:81920:81921:1000" : "")
                    .value());
            server->Start(options);
            UNIT_ASSERT_VALUES_EQUAL(enabled, RegisteredInfo.measure_latency);
            UNIT_ASSERT(RegisteredInfo.io_completed_cb);
            auto complete = [](bool write, ui64 bytes, bool failed, ui64 us)
            {
                RegisteredInfo.io_completed_cb(
                    RegisteredInfo.io_completed_opaque,
                    write, bytes, failed, us);
            };
            complete(true, 81920, false, 999);
            complete(true, 81920, false, 1001);
            complete(true, 81920, true, 0);
            complete(true, 8192, false, 0);
            complete(true, 81920, false, Max<ui64>());
            // No backend snapshot is available, including while unregistering.
            auto snapshot = server->GetStats({});
            const auto& write = snapshot.SimpleStats.Requests[1];
            UNIT_ASSERT_VALUES_EQUAL(4, write.FinalCount);
            UNIT_ASSERT_VALUES_EQUAL(1, write.FinalErrors);
            UNIT_ASSERT_VALUES_EQUAL(enabled ? 1 : 0, write.LatencyGood);
            UNIT_ASSERT_VALUES_EQUAL(enabled ? 2 : 0, write.LatencyBad);
            UNIT_ASSERT_VALUES_EQUAL(enabled ? 2 : 0, write.LatencyUnknown);
            CancelOnUnregister = true;
            server->Stop();
            CancelOnUnregister = false;
            const auto final = server->GetStats(snapshot.SimpleStats);
            UNIT_ASSERT_VALUES_EQUAL(
                1, final.SimpleStats.Requests[0].FinalErrors);
            UNIT_ASSERT_VALUES_EQUAL(
                enabled ? 1 : 0, final.SimpleStats.Requests[0].LatencyBad);
            UNIT_ASSERT_VALUES_EQUAL(
                4, final.SimpleStats.Requests[1].FinalCount);
            UNIT_ASSERT_VALUES_EQUAL(
                1, final.SimpleStats.Requests[1].FinalErrors);
        }
    }

    Y_UNIT_TEST(ShouldKeepPreviousStatsUntilNextCompletionSnapshot)
    {
        auto backend = std::make_shared<TStatsBackend>();
        auto server = CreateServer(CreateLoggingService("console"), backend);
        TSimpleStats previous;
        previous.Completed = 12;
        previous.Requests[0].Errors = 3;

        const auto missing = server->GetStats(previous);
        UNIT_ASSERT_VALUES_EQUAL(12, missing.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(3, missing.SimpleStats.Requests[0].Errors);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), backend->RequestedTimeout);

        backend->Snapshot.emplace();
        backend->Snapshot->Completed = 17;
        backend->Snapshot->Requests[0].Errors = 4;
        const auto stats = server->GetStats(previous);
        UNIT_ASSERT_VALUES_EQUAL(17, stats.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(4, stats.SimpleStats.Requests[0].Errors);
        backend->Snapshot.reset();
        const auto next = server->GetStats(stats.SimpleStats);
        UNIT_ASSERT_VALUES_EQUAL(17, next.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(4, next.SimpleStats.Requests[0].Errors);
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
