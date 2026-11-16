#include "backend.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NVHostServer {
namespace {
vhd_bdev_info RegisteredInfo{};
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
    Y_UNIT_TEST(ShouldPublishTheDrainedBackendSnapshot)
    {
        auto backend = std::make_shared<TStatsBackend>();
        auto server = CreateServer(CreateLoggingService("console"), backend);
        TOptions options;
        options.QueueCount = 0;
        options.NoChmod = true;
        server->Start(options);
        backend->Snapshot.emplace();
        auto& read = backend->Snapshot->Requests[0];
        read.FinalCount = 2;
        read.LatencyGood = 2;
        const auto snapshot = server->GetStats({});
        UNIT_ASSERT_VALUES_EQUAL(2, snapshot.SimpleStats.Requests[0].LatencyGood);

        // The last backend completion must reach the final parent report.
        read.FinalErrors = 1;
        read.LatencyBad = 1;
        server->Stop();
        backend->Snapshot.reset();
        const auto final = server->GetStats(snapshot.SimpleStats);
        UNIT_ASSERT_VALUES_EQUAL(2, final.SimpleStats.Requests[0].FinalCount);
        UNIT_ASSERT_VALUES_EQUAL(1, final.SimpleStats.Requests[0].FinalErrors);
        UNIT_ASSERT_VALUES_EQUAL(2, final.SimpleStats.Requests[0].LatencyGood);
        UNIT_ASSERT_VALUES_EQUAL(1, final.SimpleStats.Requests[0].LatencyBad);
        UNIT_ASSERT_VALUES_EQUAL(0, final.SimpleStats.Requests[0].LatencyUnknown);
    }

    Y_UNIT_TEST(ShouldKeepPreviousStatsUntilNextCompletionSnapshot)
    {
        auto backend = std::make_shared<TStatsBackend>();
        auto server = CreateServer(CreateLoggingService("console"), backend);
        TSimpleStats previous;
        previous.Completed = 12;
        previous.Requests[0].Errors = 3;
        previous.Requests[0].LatencyBad = 3;

        const auto missing = server->GetStats(previous);
        UNIT_ASSERT_VALUES_EQUAL(12, missing.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(3, missing.SimpleStats.Requests[0].Errors);
        UNIT_ASSERT_VALUES_EQUAL(3, missing.SimpleStats.Requests[0].LatencyBad);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), backend->RequestedTimeout);

        backend->Snapshot.emplace();
        backend->Snapshot->Completed = 17;
        backend->Snapshot->Requests[0].Errors = 4;
        backend->Snapshot->Requests[0].LatencyBad = 4;
        const auto stats = server->GetStats(previous);
        UNIT_ASSERT_VALUES_EQUAL(17, stats.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(4, stats.SimpleStats.Requests[0].Errors);
        UNIT_ASSERT_VALUES_EQUAL(4, stats.SimpleStats.Requests[0].LatencyBad);
        backend->Snapshot.reset();
        const auto next = server->GetStats(stats.SimpleStats);
        UNIT_ASSERT_VALUES_EQUAL(17, next.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(4, next.SimpleStats.Requests[0].Errors);
        UNIT_ASSERT_VALUES_EQUAL(4, next.SimpleStats.Requests[0].LatencyBad);
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
