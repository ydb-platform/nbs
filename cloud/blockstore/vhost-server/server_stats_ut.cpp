#include "backend.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NVHostServer {
namespace {

ui64 StatsNowCycles = 0;

// Bind the implementation's clock to controlled time in this translation unit.
// The production server and its interface keep using the real clock.
ui64 GetCycleCount()
{
    return StatsNowCycles;
}

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

Y_UNIT_TEST_SUITE(TServerStatsFreshnessTest)
{
    Y_UNIT_TEST(ShouldCheckCompletionSnapshotAge)
    {
        auto backend = std::make_shared<TStatsBackend>();
        auto server = CreateServer(CreateLoggingService("console"), backend);
        const auto limit = DurationToCyclesSafe(TDuration::Seconds(1));
        StatsNowCycles = limit * 10;
        TSimpleStats previous;
        previous.Completed = 12;
        previous.Requests[0].LatencyBad = 3;

        const auto missing = server->GetStats(previous);
        UNIT_ASSERT(!missing.Fresh);
        UNIT_ASSERT_VALUES_EQUAL(12, missing.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(3, missing.SimpleStats.Requests[0].LatencyBad);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), backend->RequestedTimeout);

        for (const auto [captured, expectedFresh]:
             {std::pair<ui64, bool>{0, false},
              {StatsNowCycles, true},
              {StatsNowCycles - limit, true},
              {StatsNowCycles - limit - 1, false},
              {StatsNowCycles + 1, false}})
        {
            backend->Snapshot.emplace();
            backend->Snapshot->CompletionSnapshotCycles = captured;
            backend->Snapshot->Completed = 17;
            backend->Snapshot->Requests[0].LatencyBad = 4;
            const auto stats = server->GetStats(previous);
            UNIT_ASSERT_VALUES_EQUAL_C(expectedFresh, stats.Fresh, captured);
            UNIT_ASSERT_VALUES_EQUAL(17, stats.SimpleStats.Completed);
            UNIT_ASSERT_VALUES_EQUAL(4, stats.SimpleStats.Requests[0].LatencyBad);
        }
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
