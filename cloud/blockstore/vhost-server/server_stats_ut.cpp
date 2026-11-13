#include "backend.h"

#include <library/cpp/testing/unittest/registar.h>

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
    Y_UNIT_TEST(ShouldKeepPreviousStatsUntilNextCompletionSnapshot)
    {
        auto backend = std::make_shared<TStatsBackend>();
        auto server = CreateServer(CreateLoggingService("console"), backend);
        TSimpleStats previous;
        previous.Completed = 12;
        previous.Requests[0].LatencyBad = 3;

        const auto missing = server->GetStats(previous);
        UNIT_ASSERT_VALUES_EQUAL(12, missing.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(3, missing.SimpleStats.Requests[0].LatencyBad);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), backend->RequestedTimeout);

        backend->Snapshot.emplace();
        backend->Snapshot->Completed = 17;
        backend->Snapshot->Requests[0].LatencyBad = 4;
        const auto stats = server->GetStats(previous);
        UNIT_ASSERT_VALUES_EQUAL(17, stats.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(4, stats.SimpleStats.Requests[0].LatencyBad);
        backend->Snapshot.reset();
        const auto next = server->GetStats(stats.SimpleStats);
        UNIT_ASSERT_VALUES_EQUAL(17, next.SimpleStats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(4, next.SimpleStats.Requests[0].LatencyBad);
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
