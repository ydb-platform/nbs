#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/guid.h>
#include <util/memory/blob.h>

namespace {

using namespace NMonitoring;
using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

class TTest
{
    ui32 NumClients;
    NCloud::ITimerPtr Timer;
    std::shared_ptr<NCloud::TTestScheduler> Scheduler;
    NCloud::ILoggingServicePtr Logging;
    NCloud::IMonitoringServicePtr Monitoring;
    IStatsAggregatorPtr StatsAggregator;

public:
    TTest(ui32 numClients)
        : NumClients(numClients)
    {
        SetupTest();
        Start();
    }

    ~TTest()
    {
        Stop();
    }

    void Run() const
    {
        Scheduler->RunAllScheduledTasks();
    }

private:
    void SetupTest()
    {
        Timer = NCloud::CreateWallClockTimer();
        Scheduler = std::make_shared<NCloud::TTestScheduler>();

        Logging = NCloud::CreateLoggingService("console");

        Monitoring = NCloud::CreateMonitoringServiceStub();

        StatsAggregator = CreateStatsAggregator(
            Timer,
            Scheduler,
            Logging,
            Monitoring,
            UpdateClientStats);

        for (ui32 i = 0; i < NumClients; ++i) {
            auto counters = SetupClientMetrics();

            TStringStream out;
            auto encoder = CreateEncoder(&out, EFormat::SPACK);
            counters->Accept("", "", *encoder);

            StatsAggregator->AddStats(CreateGuidAsString(), out.Str());
        }
    }

    void Start()
    {
        if (Scheduler) {
            Scheduler->Start();
        }

        if (Logging) {
            Logging->Start();
        }

        if (StatsAggregator) {
            StatsAggregator->Start();
        }
    }

    void Stop()
    {
        if (StatsAggregator) {
            StatsAggregator->Stop();
        }

        if (Logging) {
            Logging->Stop();
        }

        if (Scheduler) {
            Scheduler->Stop();
        }
    }

    TDynamicCountersPtr SetupClientMetrics()
    {
        TDynamicCountersPtr counters = new TDynamicCounters();

        auto rootGroup = counters->GetSubgroup("counters", "blockstore");

        auto clientCounters = SetupComponentMetrics("client_stats");
        rootGroup->RegisterSubgroup("component", "client", clientCounters);

        auto clientVolumeGroup =
            counters->GetSubgroup("component", "client_volume");
        auto clientVolumeCounters =
            SetupComponentMetrics("client_volume_stats");
        clientVolumeGroup->RegisterSubgroup(
            "volume",
            CreateGuidAsString(),
            clientVolumeCounters);

        return counters;
    }

    TDynamicCounterPtr SetupComponentMetrics(const char* name)
    {
        auto data = NResource::Find(name);

        TDynamicCounterPtr counters = new TDynamicCounters;
        auto parser = NCloud::NBlockStore::CreateStatsParser(counters);

        DecodeJson(data, parser.get());
        return counters;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

static const TTest test1(1);
static const TTest test10(10);
static const TTest test50(50);

Y_CPU_BENCHMARK(Test1, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        test1.Run();
    }
}

Y_CPU_BENCHMARK(Test10, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        test10.Run();
    }
}

Y_CPU_BENCHMARK(Test50, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        test50.Run();
    }
}
