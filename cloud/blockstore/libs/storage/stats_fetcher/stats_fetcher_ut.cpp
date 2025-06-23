#include "stats_fetcher.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStatsFetcherMock: public NCloud::NStorage::IStatsFetcher
{
    TResultOrError<TDuration> CpuWaitValue = TDuration::Zero();

    void SetCpuWaitValue(TResultOrError<TDuration> value)
    {
        CpuWaitValue = std::move(value);
    }

    void Start() override
    {}

    void Stop() override
    {}

    TResultOrError<TDuration> GetCpuWait() override
    {
        return CpuWaitValue;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    TTestEnv Env;
    TActorId Sender;

    TStorageConfigPtr StorageConfig;
    std::shared_ptr<TStatsFetcherMock> Fetcher;

    TBootstrap()
    {
        NProto::TStorageServiceConfig configProto;
        StorageConfig = std::make_shared<TStorageConfig>(
            configProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        Fetcher = std::make_shared<TStatsFetcherMock>();

        NActors::IActorPtr actor =
            CreateStatsFetcherActor(StorageConfig, Fetcher);
        Sender = Env.GetRuntime().Register(actor.release());
        Env.GetRuntime().EnableScheduleForActor(Sender);
    }

    void AdjustTime(TDuration interval)
    {
        Env.GetRuntime().AdvanceCurrentTime(interval);
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
    }

    void Send(const TActorId& recipient, IEventBasePtr event)
    {
        Env.GetRuntime().Send(
            new IEventHandle(recipient, Sender, event.release()));
    }

    void DispatchEvents(TDuration timeout)
    {
        Env.GetRuntime().DispatchEvents(TDispatchOptions(), timeout);
    }

    void SendWakeUpEvent(TActorId receiver)
    {
        Send(receiver, std::make_unique<TEvents::TEvWakeup>());
    }

    void MockCpuWaitAndRun(TResultOrError<TDuration> cpuWait, TDuration runFor)
    {
        auto now = Env.GetRuntime().GetCurrentTime();
        while (Env.GetRuntime().GetCurrentTime() - now < runFor) {
            AdjustTime(TDuration::Seconds(15));

            Fetcher->SetCpuWaitValue(
                HasError(cpuWait)
                    ? TResultOrError<TDuration>(cpuWait.GetError())
                    : TResultOrError<TDuration>(cpuWait.GetResult()));

            SendWakeUpEvent(Sender);

            DispatchEvents(TDuration::Seconds(1));
        }
    }
};

auto SetupCriticalEvents(IMonitoringServicePtr monitoring)
{
    auto rootGroup =
        monitoring->GetCounters()->GetSubgroup("counters", "storage");

    auto serverGroup = rootGroup->GetSubgroup("component", "server");
    InitCriticalEventsCounter(serverGroup);

    return serverGroup;
}
}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStatsFetcherActorTest)
{
    Y_UNIT_TEST(ShouldSetCpuWaitCounter)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto serverGroup = SetupCriticalEvents(monitoring);

        TBootstrap testEnv;

        testEnv.DispatchEvents(TDuration());

        testEnv.MockCpuWaitAndRun(
            TResultOrError<TDuration>(TDuration::Seconds(9)),
            TDuration::Seconds(15));

        auto counters = testEnv.Env.GetRuntime()
                            .GetAppData(0)
                            .Counters->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server");

        auto cpuWaitCounter = counters->GetCounter("CpuWait", false);
        UNIT_ASSERT_VALUES_UNEQUAL(0, cpuWaitCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            serverGroup
                ->GetCounter("AppCriticalEvents/CpuWaitCounterReadError", true)
                ->Val());
    }

    Y_UNIT_TEST(ShouldSetCpuWaitCounterReadError)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto serverGroup = SetupCriticalEvents(monitoring);

        TBootstrap testEnv;

        testEnv.DispatchEvents(TDuration());

        testEnv.MockCpuWaitAndRun(
            MakeError(E_INVALID_STATE),
            TDuration::Seconds(15));

        auto counters = testEnv.Env.GetRuntime()
                            .GetAppData(0)
                            .Counters->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server");

        auto cpuWaitCounter = counters->GetCounter("CpuWait", false);
        UNIT_ASSERT_VALUES_EQUAL(0, cpuWaitCounter->Val());

        UNIT_ASSERT_VALUES_UNEQUAL(
            0,
            serverGroup
                ->GetCounter("AppCriticalEvents/CpuWaitCounterReadError", true)
                ->Val());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
