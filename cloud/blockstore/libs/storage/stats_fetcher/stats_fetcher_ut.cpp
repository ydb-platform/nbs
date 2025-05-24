#include "stats_fetcher.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/core/config.h>
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
    TResultOrError<TDuration> Value = TDuration::Zero();

    void SetCpuWaitValue(TResultOrError<TDuration> value)
    {
        Value = std::move(value);
    }

    void Start() override
    {}

    void Stop() override
    {}

    TResultOrError<TDuration> GetCpuWait() override
    {
        return Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStatsFetcherActorTestEnv
{
private:
    TTestEnv TestEnv;
    TActorId Sender;

public:
    std::shared_ptr<TStorageConfig> StorageConfig;
    std::shared_ptr<TStatsFetcherMock> Fetcher;

    TStatsFetcherActorTestEnv()
    {
        NProto::TStorageServiceConfig configProto;
        StorageConfig = std::make_shared<TStorageConfig>(
            configProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        Fetcher = std::make_shared<TStatsFetcherMock>();
    }

    TTestActorRuntime& GetRuntime()
    {
        return TestEnv.GetRuntime();
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = TestEnv.GetRuntime().Register(actor.release());
        TestEnv.GetRuntime().EnableScheduleForActor(actorId);

        return actorId;
    }

    void AdjustTime()
    {
        TestEnv.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));
        TestEnv.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
    }

    void AdjustTime(TDuration interval)
    {
        TestEnv.GetRuntime().AdvanceCurrentTime(interval);
        TestEnv.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
    }

    void Send(const TActorId& recipient, IEventBasePtr event)
    {
        TestEnv.GetRuntime().Send(
            new IEventHandle(recipient, Sender, event.release()));
    }

    void DispatchEvents()
    {
        TestEnv.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration());
    }

    void DispatchEvents(TDuration timeout)
    {
        TestEnv.GetRuntime().DispatchEvents(TDispatchOptions(), timeout);
    }

    void SendWakeUpEvent(TActorId receiver)
    {
        Send(receiver, std::make_unique<TEvents::TEvWakeup>());
    }
};

TString RunState(
    TStatsFetcherActorTestEnv& testEnv,
    TActorId actorId,
    TResultOrError<double> cpuWait,
    TDuration runFor)
{
    auto now = testEnv.GetRuntime().GetCurrentTime();
    while (testEnv.GetRuntime().GetCurrentTime() - now < runFor) {
        testEnv.AdjustTime();

        testEnv.Fetcher->SetCpuWaitValue(
            HasError(cpuWait)
                ? TResultOrError<TDuration>(cpuWait.GetError())
                : TResultOrError<TDuration>(
                      cpuWait.GetResult() * TDuration::Seconds(15)));

        testEnv.SendWakeUpEvent(actorId);

        testEnv.DispatchEvents(TDuration::Seconds(1));
    }

    return {};
}

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

        TStatsFetcherActorTestEnv testEnv;

        auto statsFetcherActorID = testEnv.Register(
            CreateStatsFetcherActor(testEnv.StorageConfig, testEnv.Fetcher));

        testEnv.DispatchEvents();

        RunState(testEnv, statsFetcherActorID, .9, TDuration::Seconds(15));

        auto counters = testEnv.GetRuntime()
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

        TStatsFetcherActorTestEnv testEnv;

        auto statsFetcherActorID = testEnv.Register(
            CreateStatsFetcherActor(testEnv.StorageConfig, testEnv.Fetcher));

        testEnv.DispatchEvents();

        RunState(
            testEnv,
            statsFetcherActorID,
            MakeError(E_INVALID_STATE),
            TDuration::Seconds(15));

        auto counters = testEnv.GetRuntime()
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
