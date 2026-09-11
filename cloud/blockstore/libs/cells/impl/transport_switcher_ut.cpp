#include "transport_switcher.h"

#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCountingService: public TTestService
{
    ui32 ReadCount = 0;

    TCountingService()
    {
        ReadBlocksLocalHandler =
            [this](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(request);
            ++ReadCount;
            return MakeFuture(NProto::TReadBlocksLocalResponse{});
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

void Read(const IBlockStorePtr& endpoint)
{
    endpoint->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::make_shared<NProto::TReadBlocksLocalRequest>());
}

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    std::shared_ptr<TCountingService> Initial =
        std::make_shared<TCountingService>();
    std::shared_ptr<TCountingService> Better =
        std::make_shared<TCountingService>();

    IEndpointRouterPtr Router = CreateEndpointRouter(Initial);

    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>(TInstant::Zero());
    ILoggingServicePtr Logging = CreateLoggingService("console");

    ui32 FactoryCalls = 0;

    TDuration SettleTime = TDuration::Seconds(10);
    ITransportSwitcherPtr Switcher;

    // moves both clocks forward and lets everything due by now run
    void AdvanceTime(TDuration duration)
    {
        Timer->AdvanceTime(duration);
        Scheduler->AdvanceTime(duration);
        Scheduler->RunAllScheduledTasksUntilNow();
    }

    void StartSwitching(TEndpointFactory factory)
    {
        Switcher = StartTransportSwitching(
            Router,
            Initial,   // the endpoint the router starts on
            std::move(factory),
            Timer,
            Scheduler,
            Logging,
            "test-host",
            TTransportSwitcherConfig{
                .SettleTime = SettleTime,
            });
    }

    TEndpointFactory AlwaysSucceeds()
    {
        return [this](
                   NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler)
        {
            Y_UNUSED(handler);
            ++FactoryCalls;

            return TResultOrError<IBlockStorePtr>(Better);
        };
    }

    TEndpointFactory AlwaysFails()
    {
        return [this](
                   NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler)
        {
            Y_UNUSED(handler);
            ++FactoryCalls;

            return TResultOrError<IBlockStorePtr>(
                MakeError(E_REJECTED, "rdma client is down"));
        };
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTransportSwitcherTest)
{
    Y_UNIT_TEST(ShouldInstallEndpointIntoRouterWhenItIsReady)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        env.Switcher->GetEndpointHandler()->HandleConnected();

        Read(env.Router);

        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldStayOnFallbackWhenTheEndpointCannotBeCreated)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysFails());

        // asked once and not again: an endpoint that was handed back would
        // reconnect on its own, so a failure here is the rdma client itself
        // refusing, and nothing about it changes on a timer
        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);

        env.AdvanceTime(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldReturnToRdmaOnlyAfterItSettles)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();

        // the first connect moves the data over at once; only a link that has
        // dropped once has to serve out the wait
        handler->HandleConnected();
        handler->HandleDisconnected();
        handler->HandleConnected();

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);

        env.AdvanceTime(TDuration::Seconds(10));

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldSwitchToRdmaAtOnceWhenSettleTimeIsZero)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        env.Switcher->GetEndpointHandler()->HandleConnected();

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldNotSwitchWhenRdmaBreaksWhileSettling)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected();

        env.AdvanceTime(TDuration::Seconds(5));
        handler->HandleDisconnected();
        env.AdvanceTime(TDuration::Seconds(10));

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldFallBackToGrpcWhenRdmaBreaks)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected();
        handler->HandleDisconnected();

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldReturnToRdmaAfterItComesBack)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected();
        handler->HandleDisconnected();
        handler->HandleConnected();

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldIgnoreUnavailable)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected();
        handler->HandleUnavailable();

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldTolerateRepeatedConnected)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected();
        handler->HandleConnected();

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldNotSettleOntoAReleasedRouter)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        env.Switcher->GetEndpointHandler()->HandleConnected();
        env.Router.reset();

        // the settle timer must find the router gone and do nothing
        env.AdvanceTime(TDuration::Seconds(10));
    }

    Y_UNIT_TEST(ShouldSwitchToRdmaAtOnceOnTheFirstConnect)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        env.Switcher->GetEndpointHandler()->HandleConnected();

        // the settle time guards a return to a link that has already proved it
        // can drop; a link that has never dropped has nothing to prove
        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldRestartTheSettleTimeOnReconnect)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected();

        env.AdvanceTime(TDuration::Seconds(5));
        handler->HandleDisconnected();
        handler->HandleConnected();

        // the first timer is due now, but its generation is stale
        env.AdvanceTime(TDuration::Seconds(5));
        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);

        env.AdvanceTime(TDuration::Seconds(5));
        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }
}

}   // namespace NCloud::NBlockStore::NCells
