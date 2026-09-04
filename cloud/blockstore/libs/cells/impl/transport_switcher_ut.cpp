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

    // moves both clocks forward and lets everything due by now run
    void AdvanceTime(TDuration duration)
    {
        Timer->AdvanceTime(duration);
        Scheduler->AdvanceTime(duration);
        Scheduler->RunAllScheduledTasksUntilNow();
    }

    void StartSwitching(TEndpointFactory factory)
    {
        StartTransportSwitching(
            Router,
            std::move(factory),
            Timer,
            Scheduler,
            Logging,
            "test-host",
            TTransportSwitcherConfig{
                .InitialRetryDelay = TDuration::Seconds(1),
                .MaxRetryDelay = TDuration::Seconds(4),
            });
    }

    TEndpointFactory AlwaysSucceeds()
    {
        return FailsThenSucceeds(0);
    }

    TEndpointFactory FailsThenSucceeds(ui32 failures)
    {
        return [this, failures]
        {
            ++FactoryCalls;

            if (FactoryCalls <= failures) {
                return MakeFuture(TResultOrError<IBlockStorePtr>(
                    MakeError(E_REJECTED, "endpoint is not up yet")));
            }

            return MakeFuture(TResultOrError<IBlockStorePtr>(Better));
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
        env.StartSwitching(env.AlwaysSucceeds());

        Read(env.Router);

        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldRetryAfterFailedAttempt)
    {
        TTestEnv env;
        env.StartSwitching(env.FailsThenSucceeds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, env.FactoryCalls);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldNotRetryBeforeDelayElapses)
    {
        TTestEnv env;
        env.StartSwitching(env.FailsThenSucceeds(1));

        env.Scheduler->RunAllScheduledTasksUntilNow();
        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);

        env.AdvanceTime(TDuration::MilliSeconds(999));
        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);

        env.AdvanceTime(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, env.FactoryCalls);
    }

    Y_UNIT_TEST(ShouldGrowDelayBetweenAttempts)
    {
        TTestEnv env;
        env.StartSwitching(env.FailsThenSucceeds(2));

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, env.FactoryCalls);

        // the second delay is twice the first one, so one second is no longer
        // enough to trigger the next attempt
        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, env.FactoryCalls);

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(3, env.FactoryCalls);
    }

    // The switcher cannot cancel an attempt that is already in flight, so it
    // outlives the router until that attempt completes. Observed through the
    // factory, which the switcher owns and releases along with itself.
    Y_UNIT_TEST(ShouldOutliveRouterUntilPendingAttemptCompletes)
    {
        auto sentinel = std::make_shared<int>(0);
        std::weak_ptr<int> weakSentinel = sentinel;

        auto promise = NewPromise<TResultOrError<IBlockStorePtr>>();

        TTestEnv env;
        env.StartSwitching(
            [sentinel = std::move(sentinel), promise]
            { return promise.GetFuture(); });

        env.Router.reset();

        UNIT_ASSERT_C(
            weakSentinel.lock(),
            "switcher was released while its attempt was still in flight");

        promise.SetValue(TResultOrError<IBlockStorePtr>(
            MakeError(E_REJECTED, "endpoint is not up yet")));

        UNIT_ASSERT_C(
            !weakSentinel.lock(),
            "switcher was not released once the pending attempt completed");
    }

    Y_UNIT_TEST(ShouldStopRetryingOnceRouterIsGone)
    {
        TTestEnv env;
        env.StartSwitching(env.FailsThenSucceeds(10));

        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);

        env.Router.reset();

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, env.FactoryCalls);
    }
}

}   // namespace NCloud::NBlockStore::NCells
