#include "tablet_throttler.h"

#include "tablet_throttler_logger.h"
#include "tablet_throttler_policy.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/context.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

class TSampleActorWithThrottler final: public TActor<TSampleActorWithThrottler>
{
public:
    TSampleActorWithThrottler()
        : TActor(&TThis::StateWork)
    {}

    void ResetThrottler(ITabletThrottlerPtr throttler)
    {
        Throttler = std::move(throttler);
    }

    STRICT_STFUNC(
        StateWork, HFunc(NActors::TEvents::TEvWakeup, HandleWakeUp);
        HFunc(NActors::TEvents::TEvFlushLog, HandleFlush));

    STRICT_STFUNC(
        StateZombie, HFunc(NActors::TEvents::TEvWakeup, RejectRequest);)

    void HandleWakeUp(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        if (RequestsCount++ == 0) {
            // The first request should be postponed

            auto callContext =
                MakeIntrusive<TCallContextBase>(static_cast<ui64>(0));
            auto requestInfo = TThrottlingRequestInfo{};

            Throttler->Throttle(
                ctx,
                callContext,
                requestInfo,
                [ev]() -> NActors::IEventHandlePtr
                { return NActors::IEventHandlePtr(ev.Release()); },
                "TestMethod");
        } else {
            Become(&TThis::StateZombie);

            Throttler->OnShutDown(ctx);
        }
    }

    void HandleFlush(
        const NActors::TEvents::TEvFlushLog::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Throttler->StartFlushing(ctx);
    }

    static void RejectRequest(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response = std::make_unique<NActors::TEvents::TEvActorDied>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }

private:
    ITabletThrottlerPtr Throttler;

    ui64 RequestsCount = 0;
};

/////////////////////////////////////////////////////////////////////////////

struct TTabletThrottlerLoggerStub: public ITabletThrottlerLogger
{
    void LogRequestPostponedBeforeSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        TDuration delay,
        const char* methodName) const override
    {
        Y_UNUSED(ctx, callContext, delay, methodName);
    }

    void LogRequestPostponedAfterSchedule(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        ui32 postponedCount,
        const char* methodName) const override
    {
        Y_UNUSED(ctx, callContext, postponedCount, methodName);
    }

    void LogRequestAdvanced(
        const NActors::TActorContext& ctx,
        TCallContextBase& callContext,
        const char* methodName,
        ui32 opType,
        TDuration delay) const override
    {
        Y_UNUSED(ctx, callContext, methodName, opType, delay);
    }
};

//////////////////////////////////////////////////////////////////////////////

struct TTabletThrottlerPolicyAlwaysPostpone: public ITabletThrottlerPolicy
{
    bool TryPostpone(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) override
    {
        Y_UNUSED(ts, requestInfo);
        return true;
    }

    TMaybe<TDuration> SuggestDelay(
        TInstant ts,
        TDuration queueTime,
        const TThrottlingRequestInfo& requestInfo) override
    {
        Y_UNUSED(ts, queueTime, requestInfo);
        return TDuration::Seconds(1);
    }

    void OnPostponedEvent(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) override
    {
        Y_UNUSED(ts, requestInfo);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TTabletThrottlerTest)
{
    /**
     * Scenario that caused a crash:
     * 1. A request is present in the postponed queue
     * 2. Throttler flush is initiated
     * 3. During the processing of the postponed queue, an actor shutdown is
     *    initiated
     * 4. During the shutdown, Throttle->OnShutDown is called. It is not
     *    supposed to process the request that was initially in the postponed
     *    queue the second time.
     */
    Y_UNIT_TEST(ShouldNotFlushNullEventOnShutdown)
    {
        TTabletThrottlerLoggerStub logger;
        TTabletThrottlerPolicyAlwaysPostpone policy;

        std::unique_ptr<TSampleActorWithThrottler> actor =
            std::make_unique<TSampleActorWithThrottler>();
        auto throttler = CreateTabletThrottler(*actor, logger, policy);
        actor->ResetThrottler(std::move(throttler));

        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        auto senderId = runtime.AllocateEdgeActor();

        auto actorId = runtime.Register(actor.release());

        // One request is postponed
        runtime.Send(
            TAutoPtr<IEventHandle>(new IEventHandle(
                actorId,
                senderId,
                new NActors::TEvents::TEvWakeup())));

        // Flush is initiated
        runtime.Send(
            TAutoPtr<IEventHandle>(new IEventHandle(
                actorId,
                senderId,
                new NActors::TEvents::TEvFlushLog())));
    }
}

}   // namespace NCloud
