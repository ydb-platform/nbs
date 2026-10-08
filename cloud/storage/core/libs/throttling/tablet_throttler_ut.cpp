#include "tablet_throttler.h"

#include "tablet_throttler_logger.h"
#include "tablet_throttler_policy.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/context.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>

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

//////////////////////////////////////////////////////////////////////////////

struct TScriptedThrottlerPolicy: public ITabletThrottlerPolicy
{
    TDeque<TMaybe<TDuration>> Delays;
    TMaybe<double> QuotaCostShare;
    mutable ui32 QuotaShareQueries = 0;

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
        UNIT_ASSERT(!Delays.empty());
        auto delay = Delays.front();
        Delays.pop_front();
        return delay;
    }

    void OnPostponedEvent(
        TInstant ts,
        const TThrottlingRequestInfo& requestInfo) override
    {
        Y_UNUSED(ts, requestInfo);
    }

    TMaybe<double> GetQuotaCostShare(
        const TThrottlingRequestInfo& requestInfo) const override
    {
        Y_UNUSED(requestInfo);
        ++QuotaShareQueries;
        return QuotaCostShare;
    }
};

//////////////////////////////////////////////////////////////////////////////

struct TThrottledRequests
{
    TVector<TCallContextBasePtr> CallContexts;
    TVector<ETabletThrottlerStatus> Statuses;
};

// TEvPing carries a request (its cookie is the index of the call context),
// TEvFlushLog starts flushing. The flush scheduled by the throttler itself is
// ignored to keep the test deterministic.
class TActorWithQuotaThrottler final: public TActor<TActorWithQuotaThrottler>
{
private:
    ITabletThrottlerPtr Throttler;
    TThrottledRequests& Requests;

public:
    explicit TActorWithQuotaThrottler(TThrottledRequests& requests)
        : TActor(&TThis::StateWork)
        , Requests(requests)
    {}

    void ResetThrottler(ITabletThrottlerPtr throttler)
    {
        Throttler = std::move(throttler);
    }

    STRICT_STFUNC(
        StateWork, HFunc(NActors::TEvents::TEvPing, HandleRequest);
        HFunc(NActors::TEvents::TEvFlushLog, HandleFlush);
        IgnoreFunc(NActors::TEvents::TEvWakeup));

    void HandleRequest(
        const NActors::TEvents::TEvPing::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const ui64 index = ev->Cookie;
        auto callContext = Requests.CallContexts[index];
        Requests.Statuses[index] = Throttler->Throttle(
            ctx,
            callContext,
            TThrottlingRequestInfo{.ByteCount = 4096},
            [ev]() -> NActors::IEventHandlePtr
            { return NActors::IEventHandlePtr(ev.Release()); },
            "TestMethod");
    }

    void HandleFlush(
        const NActors::TEvents::TEvFlushLog::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Throttler->StartFlushing(ctx);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TTabletThrottlerTest)
{
    Y_UNIT_TEST(ShouldReportQuotaDelay)
    {
        TTabletThrottlerLoggerStub logger;
        TScriptedThrottlerPolicy policy;
        TThrottledRequests requests;
        for (ui64 i = 0; i < 3; ++i) {
            requests.CallContexts.push_back(
                MakeIntrusive<TCallContextBase>(i));
            requests.Statuses.push_back(ETabletThrottlerStatus::ADVANCED);
        }

        auto actor = std::make_unique<TActorWithQuotaThrottler>(requests);
        actor->ResetThrottler(CreateTabletThrottler(*actor, logger, policy));

        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto senderId = runtime.AllocateEdgeActor();
        const auto actorId = runtime.Register(actor.release());

        const auto send = [&](NActors::IEventBase* event, ui64 cookie = 0)
        {
            runtime.Send(TAutoPtr<IEventHandle>(
                new IEventHandle(actorId, senderId, event, 0, cookie)));
        };

        // Capture the share at arrival, not when the queue is redelivered.
        policy.QuotaCostShare = 0.5;
        policy.Delays = {TDuration::Seconds(1), TDuration::Zero()};
        send(new NActors::TEvents::TEvPing(), 0);
        UNIT_ASSERT_EQUAL(
            ETabletThrottlerStatus::POSTPONED,
            requests.Statuses[0]);

        policy.QuotaCostShare = 1.;
        Sleep(TDuration::MilliSeconds(20));
        send(new NActors::TEvents::TEvFlushLog());
        UNIT_ASSERT_EQUAL(ETabletThrottlerStatus::ADVANCED, requests.Statuses[0]);
        // The share is queried once per request, not per redelivery.
        UNIT_ASSERT_VALUES_EQUAL(1, policy.QuotaShareQueries);
        const auto& postponed = *requests.CallContexts[0];
        UNIT_ASSERT(
            postponed.Time(EProcessingStage::Postponed) >=
            TDuration::MilliSeconds(20));
        UNIT_ASSERT(postponed.GetThrottlerQuotaDelay());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(
                postponed.Time(EProcessingStage::Postponed).MicroSeconds() / 2),
            *postponed.GetThrottlerQuotaDelay());

        // Immediate rejection has no measured waiting to attribute.
        policy.QuotaCostShare = 1.;
        policy.Delays = {Nothing()};
        send(new NActors::TEvents::TEvPing(), 1);
        UNIT_ASSERT_EQUAL(ETabletThrottlerStatus::REJECTED, requests.Statuses[1]);
        const auto& rejected = *requests.CallContexts[1];
        UNIT_ASSERT(rejected.GetThrottlerQuotaDelay());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            *rejected.GetThrottlerQuotaDelay());

        // The policy does not measure the quota delay.
        policy.QuotaCostShare = Nothing();
        policy.Delays = {TDuration::Zero()};
        send(new NActors::TEvents::TEvPing(), 2);
        UNIT_ASSERT_EQUAL(ETabletThrottlerStatus::ADVANCED, requests.Statuses[2]);
        const auto& unmeasured = *requests.CallContexts[2];
        UNIT_ASSERT(!unmeasured.GetThrottlerQuotaDelay());
    }

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
