#include "tablet_throttler.h"

#include "tablet_throttler_logger.h"
#include "tablet_throttler_policy.h"

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/datetime/base.h>
#include <util/generic/list.h>
#include <util/system/datetime.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TTabletThrottler final
    : public ITabletThrottler
{
private:
    NActors::IActor& Owner;
    ITabletThrottlerLogger& Logger;
    ITabletThrottlerPolicy& Policy;

    struct TPostponedRequest
    {
        TThrottlingRequestInfo Info;
        TCallContextBasePtr CallContext;
        NActors::IEventHandlePtr Event;
        TMaybe<double> QuotaCostShare;
    };
    // TODO: replace with a ring buffer
    TList<TPostponedRequest> PostponedRequests;
    bool PostponedQueueFlushScheduled = false;
    bool PostponedQueueFlushInProgress = false;

public:
    TTabletThrottler(
            NActors::IActor& owner,
            ITabletThrottlerLogger& logger,
            ITabletThrottlerPolicy& policy)
        : Owner(owner)
        , Logger(logger)
        , Policy(policy)
    {}

    ui64 GetPostponedRequestsCount() const override
    {
        return PostponedRequests.size();
    }

    void ResetPolicy(ITabletThrottlerPolicy& policy) override
    {
        Policy = policy;
    }

    void OnShutDown(const NActors::TActorContext&) override
    {
        if (PostponedQueueFlushInProgress) {
            // We are already in the process of flushing postponed requests
            return;
        }
        PostponedQueueFlushScheduled = false;

        while (PostponedRequests.size()) {
            auto& x = PostponedRequests.front();
            x.CallContext->Advance(GetCycleCount());

            TAutoPtr<NActors::IEventHandle> ev = x.Event.release();
            Owner.Receive(ev);

            // When shutting down, we do not expect that the actor will try to
            // schedule flushing again
            Y_ABORT_UNLESS(!PostponedQueueFlushScheduled);
            PostponedRequests.pop_front();
        }
    }

    void StartFlushing(const NActors::TActorContext& ctx) override
    {
        Y_DEBUG_ABORT_UNLESS(PostponedQueueFlushScheduled);
        PostponedQueueFlushScheduled = false;
        PostponedQueueFlushInProgress = true;

        while (PostponedRequests.size()) {
            auto& x = PostponedRequests.front();
            Policy.OnPostponedEvent(ctx.Now(), x.Info);

            TAutoPtr<NActors::IEventHandle> ev = x.Event.release();
            Owner.Receive(ev);

            if (PostponedQueueFlushScheduled) {
                Y_ABORT_UNLESS(x.Event);
                break;
            }

            PostponedRequests.pop_front();
        }

        PostponedQueueFlushInProgress = false;
    }

    ETabletThrottlerStatus Throttle(
        const NActors::TActorContext& ctx,
        TCallContextBasePtr callContext,
        const TThrottlingRequestInfo& requestInfo,
        const std::function<NActors::IEventHandlePtr(void)>& eventReleaser,
        const char* methodName) override
    {
        // Keep the arrival-time share across redelivery and policy changes.
        // This is a proportional estimate, not a reconstruction of the wait.
        TMaybe<double> quotaCostShare;
        if (PostponedQueueFlushInProgress) {
            quotaCostShare = PostponedRequests.front().QuotaCostShare;
        } else {
            quotaCostShare =
                Policy.GetQuotaCostShare(requestInfo);
        }

        bool rejected = false;
        TDuration queueTime = TDuration::Zero();
        if (PostponedRequests && !PostponedQueueFlushInProgress) {
            Y_DEBUG_ABORT_UNLESS(PostponedQueueFlushScheduled);

            if (Policy.TryPostpone(ctx.Now(), requestInfo)) {
                Logger.LogRequestPostponedAfterSchedule(
                    ctx,
                    *callContext,
                    GetPostponedRequestsCount(),
                    methodName);

                Postpone(
                    ctx,
                    requestInfo,
                    std::move(callContext),
                    eventReleaser(),
                    quotaCostShare);

                return ETabletThrottlerStatus::POSTPONED;
            }

            rejected = true;
        } else {
            const auto nowTs = ctx.Now();
            const auto postponeTs = callContext->GetPostponeTs();
            if (postponeTs) {
                queueTime = nowTs - postponeTs;
            }
            const auto delay = Policy.SuggestDelay(nowTs, queueTime, requestInfo);

            if (delay.Defined()) {
                if (delay->GetValue()) {
                    Logger.LogRequestPostponedBeforeSchedule(
                        ctx,
                        *callContext,
                        *delay,
                        methodName);

                    Postpone(
                        ctx,
                        requestInfo,
                        std::move(callContext),
                        eventReleaser(),
                        quotaCostShare);

                    Y_DEBUG_ABORT_UNLESS(!PostponedQueueFlushScheduled);
                    PostponedQueueFlushScheduled = true;

                    ctx.Schedule(
                        Max(*delay, MinPostponeQueueFlushInterval),
                        new NActors::TEvents::TEvWakeup());

                    return ETabletThrottlerStatus::POSTPONED;
                }
            } else {
                rejected = true;
            }
        }

        if (rejected) {
            const TDuration waited =
                PostponedQueueFlushInProgress ? queueTime : TDuration::Zero();
            SetQuotaDelay(*callContext, quotaCostShare, waited);
            return ETabletThrottlerStatus::REJECTED;
        }

        TDuration delay = TDuration::Zero();
        if (PostponedQueueFlushInProgress) {
            delay = callContext->Advance(GetCycleCount());
        }
        SetQuotaDelay(*callContext, quotaCostShare, delay);
        Logger.LogRequestAdvanced(
            ctx,
            *callContext,
            methodName,
            requestInfo.OpType,
            delay);

        return ETabletThrottlerStatus::ADVANCED;
    }

private:
    static void SetQuotaDelay(
        TCallContextBase& callContext,
        const TMaybe<double>& quotaCostShare,
        TDuration throttlerDelay)
    {
        // Scale measured waiting by the original limits' share of request cost.
        // Unsupported policies keep the existing unmeasured-delay behavior.
        if (!quotaCostShare && !callContext.GetThrottlerQuotaDelay()) {
            return;
        }
        callContext.SetThrottlerQuotaDelay(
            quotaCostShare
                ? TMaybe<TDuration>(TDuration::MicroSeconds(
                      throttlerDelay.MicroSeconds() * *quotaCostShare))
                : Nothing());
    }

    void Postpone(
        const NActors::TActorContext& ctx,
        TThrottlingRequestInfo requestInfo,
        TCallContextBasePtr callContext,
        NActors::IEventHandlePtr ev,
        const TMaybe<double>& quotaCostShare)
    {
        if (PostponedQueueFlushInProgress) {
            Y_DEBUG_ABORT_UNLESS(!PostponedRequests.front().Event);
            auto& pr = PostponedRequests.front();
            pr.Event = std::move(ev);
            pr.Info = requestInfo;
        } else {
            callContext->Postpone(GetCycleCount());
            callContext->SetPostponeTs(ctx.Now());
            PostponedRequests.push_back({
                requestInfo,
                std::move(callContext),
                std::move(ev),
                quotaCostShare});
       }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletThrottlerPtr CreateTabletThrottler(
    NActors::IActor& owner,
    ITabletThrottlerLogger& logger,
    ITabletThrottlerPolicy& policy)
{
    return std::make_unique<TTabletThrottler>(owner, logger, policy);
}

}   // namespace NCloud
