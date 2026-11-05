#include "tablet_proxy.h"

#include <cloud/storage/core/libs/api/tablet_proxy.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/protos/base.pb.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <memory>
#include <utility>

namespace NCloud::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTabletProxyActor final: public TActor<TTabletProxyActor>
{
private:
    struct TInflightRequest: public TIntrusiveListItem<TInflightRequest>
    {
        const ui64 RequestId;
        const ui64 TabletId;
        const TActorId Sender;
        const ui64 Cookie;

        TInflightRequest(
                ui64 requestId,
                ui64 tabletId,
                TActorId sender,
                ui64 cookie)
            : RequestId(requestId)
            , TabletId(tabletId)
            , Sender(sender)
            , Cookie(cookie)
        {}
    };

    struct TTablet
    {
        const TActorId ClientId;
        const ui64 Generation = 0;

        TIntrusiveList<TInflightRequest> Inflight;
        TInstant LastActivity = TInstant::Now();
        bool IdleCheckScheduled = false;

        TTablet(TActorId clientId, ui64 generation)
            : ClientId(clientId)
            , Generation(generation)
        {}
    };

private:
    const TTabletProxyConfig Config;

    THashMap<ui64, TTablet> Tablets;

    // by local request id, linked into the tablet's list
    THashMap<ui64, TInflightRequest> Requests;
    ui64 LastRequestId = 0;
    ui64 LastTabletGeneration = 0;

public:
    explicit TTabletProxyActor(TTabletProxyConfig config)
        : TActor(&TThis::StateWork)
        , Config(std::move(config))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletProxy::TEvForward, HandleForward);
            HFunc(TEvTabletPipe::TEvClientConnected, HandleClientConnected);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleClientDestroyed);
            HFunc(TEvents::TEvWakeup, HandleIdleCheck);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            default:
                HandleTabletEvent(ev, ActorContext());
                break;
        }
    }

    void HandleForward(
        const TEvTabletProxy::TEvForward::TPtr& ev,
        const TActorContext& ctx)
    {
        const ui64 tabletId = ev->Get()->TabletId;
        auto request = std::move(ev->Get()->Request);

        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            auto client = ctx.Register(NTabletPipe::CreateClient(
                SelfId(),
                tabletId,
                NTabletPipe::TClientRetryPolicy{
                    .RetryLimitCount = Config.PipeClientRetryCount,
                    .MinRetryTime = Config.PipeClientMinRetryTime,
                    .MaxRetryTime = Config.PipeClientMaxRetryTime}));

            it = Tablets.try_emplace(tabletId, client, ++LastTabletGeneration).first;
        }

        auto& tablet = it->second;

        const ui64 requestId = ++LastRequestId;
        auto [entry, inserted] = Requests.try_emplace(
            requestId,
            requestId,
            tabletId,
            request->Sender,
            request->Cookie);
        Y_ABORT_UNLESS(inserted);

        tablet.Inflight.PushBack(&entry->second);
        tablet.LastActivity = ctx.Now();

        if (request->HasEvent()) {
            NTabletPipe::SendData(
                SelfId(),
                tablet.ClientId,
                request->ReleaseBase().Release(),
                requestId,
                std::move(request->TraceId));
        } else {
            NTabletPipe::SendData(
                SelfId(),
                tablet.ClientId,
                request->Type,
                request->ReleaseChainBuffer(),
                requestId,
                std::move(request->TraceId));
        }
    }

    void HandleTabletEvent(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx)
    {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_WARN(
                ctx,
                Config.LogComponent,
                "Unexpected event 0x%08X from %s",
                ev->GetTypeRewrite(),
                ToString(ev->Sender).c_str());
            return;
        }

        const ui64 tabletId = it->second.TabletId;
        const TActorId sender = it->second.Sender;
        const ui64 cookie = it->second.Cookie;
        Requests.erase(it);

        const TActorId forward = ev->GetForwardOnNondeliveryRecipient();
        if (ev->HasEvent()) {
            ctx.Send(new IEventHandle(
                sender,
                ev->Sender,
                ev->ReleaseBase().Release(),
                ev->Flags,
                cookie,
                forward ? &forward : nullptr,
                std::move(ev->TraceId)));
        } else {
            ctx.Send(new IEventHandle(
                ev->Type,
                ev->Flags,
                sender,
                ev->Sender,
                ev->ReleaseChainBuffer(),
                cookie,
                forward ? &forward : nullptr,
                std::move(ev->TraceId)));
        }

        auto tit = Tablets.find(tabletId);

        Y_ABORT_UNLESS(tit != Tablets.end(), "lost tablet state for [%lu]", tabletId);
        ScheduleIdleCheck(ctx, tabletId, tit->second);
    }

    void ScheduleIdleCheck(
        const TActorContext& ctx,
        ui64 tabletId,
        TTablet& tablet)
    {
        tablet.LastActivity = ctx.Now();
        if (Config.PipeInactivityTimeout != TDuration::Zero() &&
            tablet.Inflight.Empty() &&
            !tablet.IdleCheckScheduled)
        {
            ScheduleWakeup(ctx, tabletId, tablet, Config.PipeInactivityTimeout);
        }
    }

    void ScheduleWakeup(
        const TActorContext& ctx,
        ui64 tabletId,
        TTablet& tablet,
        TDuration delay)
    {
        tablet.IdleCheckScheduled = true;
        ctx.Schedule(
            delay,
            std::make_unique<IEventHandle>(
                SelfId(),
                SelfId(),
                new TEvents::TEvWakeup(tabletId),
                0,   // flags
                tablet.Generation));
    }

    void HandleIdleCheck(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        const ui64 tabletId = ev->Get()->Tag;
        auto it = Tablets.find(tabletId);
        if (it == Tablets.end() || it->second.Generation != ev->Cookie) {
            return;
        }

        auto& tablet = it->second;
        tablet.IdleCheckScheduled = false;
        if (!tablet.Inflight.Empty()) {
            return;
        }

        TInstant deadline = tablet.LastActivity + Config.PipeInactivityTimeout;
        if (ctx.Now() < deadline) {
            ScheduleWakeup(ctx, tabletId, tablet, deadline - ctx.Now());
            return;
        }

        LOG_INFO(
            ctx,
            Config.LogComponent,
            "Pipe to tablet %lu closed idle",
            tabletId);

        NTabletPipe::CloseClient(ctx, tablet.ClientId);
        Tablets.erase(it);
    }

    void HandleClientConnected(
        const TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        auto* tablet = Tablets.FindPtr(msg->TabletId);
        if (!tablet || tablet->ClientId != msg->ClientId ||
            msg->Status == NKikimrProto::OK)
        {
            return;
        }

        OnPipeBroken(
            ctx,
            msg->TabletId,
            TStringBuilder()
                << "could not connect to tablet " << msg->TabletId << ": "
                << NKikimrProto::EReplyStatus_Name(msg->Status)
                << (msg->Dead ? ", tablet is dead" : ""));
    }

    void HandleClientDestroyed(
        const TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        auto* tablet = Tablets.FindPtr(msg->TabletId);
        if (!tablet || tablet->ClientId != msg->ClientId) {
            return;
        }

        OnPipeBroken(
            ctx,
            msg->TabletId,
            TStringBuilder() << "pipe to tablet " << msg->TabletId << " broke");
    }

    void OnPipeBroken(
        const TActorContext& ctx,
        ui64 tabletId,
        const TString& reason)
    {
        LOG_INFO(ctx, Config.LogComponent, "%s", reason.c_str());

        auto it = Tablets.find(tabletId);
        Fail(ctx, tabletId, it->second, reason);
        Tablets.erase(it);
    }

    void Fail(
        const TActorContext& ctx,
        ui64 tabletId,
        TTablet& tablet,
        const TString& reason)
    {
        const auto error = MakeError(E_REJECTED, reason);
        while (!tablet.Inflight.Empty()) {
            const auto* inflight = tablet.Inflight.PopFront();
            const ui64 requestId = inflight->RequestId;
            ctx.Send(
                inflight->Sender,
                new TEvTabletProxy::TEvDisconnected(error, tabletId),
                0,   // flags
                inflight->Cookie);
            Requests.erase(requestId);
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        for (auto& [tabletId, tablet]: Tablets) {
            NTabletPipe::CloseClient(ctx, tablet.ClientId);
            Fail(ctx, tabletId, tablet, "tablet proxy is shutting down");
        }

        Tablets.clear();
        Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateTabletProxy(TTabletProxyConfig config)
{
    return std::make_unique<TTabletProxyActor>(std::move(config));
}

}   // namespace NCloud::NStorage
