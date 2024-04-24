#include "tablet_actor.h"

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCleanupSessionsActor final
    : public TActorBootstrapped<TCleanupSessionsActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TString FileSystemId;
    const TRequestInfoPtr RequestInfo;
    const TVector<NProto::TSession> Sessions;

    size_t ResponsesCollected = 0;

public:
    TCleanupSessionsActor(
        TString logTag,
        TActorId tablet,
        TString fileSystemId,
        TRequestInfoPtr requestInfo,
        TVector<NProto::TSession> sessions);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DestroySessions(const TActorContext& ctx);
    void HandleSessionDestroyed(
        const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TCleanupSessionsActor::TCleanupSessionsActor(
        TString logTag,
        TActorId tablet,
        TString fileSystemId,
        TRequestInfoPtr requestInfo,
        TVector<NProto::TSession> sessions)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , FileSystemId(std::move(fileSystemId))
    , RequestInfo(std::move(requestInfo))
    , Sessions(std::move(sessions))
{}

void TCleanupSessionsActor::Bootstrap(const TActorContext& ctx)
{
    DestroySessions(ctx);
    Become(&TThis::StateWork);
}

void TCleanupSessionsActor::DestroySessions(const TActorContext& ctx)
{
    for (size_t i = 0; i < Sessions.size(); ++i) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s[%s] initiating session cleanup",
            LogTag.c_str(),
            Sessions[i].GetSessionId().c_str());

        auto request = std::make_unique<TEvIndexTablet::TEvDestroySessionRequest>();
        request->CallContext = MakeIntrusive<TCallContext>(FileSystemId);

        auto* headers = request->Record.MutableHeaders();
        headers->SetSessionId(Sessions[i].GetSessionId());
        headers->SetClientId(Sessions[i].GetClientId());
        headers->SetSessionSeqNo(Sessions[i].GetMaxSeqNo());

        NCloud::Send(ctx, Tablet, std::move(request), i);
    }
}

void TCleanupSessionsActor::HandleSessionDestroyed(
    const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < Sessions.size());
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s[%s] session cleaned: (error %s)",
        LogTag.c_str(),
        Sessions[ev->Cookie].GetSessionId().c_str(),
        FormatError(msg->GetError()).c_str());

    if (++ResponsesCollected == Sessions.size()) {
        ReplyAndDie(ctx);
    }
}

void TCleanupSessionsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TCleanupSessionsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvCleanupSessionsCompleted;
        auto response = std::make_unique<TCompletion>(error);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        using TResponse = TEvIndexTabletPrivate::TEvCleanupSessionsResponse;
        auto response = std::make_unique<TResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TCleanupSessionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTablet::TEvDestroySessionResponse, HandleSessionDestroyed);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ScheduleCleanupSessions(const TActorContext& ctx)
{
    if (!CleanupSessionsScheduled) {
        ctx.Schedule(
            Config->GetIdleSessionTimeout(),
            new TEvIndexTabletPrivate::TEvCleanupSessionsRequest());
        CleanupSessionsScheduled = true;
    }
}

void TIndexTabletActor::HandleCleanupSessions(
    const TEvIndexTabletPrivate::TEvCleanupSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Sender == ctx.SelfID) {
        CleanupSessionsScheduled = false;
    }

    auto sessions = GetTimeoutedSessions(ctx.Now());
    if (!sessions) {
        // nothing to do
        auto response = std::make_unique<TEvIndexTabletPrivate::TEvCleanupSessionsResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));

        ScheduleCleanupSessions(ctx);
        return;
    }

    Metrics.SessionTimeouts.fetch_add(
        sessions.size(),
        std::memory_order_relaxed);

    TVector<NProto::TSession> list(sessions.size());
    for (size_t i = 0; i < sessions.size(); ++i) {
        list[i].CopyFrom(*sessions[i]);
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto actor = std::make_unique<TCleanupSessionsActor>(
        LogTag,
        ctx.SelfID,
        GetFileSystemId(),
        std::move(requestInfo),
        std::move(list));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleCleanupSessionsCompleted(
    const TEvIndexTabletPrivate::TEvCleanupSessionsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s CleanupSessions completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    WorkerActors.erase(ev->Sender);
    ScheduleCleanupSessions(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
