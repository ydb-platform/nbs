#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroySessionActor final
    : public TActorBootstrapped<TDestroySessionActor>
{
private:
    const TStorageConfigPtr Config;
    const TRequestInfoPtr RequestInfo;
    const TString ClientId;
    const TString FileSystemId;
    const TString SessionId;
    const ui64 SeqNo;
    const TInstant InactivityDeadline;

public:
    TDestroySessionActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        TString clientId,
        TString fileSystemId,
        TString sessionId,
        ui64 seqNo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DestroySession(const TActorContext& ctx);
    void HandleDestroySessionResponse(
        const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeUp(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);

    void Notify(
        const TActorContext& ctx,
        const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TDestroySessionActor::TDestroySessionActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        TString clientId,
        TString fileSystemId,
        TString sessionId,
        ui64 seqNo)
    : Config(std::move(config))
    , RequestInfo(std::move(requestInfo))
    , ClientId(std::move(clientId))
    , FileSystemId(std::move(fileSystemId))
    , SessionId(std::move(sessionId))
    , SeqNo(seqNo)
    , InactivityDeadline(Config->GetIdleSessionTimeout().ToDeadLine())
{}

void TDestroySessionActor::Bootstrap(const TActorContext& ctx)
{
    DestroySession(ctx);
    Become(&TThis::StateWork);
}

void TDestroySessionActor::DestroySession(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTablet::TEvDestroySessionRequest>();
    request->Record.SetFileSystemId(FileSystemId);

    auto* headers = request->Record.MutableHeaders();
    headers->SetClientId(ClientId);
    headers->SetSessionId(SessionId);
    headers->SetSessionSeqNo(SeqNo);

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TDestroySessionActor::HandleDestroySessionResponse(
    const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE_WORKER,
        "[%s] client removed (%s)",
        SessionId.Quote().c_str(),
        FormatError(msg->GetError()).c_str());

    if (msg->GetStatus() == E_REJECTED) {
        // Pipe error
        if (ctx.Now() < InactivityDeadline) {
            return ctx.Schedule(
                TDuration::Seconds(1),
                new TEvents::TEvWakeup());
        }
    }

    return ReplyAndDie(ctx, msg->GetError());
}

void TDestroySessionActor::HandleWakeUp(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    DestroySession(ctx);
}

void TDestroySessionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE_WORKER,
        "[%s] destroy poisoned, dying",
        SessionId.Quote().c_str());

    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TDestroySessionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    Notify(ctx, error);
    Die(ctx);
}

void TDestroySessionActor::Notify(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "[%s] session stop completed (%s)",
        SessionId.Quote().c_str(),
        FormatError(error).c_str());

    auto response =
        std::make_unique<TEvServicePrivate::TEvSessionDestroyed>(error);
    response->SessionId = SessionId;
    response->SeqNo = SeqNo;

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

STFUNC(TDestroySessionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeUp);

        HFunc(
            TEvIndexTablet::TEvDestroySessionResponse,
            HandleDestroySessionResponse);
        IgnoreFunc(TEvServicePrivate::TEvPingSession);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleDestroySession(
    const TEvService::TEvDestroySessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& fsId = GetFileSystemId(msg->Record);
    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto reply = [&, inflight = std::move(inflight)] (auto error) {
        auto response =
            std::make_unique<TEvService::TEvDestroySessionResponse>(error);
        inflight->Complete(ctx.Now(), std::move(error));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session) {
        // session still needs to be destroyed in index tablet
        LOG_INFO(ctx, TFileStoreComponents::SERVICE,
            "%s DestroySession - tablet only",
            LogTag(fsId, clientId, sessionId, seqNo).c_str());

        auto requestInfo = CreateRequestInfo(
            SelfId(),
            cookie,
            msg->CallContext);

        auto actor = std::make_unique<TDestroySessionActor>(
            StorageConfig,
            std::move(requestInfo),
            clientId,
            fsId,
            sessionId,
            seqNo);

        NCloud::Register(ctx, std::move(actor));

        return;
    }

    if (session->CreateDestroyState != ESessionCreateDestroyState::STATE_NONE) {
        return reply(MakeError(
            E_REJECTED,
            "Another create or destroy request is in progress"));
    }

    if (session->ClientId != clientId
            || session->FileStore.GetFileSystemId() != fsId)
    {
        return reply(ErrorInvalidSession(clientId, sessionId, seqNo));
    }

    if (session->ShouldStop) {
        return reply(
            MakeError(E_REJECTED, "session destruction is in progress"));
    }

    LOG_INFO(ctx, TFileStoreComponents::SERVICE,
        "%s DestroySession",
        LogTag(fsId, clientId, sessionId, seqNo).c_str());

    if (State->IsLastSubSession(sessionId, seqNo)) {
        LOG_INFO(ctx, TFileStoreComponents::SERVICE,
            "%s Kill session actor %s from %s",
            LogTag(fsId, clientId, sessionId, seqNo).c_str(),
            ToString(session->SessionActor).c_str(),
            ToString(SelfId()).c_str());

        NCloud::Send(
            ctx,
            session->SessionActor,
            std::make_unique<TEvents::TEvPoisonPill>());

        session->SessionActor = {};
        session->ShouldStop = true;
    }

    session->CreateDestroyState =
        ESessionCreateDestroyState::STATE_DESTROY_SESSION;

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TDestroySessionActor>(
        StorageConfig,
        std::move(requestInfo),
        session->ClientId,
        session->FileStore.GetFileSystemId(),
        session->SessionId,
        seqNo);

    NCloud::Register(ctx, std::move(actor));
}

void TStorageServiceActor::HandleSessionDestroyed(
    const TEvServicePrivate::TEvSessionDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto* session = State->FindSession(msg->SessionId, msg->SeqNo);
    if (session) {
        session->CreateDestroyState = ESessionCreateDestroyState::STATE_NONE;
        const auto fsId = session->FileStore.GetFileSystemId();
        const auto clientId = session->ClientId;
        if (!State->RemoveSession(msg->SessionId, msg->SeqNo)) {
            StatsRegistry->Unregister(fsId, clientId);
        }
    } else {
        // shutdown or stop before start
    }

    auto* inflight = FindInFlightRequest(ev->Cookie);
    if (!inflight) {
        LOG_CRIT(ctx, TFileStoreComponents::SERVICE,
            "[%s] failed to complete DestroySession: invalid cookie (%lu)",
            msg->SessionId.Quote().c_str(),
            ev->Cookie);
        return;
    }

    LOG_INFO(ctx, TFileStoreComponents::SERVICE,
        "[%s] DestroySession completed (%s)",
        msg->SessionId.Quote().c_str(),
        FormatError(msg->GetError()).c_str());

    auto response = std::make_unique<TEvService::TEvDestroySessionResponse>(
        msg->GetError());
    NCloud::Reply(ctx, *inflight, std::move(response));

    inflight->Complete(ctx.Now(), msg->GetError());
}

}   // namespace NCloud::NFileStore::NStorage
