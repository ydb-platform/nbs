#include "service_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <contrib/ydb/core/base/tablet_pipe.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/system/hostname.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::pair<ui64, ui64> GetSeqNo(const NProto::TGetSessionEventsResponse& response)
{
    size_t numEvents = response.EventsSize();
    Y_ABORT_UNLESS(numEvents);

    return {
        response.GetEvents(0).GetSeqNo(),
        response.GetEvents(numEvents - 1).GetSeqNo(),
    };
}

////////////////////////////////////////////////////////////////////////////////

class TCreateSessionActor final
    : public TActorBootstrapped<TCreateSessionActor>
{
private:
    const TStorageConfigPtr Config;
    TRequestInfoPtr RequestInfo;

    TString ClientId;
    TString FileSystemId;
    TString CheckpointId;
    TString OriginFqdn;
    bool RestoreClientSession;
    ui64 SeqNo;
    bool ReadOnly;
    TString SessionId;
    TString SessionState;
    NProto::TFileStore FileStore;

    ui64 TabletId = -1;
    TActorId PipeClient;

    TInstant LastPipeResetTime;
    TInstant LastPing;

    static const int MaxStoredEvents = 1000;
    google::protobuf::RepeatedPtrField<NProto::TSessionEvent> StoredEvents;
    TActorId EventListener;

    bool FirstWakeupScheduled = false;

    TActorId Owner;

public:
    TCreateSessionActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        TString clientId,
        TString fileSystemId,
        TString sessionId,
        TString checkpointId,
        TString originFqdn,
        ui64 seqNo,
        bool readOnly,
        bool restoreClientSession,
        TActorId owner);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateResolve);
    STFUNC(StateWork);
    STFUNC(StateShutdown);

    void DescribeFileStore(const TActorContext& ctx);
    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void CreateSession(const TActorContext& ctx);
    void HandleCreateSessionResponse(
        const TEvIndexTablet::TEvCreateSessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void CreatePipe(const TActorContext& ctx);

    void HandleConnect(
        TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx);

    void HandleDisconnect(
        TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateSession(
        const TEvServicePrivate::TEvCreateSession::TPtr& ev,
        const TActorContext& ctx);

    void RejectCreateSession(
        const TEvServicePrivate::TEvCreateSession::TPtr& ev,
        const TActorContext& ctx);

    void HandlePingSession(
        const TEvServicePrivate::TEvPingSession::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetSessionEvents(
        const TEvService::TEvGetSessionEventsRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetSessionEventsResponse(
        const TEvService::TEvGetSessionEventsResponse::TPtr& ev,
        const TActorContext& ctx);

    void ScheduleWakeup(const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void OnDisconnect(const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void Notify(
        const TActorContext& ctx,
        const NProto::TError& error,
        bool shutdown);

    void CancelPendingRequests(
        const TActorContext& ctx,
        const NProto::TError& error);

    void Die(const TActorContext& ctx) override;

    TString LogTag()
    {
        return Sprintf("[f:%s][c:%s][s:%s][a:%s]",
            FileSystemId.Quote().c_str(),
            ClientId.Quote().c_str(),
            SessionId.Quote().c_str(),
            ToString(SelfId()).c_str());
    }
};

////////////////////////////////////////////////////////////////////////////////

TCreateSessionActor::TCreateSessionActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        TString clientId,
        TString fileSystemId,
        TString sessionId,
        TString checkpointId,
        TString originFqdn,
        ui64 seqNo,
        bool readOnly,
        bool restoreClientSession,
        TActorId owner)
    : Config(std::move(config))
    , RequestInfo(std::move(requestInfo))
    , ClientId(std::move(clientId))
    , FileSystemId(std::move(fileSystemId))
    , CheckpointId(std::move(checkpointId))
    , OriginFqdn(std::move(originFqdn))
    , RestoreClientSession(restoreClientSession)
    , SeqNo(seqNo)
    , ReadOnly(readOnly)
    , SessionId(std::move(sessionId))
    , Owner(owner)
{}

void TCreateSessionActor::Bootstrap(const TActorContext& ctx)
{
    LastPing = ctx.Now();
    DescribeFileStore(ctx);
    Become(&TThis::StateResolve);
}

void TCreateSessionActor::DescribeFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        FileSystemId);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TCreateSessionActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        if (GetErrorKind(msg->GetError()) != EErrorKind::ErrorRetriable) {
            LOG_ERROR(
                ctx,
                TFileStoreComponents::SERVICE_WORKER,
                "%s Failed to describe filestore %s Error: %s",
                LogTag().c_str(),
                FileSystemId.c_str(),
                FormatError(msg->GetError()).c_str());
            ReportDescribeFileStoreError();
        }

        Notify(ctx, msg->GetError(), false);
        Die(ctx);

        return;
    }

    const auto& pathDescr = msg->PathDescription;
    const auto& fsDescr = pathDescr.GetFileStoreDescription();

    TabletId = fsDescr.GetIndexTabletId();

    CreatePipe(ctx);

    Become(&TThis::StateWork);
}

void TCreateSessionActor::CreatePipe(const TActorContext& ctx)
{
    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s creating pipe for tablet %lu, last reset at %lu",
        LogTag().c_str(),
        TabletId,
        LastPipeResetTime.Seconds());

    if (!LastPipeResetTime) {
        LastPipeResetTime = ctx.Now();
    }

    // TODO
    NTabletPipe::TClientConfig clientConfig;
    PipeClient = ctx.Register(CreateClient(SelfId(), TabletId, clientConfig));
}

void TCreateSessionActor::HandleConnect(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        LOG_ERROR(ctx, TFileStoreComponents::SERVICE_WORKER,
            "%s failed to connect to %lu: %s",
            LogTag().c_str(),
            TabletId,
            NKikimrProto::EReplyStatus_Name(msg->Status).data());

        OnDisconnect(ctx);

        return;
    }

    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s pipe connected to %lu",
        LogTag().c_str(),
        TabletId);

    LastPipeResetTime = {};
    LastPing = ctx.Now();

    CreateSession(ctx);
}

void TCreateSessionActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr&,
    const TActorContext& ctx)
{
    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s pipe disconnected",
        LogTag().c_str(),
        TabletId);

    OnDisconnect(ctx);
}

void TCreateSessionActor::OnDisconnect(const TActorContext& ctx)
{
    NTabletPipe::CloseClient(ctx, PipeClient);
    PipeClient = {};

    if (!FirstWakeupScheduled) {
        // Wakeup cycle is inactive => reconnect won't be initiated
        // if we don't initiate it here
        CreatePipe(ctx);
    }
}

void TCreateSessionActor::HandleCreateSession(
    const TEvServicePrivate::TEvCreateSession::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s got create session: seqno %lu ro %u",
        LogTag().c_str(),
        msg->SessionSeqNo,
        msg->ReadOnly);

    LastPing = ctx.Now();

    ClientId = msg->ClientId;
    FileSystemId = msg->FileSystemId;
    SessionId = msg->SessionId;
    CheckpointId = msg->CheckpointId;
    ReadOnly = msg->ReadOnly;
    SeqNo = msg->SessionSeqNo;
    RestoreClientSession = msg->RestoreClientSession;
    RequestInfo = std::move(msg->RequestInfo);

    CreateSession(ctx);
}

void TCreateSessionActor::RejectCreateSession(
    const TEvServicePrivate::TEvCreateSession::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto error = MakeError(E_REJECTED, "TCreateSessionActor: shutting down");

    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s reject create session: seqno %lu error %s",
        LogTag().c_str(),
        msg->SessionSeqNo,
        FormatError(error).c_str());

    auto response = std::make_unique<TEvServicePrivate::TEvSessionCreated>(error);
    response->ClientId = msg->ClientId;
    response->SessionId = msg->SessionId;
    response->SessionSeqNo = msg->SessionSeqNo;
    response->ReadOnly = msg->ReadOnly;
    response->RequestInfo = std::move(msg->RequestInfo);

    NCloud::Send(ctx, MakeStorageServiceId(), std::move(response));
}

void TCreateSessionActor::CreateSession(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTablet::TEvCreateSessionRequest>();
    request->Record.SetFileSystemId(FileSystemId);
    request->Record.SetCheckpointId(CheckpointId);
    request->Record.SetRestoreClientSession(RestoreClientSession);
    request->Record.SetMountSeqNumber(SeqNo);
    request->Record.SetReadOnly(ReadOnly);

    auto* headers = request->Record.MutableHeaders();
    headers->SetClientId(ClientId);
    headers->SetSessionId(SessionId);
    headers->SetSessionSeqNo(SeqNo);
    headers->SetOriginFqdn(OriginFqdn);

    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s do creating session: %s",
        LogTag().c_str(),
        DumpMessage(request->Record).c_str());

    NTabletPipe::SendData(ctx, PipeClient, request.release());
}

void TCreateSessionActor::HandleCreateSessionResponse(
    const TEvIndexTablet::TEvCreateSessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& sessionId = msg->Record.GetSessionId();
    if (FAILED(msg->GetStatus())) {
        if (GetErrorKind(msg->GetError()) != EErrorKind::ErrorRetriable) {
            ReportCreateSessionError();
        }

        return Notify(ctx, msg->GetError(), false);
    }

    if (!sessionId) {
        ReportMissingSessionId();

        return Notify(
            ctx,
            MakeError(E_FAIL, "empty session id"),
            false);
    }

    if (sessionId != SessionId) {
        LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
            "%s restored session id: actual id %s, state(%lu)",
            LogTag().c_str(),
            sessionId.Quote().c_str(),
            msg->Record.GetSessionState().size());

        SessionId = sessionId;
    } else {
        LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
            "%s session established: %s, state(%lu)",
            LogTag().c_str(),
            FormatError(msg->GetError()).c_str(),
            msg->Record.GetSessionState().size());
    }

    SessionState = msg->Record.GetSessionState();
    FileStore = msg->Record.GetFileStore();

    // Some of the features of the filestore are set not by the tablet but also
    // by the client-side config
    FileStore.MutableFeatures()->SetKeepCacheAllowed(
        Config->GetKeepCacheAllowed());

    Notify(ctx, {}, false);

    if (!FirstWakeupScheduled) {
        ScheduleWakeup(ctx);
        FirstWakeupScheduled = true;
    }
}

void TCreateSessionActor::HandlePingSession(
    const TEvServicePrivate::TEvPingSession::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LastPing = ctx.Now();
}

void TCreateSessionActor::HandleGetSessionEvents(
    const TEvService::TEvGetSessionEventsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(SessionId);

    if (ev->Cookie == TEvService::StreamCookie) {
        LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
            "%s subscribe event listener (%s)",
            LogTag().c_str(),
            ToString(EventListener).c_str());

        EventListener = ev->Sender;
    }

    auto response = std::make_unique<TEvService::TEvGetSessionEventsResponse>();
    response->Record.MutableEvents()->Swap(&StoredEvents);

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TCreateSessionActor::HandleGetSessionEventsResponse(
    const TEvService::TEvGetSessionEventsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(SessionId);

    const auto* msg = ev->Get();

    const auto [firstSeqNo, lastSeqNo] = GetSeqNo(msg->Record);
    LOG_ERROR(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s got session events (seqNo: %lu-%lu)",
        LogTag().c_str(),
        firstSeqNo,
        lastSeqNo);

    if (EventListener) {
        Y_ABORT_UNLESS(StoredEvents.size() == 0);

        // just forward response to listener as-is
        ctx.Send(ev->Forward(EventListener));
        return;
    }

    for (const auto& event: msg->Record.GetEvents()) {
        if (StoredEvents.size() < MaxStoredEvents) {
            StoredEvents.Add()->CopyFrom(event);
        } else {
            // number of stored events exceeded
            LOG_WARN(ctx, TFileStoreComponents::SERVICE_WORKER,
                "%s not enough space - session events will be lost (seqNo: %lu-%lu)",
                LogTag().c_str(),
                event.GetSeqNo(),
                lastSeqNo);
            break;
        }
    }
}

void TCreateSessionActor::ScheduleWakeup(const TActorContext& ctx)
{
    ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
}

void TCreateSessionActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto idleTimeout = Config->GetIdleSessionTimeout();
    if (LastPing + idleTimeout < ctx.Now()) {
        LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
            "%s closing idle session, last ping at %s",
            LogTag().c_str(),
            LastPing.ToStringUpToSeconds().c_str());
        Become(&TThis::StateShutdown);
        return Notify(ctx, MakeError(E_TIMEOUT, "closed idle session"), true);
    }

    auto connectTimeout = Config->GetEstablishSessionTimeout();
    if (LastPipeResetTime && LastPipeResetTime + connectTimeout < ctx.Now()) {
        LOG_WARN(ctx, TFileStoreComponents::SERVICE_WORKER,
            "%s create timeouted, couldn't connect from %s",
            LogTag().c_str(),
            LastPipeResetTime.ToStringUpToSeconds().c_str());
        Become(&TThis::StateShutdown);
        return Notify(ctx, MakeError(E_TIMEOUT, "failed to connect to fs"), true);
    }

    if (!PipeClient) {
        CreatePipe(ctx);
    }

    ScheduleWakeup(ctx);
}

void TCreateSessionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s session poisoned, dying %s -> %s",
        LogTag().c_str(),
        ToString(ev->Sender).c_str(),
        ToString(SelfId()).c_str());

    if (ev->Sender != Owner) {
        Become(&TThis::StateShutdown);
        Notify(ctx, MakeError(E_REJECTED, "request cancelled"), true);
    } else {
        Die(ctx);
    }
}

void TCreateSessionActor::Notify(
    const TActorContext& ctx,
    const NProto::TError& error,
    bool shutdown)
{
    Y_ABORT_UNLESS(SessionId);
    LOG_INFO(ctx, TFileStoreComponents::SERVICE_WORKER,
        "%s session notify %lu (%s)",
        LogTag().c_str(),
        SeqNo,
        FormatError(error).c_str());

    auto response = std::make_unique<TEvServicePrivate::TEvSessionCreated>(error);
    response->ClientId = ClientId;
    response->SessionId = SessionId;
    response->SessionState = SessionState;
    response->SessionSeqNo = SeqNo;
    response->ReadOnly = ReadOnly;
    response->TabletId = TabletId;
    response->FileStore = FileStore;
    response->RequestInfo = std::move(RequestInfo);
    response->Shutdown = shutdown;

    NCloud::Send(ctx, MakeStorageServiceId(), std::move(response));
}

void TCreateSessionActor::Die(const TActorContext& ctx)
{
    if (PipeClient) {
        NTabletPipe::CloseClient(ctx, PipeClient);
        PipeClient = {};
    }

    TActor::Die(ctx);
}

STFUNC(TCreateSessionActor::StateResolve)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvServicePrivate::TEvPingSession, HandlePingSession);
        HFunc(TEvSSProxy::TEvDescribeFileStoreResponse, HandleDescribeFileStoreResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

STFUNC(TCreateSessionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);

        HFunc(TEvServicePrivate::TEvCreateSession, HandleCreateSession);
        HFunc(TEvIndexTablet::TEvCreateSessionResponse, HandleCreateSessionResponse);
        HFunc(TEvServicePrivate::TEvPingSession, HandlePingSession);
        HFunc(TEvService::TEvGetSessionEventsRequest, HandleGetSessionEvents);
        HFunc(TEvService::TEvGetSessionEventsResponse, HandleGetSessionEventsResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

STFUNC(TCreateSessionActor::StateShutdown)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvServicePrivate::TEvCreateSession, RejectCreateSession);

        IgnoreFunc(TEvents::TEvWakeup);
        IgnoreFunc(TEvTabletPipe::TEvClientConnected);
        IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);
        IgnoreFunc(TEvIndexTablet::TEvCreateSessionResponse);
        IgnoreFunc(TEvServicePrivate::TEvPingSession);
        IgnoreFunc(TEvService::TEvGetSessionEventsRequest);
        IgnoreFunc(TEvService::TEvGetSessionEventsResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleCreateSession(
    const TEvService::TEvCreateSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    TString fileSystemId = msg->Record.GetFileSystemId();
    if (const auto* realId =
            StorageConfig->FindFileSystemIdByAlias(fileSystemId))
    {
        fileSystemId = *realId;
    }
    const auto& checkpointId = msg->Record.GetCheckpointId();
    auto originFqdn = GetOriginFqdn(msg->Record);

    ui64 cookie;
    TInFlightRequest* inflight;
    std::tie(cookie, inflight) = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto sessionId = GetSessionId(msg->Record);
    auto seqNo = GetSessionSeqNo(msg->Record);

    auto reply = [&] (auto* session, auto error) {
        auto response =
            std::make_unique<TEvService::TEvCreateSessionResponse>(error);
        session->GetInfo(
            *response->Record.MutableSession(),
            GetSessionSeqNo(msg->Record));
        inflight->Complete(ctx.Now(), std::move(error));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    auto proceed = [&] (auto actorId) {
        LOG_INFO(ctx, TFileStoreComponents::SERVICE,
            "%s forward create session to actor %s",
            LogTag(fileSystemId, clientId, sessionId, seqNo).c_str(),
            ToString(actorId).c_str());

        auto requestInfo = CreateRequestInfo(
            SelfId(),
            cookie,
            msg->CallContext);

        auto request = std::make_unique<TEvServicePrivate::TEvCreateSession>();
        request->ClientId = clientId;
        request->FileSystemId = fileSystemId;
        request->SessionId = sessionId;
        request->CheckpointId = checkpointId;
        request->ReadOnly =  msg->Record.GetReadOnly();
        request->RestoreClientSession = msg->Record.GetRestoreClientSession();
        request->SessionSeqNo =  msg->Record.GetMountSeqNumber();
        request->RequestInfo = std::move(requestInfo);

        NCloud::Send(ctx, std::move(actorId), std::move(request));
    };

    auto* session =
        State->FindSession(sessionId, GetSessionSeqNo(msg->Record));

    if (session) {
        if (session->ClientId != clientId) {
            auto error = MakeError(
                E_FS_INVALID_SESSION,
                TStringBuilder() <<
                    "Wrong session: " << sessionId <<
                    " for client: " << clientId);
            reply(session, std::move(error));
            return;
        }

        if (session->CreateDestroyState
                != ESessionCreateDestroyState::STATE_NONE)
        {
            auto error = MakeError(
                E_REJECTED,
                "Another create or destroy request is in progress");
            reply(session, std::move(error));
            return;
        }

        session->CreateDestroyState =
            ESessionCreateDestroyState::STATE_CREATE_SESSION;

        if (session->SessionActor) {
            return proceed(session->SessionActor);
        }
    }

    if (!sessionId) {
        sessionId = CreateGuidAsString();
    }

    if (!originFqdn) {
        originFqdn = GetFQDNHostName();
    }

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TCreateSessionActor>(
        StorageConfig,
        std::move(requestInfo),
        clientId,
        fileSystemId,
        sessionId,
        checkpointId,
        std::move(originFqdn),
        msg->Record.GetMountSeqNumber(),
        msg->Record.GetReadOnly(),
        msg->Record.GetRestoreClientSession(),
        SelfId());

    auto actorId = NCloud::Register(ctx, std::move(actor));

    LOG_INFO(ctx, TFileStoreComponents::SERVICE,
        "%s create session %s self %s",
        LogTag(fileSystemId, clientId, sessionId, seqNo).c_str(),
        ToString(actorId).c_str(),
        ToString(SelfId()).c_str());
}

bool TStorageServiceActor::RemoveSession(
    const TString& sessionId,
    ui64 seqNo,
    const TActorContext& ctx)
{
    if (auto* session = State->FindSession(sessionId, seqNo); session) {
        if (State->IsLastSubSession(sessionId, seqNo)) {
            LOG_INFO(ctx, TFileStoreComponents::SERVICE,
                "[s:%s][n:%lu] remove subsession %s self %s",
                sessionId.Quote().c_str(),
                seqNo,
                ToString(session->SessionActor).c_str(),
                ToString(SelfId()).c_str());
        }
        return State->RemoveSession(sessionId, seqNo);
    }
    return false;
}

void TStorageServiceActor::RemoveSession(
    const TString& sessionId,
    const TActorContext& ctx)
{
    if (auto* session = State->FindSession(sessionId); session) {
        LOG_INFO(ctx, TFileStoreComponents::SERVICE,
            "[s:%s]remove session %s self %s",
            sessionId.Quote().c_str(),
            ToString(session->SessionActor).c_str(),
            ToString(SelfId()).c_str());

        State->RemoveSession(sessionId);
    }
}

void TStorageServiceActor::HandleSessionCreated(
    const TEvServicePrivate::TEvSessionCreated::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto* session = State->FindSession(msg->SessionId);
    if (SUCCEEDED(msg->GetStatus())) {
        // in case of vhost restart we don't know session id
        // so inevitably will create new actor
        auto actorId = ev->Sender;
        if (session &&
            session->SessionActor &&
            session->SessionActor != ev->Sender)
        {
            ctx.Send(ev->Sender, new TEvents::TEvPoisonPill());
            actorId = session->SessionActor;
        }

        if (!session) {
            LOG_INFO(ctx, TFileStoreComponents::SERVICE,
                "%s session created (%s), state(%lu)",
                LogTag(
                    msg->FileStore.GetFileSystemId(),
                    msg->ClientId,
                    msg->SessionId,
                    msg->SessionSeqNo).c_str(),
                FormatError(msg->GetError()).c_str(),
                msg->SessionState.size());

            auto stats = StatsRegistry->GetFileSystemStats(
                msg->FileStore.GetFileSystemId(),
                msg->ClientId);

            const auto mediaKind = static_cast<NProto::EStorageMediaKind>(
                msg->FileStore.GetStorageMediaKind());

            session = State->CreateSession(
                msg->ClientId,
                msg->FileStore,
                msg->SessionId,
                msg->SessionState,
                msg->SessionSeqNo,
                msg->ReadOnly,
                mediaKind,
                std::move(stats),
                actorId,
                msg->TabletId);

            Y_ABORT_UNLESS(session);
        } else {
            session->UpdateSessionState(
                msg->SessionState,
                msg->FileStore);
            session->AddSubSession(msg->SessionSeqNo, msg->ReadOnly);
            session->SessionActor = actorId;
        }
    } else if (session) {
        // else it's an old notify from a dead actor
        session->CreateDestroyState = ESessionCreateDestroyState::STATE_NONE;
        if (session->SessionActor == ev->Sender) {
            // e.g. pipe failed or smth. client will have to restore it
            LOG_WARN(ctx, TFileStoreComponents::SERVICE,
                "%s session failed (%s)",
                LogTag(
                    msg->FileStore.GetFileSystemId(),
                    msg->ClientId,
                    msg->SessionId,
                    msg->SessionSeqNo).c_str(),
                FormatError(msg->GetError()).c_str());

            if (msg->Shutdown) {
                RemoveSession(msg->SessionId, ctx);
                ctx.Send(ev->Sender, new TEvents::TEvPoisonPill());
                session = nullptr;
            } else if (!RemoveSession(msg->SessionId, msg->SessionSeqNo, ctx)) {
                ctx.Send(ev->Sender, new TEvents::TEvPoisonPill());
                session = nullptr;
            }
        }
    } else {
        LOG_ERROR(ctx, TFileStoreComponents::SERVICE,
            "%s session creation failed (%s)",
            LogTag(
                msg->FileStore.GetFileSystemId(),
                msg->ClientId,
                msg->SessionId,
                msg->SessionSeqNo).c_str(),
            FormatError(msg->GetError()).c_str());

        if (msg->Shutdown) {
            ctx.Send(ev->Sender, new TEvents::TEvPoisonPill());
        }
    }

    if (session) {
        session->CreateDestroyState = ESessionCreateDestroyState::STATE_NONE;
    }

    if (msg->RequestInfo) {
        auto* inflight = FindInFlightRequest(msg->RequestInfo->Cookie);
        if (!inflight) {
            LOG_CRIT(ctx, TFileStoreComponents::SERVICE,
                "%s failed complete CreateSession: invalid cookie (%lu)",
                LogTag(
                    msg->FileStore.GetFileSystemId(),
                    msg->ClientId,
                    msg->SessionId,
                    msg->SessionSeqNo).c_str(),
                msg->RequestInfo->Cookie);
            return;
        }

        auto response = std::make_unique<TEvService::TEvCreateSessionResponse>(
            msg->GetError());
        if (session) {
            session->GetInfo(
                *response->Record.MutableSession(),
                msg->SessionSeqNo);
            response->Record.MutableFileStore()->CopyFrom(msg->FileStore);
        }

        NCloud::Reply(ctx, *inflight, std::move(response));
        inflight->Complete(ctx.Now(), msg->GetError());
    }
}

}   // namespace NCloud::NFileStore::NStorage
