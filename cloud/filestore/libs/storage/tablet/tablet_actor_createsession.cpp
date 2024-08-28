#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillFeatures(const TStorageConfig& config, NProto::TFileStore& fileStore)
{
    auto* features = fileStore.MutableFeatures();
    features->SetTwoStageReadEnabled(config.GetTwoStageReadEnabled());
    features->SetThreeStageWriteEnabled(config.GetThreeStageWriteEnabled());
    features->SetEntryTimeout(config.GetEntryTimeout().MilliSeconds());
    features->SetNegativeEntryTimeout(
        config.GetNegativeEntryTimeout().MilliSeconds());
    features->SetAttrTimeout(config.GetAttrTimeout().MilliSeconds());
    features->SetThreeStageWriteEnabled(config.GetThreeStageWriteEnabled());
    features->SetThreeStageWriteThreshold(config.GetThreeStageWriteThreshold());

    auto preferredBlockSizeMultiplier =
        config.GetPreferredBlockSizeMultiplier();
    if (preferredBlockSizeMultiplier) {
        features->SetPreferredBlockSize(
            fileStore.GetBlockSize() * preferredBlockSizeMultiplier);
    } else {
        features->SetPreferredBlockSize(fileStore.GetBlockSize());
    }
    features->SetAsyncDestroyHandleEnabled(
        config.GetAsyncDestroyHandleEnabled());
}

////////////////////////////////////////////////////////////////////////////////

TActorId DoRecoverSession(
    TIndexTabletDatabase& db,
    TIndexTabletState& state,
    TSession* session,
    const TString& clientId,
    const TString& sessionId,
    const TString& checkpointId,
    ui64 sessionSeqNo,
    bool readOnly,
    const TActorId& owner,
    const TActorContext& ctx)
{
    auto oldSessionSeqNo = session->GetSessionSeqNo();

    auto oldOwner =
        state.RecoverSession(session, sessionSeqNo, readOnly, owner);
    if (oldOwner) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "[s:%s][n:%lu] kill from tablet %s self %s",
            sessionId.Quote().c_str(),
            sessionSeqNo,
            ToString(oldOwner).c_str(),
            ToString(ctx.SelfID).c_str());

        NCloud::Send(ctx, oldOwner, std::make_unique<TEvents::TEvPoisonPill>());
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "DoRecoverSession c:%s, s:%s, session seqno:%lu new seqno:%lu",
        clientId.c_str(),
        session->GetSessionId().c_str(),
        session->GetSessionSeqNo(),
        sessionSeqNo);

    if (oldSessionSeqNo < session->GetSessionSeqNo()) {
        NProto::TSession proto;
        proto.SetClientId(clientId);
        proto.SetSessionId(sessionId);
        proto.SetCheckpointId(checkpointId);
        proto.SetSessionState(session->GetSessionState());
        proto.SetMaxSeqNo(session->GetSessionSeqNo());
        proto.SetMaxRwSeqNo(session->GetSessionRwSeqNo());

        db.WriteSession(proto);
    }

    return oldOwner;
}

void Convert(
    const NProto::TFileSystem& fileSystem,
    NProto::TFileStore& fileStore)
{
    fileStore.SetFileSystemId(fileSystem.GetFileSystemId());
    fileStore.SetProjectId(fileSystem.GetProjectId());
    fileStore.SetFolderId(fileSystem.GetFolderId());
    fileStore.SetCloudId(fileSystem.GetCloudId());
    fileStore.SetBlockSize(fileSystem.GetBlockSize());
    fileStore.SetBlocksCount(fileSystem.GetBlocksCount());
    // TODO need set ConfigVersion?
    fileStore.SetNodesCount(fileSystem.GetNodesCount());
    fileStore.SetStorageMediaKind(fileSystem.GetStorageMediaKind());
    fileStore.MutableFollowerFileSystemIds()->CopyFrom(
        fileSystem.GetFollowerFileSystemIds());
}

////////////////////////////////////////////////////////////////////////////////

class TCreateFollowerSessionsActor final
    : public TActorBootstrapped<TCreateFollowerSessionsActor>
{
private:
    const TString LogTag;
    const TRequestInfoPtr RequestInfo;
    const NProtoPrivate::TCreateSessionRequest Request;
    const TVector<TString> FollowerIds;
    using TResponse = TEvIndexTablet::TEvCreateSessionResponse;
    std::unique_ptr<TResponse> Response;
    ui32 CreatedSessions = 0;

    NProto::TProfileLogRequestInfo ProfileLogRequest;

public:
    TCreateFollowerSessionsActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TCreateSessionRequest request,
        TVector<TString> followerIds,
        std::unique_ptr<TEvIndexTablet::TEvCreateSessionResponse> response);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);

    void HandleCreateSessionResponse(
        const TEvIndexTablet::TEvCreateSessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TCreateFollowerSessionsActor::TCreateFollowerSessionsActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TCreateSessionRequest request,
        TVector<TString> followerIds,
        std::unique_ptr<TEvIndexTablet::TEvCreateSessionResponse> response)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , FollowerIds(std::move(followerIds))
    , Response(std::move(response))
{}

void TCreateFollowerSessionsActor::Bootstrap(const TActorContext& ctx)
{
    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TCreateFollowerSessionsActor::SendRequests(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& followerId: FollowerIds) {
        auto request =
            std::make_unique<TEvIndexTablet::TEvCreateSessionRequest>();
        request->Record = Request;
        request->Record.SetFileSystemId(followerId);

        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Sending CreateSessionRequest to follower %s",
            LogTag.c_str(),
            followerId.c_str());

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            {}, // flags
            cookie++);
    }
}

void TCreateFollowerSessionsActor::HandleCreateSessionResponse(
    const TEvIndexTablet::TEvCreateSessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower session creation failed for %s with error %s",
            LogTag.c_str(),
            FollowerIds[ev->Cookie].c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Follower session created for %s",
        LogTag.c_str(),
        FollowerIds[ev->Cookie].c_str());

    if (++CreatedSessions == FollowerIds.size()) {
        ReplyAndDie(ctx, {});
    }
}

void TCreateFollowerSessionsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TCreateFollowerSessionsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (RequestInfo) {
        if (HasError(error)) {
            // TODO(#1350): properly rollback session creation in the leader
            // tablet and in the followers
            Response = std::make_unique<TResponse>(error);
        }

        if (Response) {
            NCloud::Reply(ctx, *RequestInfo, std::move(Response));
        } else {
            Y_DEBUG_ABORT_UNLESS(0);
        }
    }

    Die(ctx);
}

STFUNC(TCreateFollowerSessionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvCreateSessionResponse,
            HandleCreateSessionResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCreateSession(
    const TEvIndexTablet::TEvCreateSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s CreateSession: %s",
        LogTag.c_str(),
        DumpMessage(msg->Record).c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvIndexTablet::TCreateSessionMethod>(*requestInfo);

    ExecuteTx<TCreateSession>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CreateSession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateSession& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_CreateSession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateSession& args)
{
    const auto& clientId = GetClientId(args.Request);
    const auto& sessionId = GetSessionId(args.Request);
    const auto& checkpointId = args.Request.GetCheckpointId();
    const auto& originFqdn = GetOriginFqdn(args.Request);
    const auto seqNo = args.Request.GetMountSeqNumber();
    const auto readOnly = args.Request.GetReadOnly();

    const auto owner = args.RequestInfo->Sender;

    TIndexTabletDatabase db(tx.DB);

    // check if client reconnecting with known session id
    auto* session = FindSession(sessionId);
    if (session) {
        if (session->GetClientId() == clientId) {
            args.SessionId = session->GetSessionId();
            auto toKill = DoRecoverSession(
                db,
                *this,
                session,
                clientId,
                args.SessionId,
                checkpointId,
                seqNo,
                readOnly,
                owner,
                ctx);
            if (toKill != owner) {
                LOG_INFO(ctx, TFileStoreComponents::TABLET,
                    "%s CreateSession c:%s, s:%s, seqno:%lu recovered by session",
                    LogTag.c_str(),
                    clientId.c_str(),
                    session->GetSessionId().c_str(),
                    seqNo);
            } else {
                args.Error = MakeError(E_INVALID_STATE, "session seqno is too old");
                LOG_ERROR(ctx, TFileStoreComponents::TABLET,
                    "%s CreateSession c:%s, s:%s, seqno:%lu failed to restore session: %s",
                    LogTag.c_str(),
                    clientId.c_str(),
                    args.SessionId.c_str(),
                    seqNo,
                    FormatError(args.Error).c_str());
            }
        } else {
            args.Error = MakeError(E_INVALID_STATE, "session client id mismatch");
        }
        return;
    }

    // check if there is existing session for the client
    if (args.Request.GetRestoreClientSession()) {
        auto* session = FindSessionByClientId(clientId);
        if (session) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s CreateSession c:%s, s:%s recovered by client",
                LogTag.c_str(),
                clientId.c_str(),
                session->GetSessionId().c_str());

            args.SessionId = session->GetSessionId();

            DoRecoverSession(
                db,
                *this,
                session,
                clientId,
                args.SessionId,
                checkpointId,
                seqNo,
                readOnly,
                owner,
                ctx);

            return;
        }

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession: no session available for client c: %s",
            LogTag.c_str(),
            clientId.c_str());
    }

    if (!sessionId) {
        args.Error = MakeError(E_ARGUMENT, "empty session id");
        return;
    }

    args.SessionId = sessionId;
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s CreateSession c:%s, s:%s, n:%lu creating new session",
        LogTag.c_str(),
        clientId.c_str(),
        args.SessionId.c_str(),
        seqNo);

    CreateSession(
        db,
        clientId,
        args.SessionId,
        checkpointId,
        originFqdn,
        seqNo,
        readOnly,
        owner);
}

void TIndexTabletActor::CompleteTx_CreateSession(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateSession& args)
{
    RemoveTransaction(*args.RequestInfo);

    using TResponse = TEvIndexTablet::TEvCreateSessionResponse;

    if (HasError(args.Error)) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession failed (%s)",
            LogTag.c_str(),
            FormatError(args.Error).c_str());

        auto response = std::make_unique<TResponse>(args.Error);
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    auto* session = FindSession(args.SessionId);
    if (!session) {
        auto message = TStringBuilder() << "Session " << args.SessionId
            << " destroyed during creation";
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s %s",
            LogTag.c_str(),
            message.c_str());

        auto response =
            std::make_unique<TResponse>(MakeError(E_REJECTED, message));
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    auto response = std::make_unique<TResponse>(args.Error);
    response->Record.SetSessionId(std::move(args.SessionId));
    response->Record.SetSessionState(session->GetSessionState());
    auto& fileStore = *response->Record.MutableFileStore();
    Convert(GetFileSystem(), fileStore);
    FillFeatures(*Config, fileStore);

    TVector<TString> followerIds;
    for (const auto& followerId: GetFileSystem().GetFollowerFileSystemIds()) {
        followerIds.push_back(followerId);
    }
    if (followerIds.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession completed (%s)",
            LogTag.c_str(),
            FormatError(args.Error).c_str());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s CreateSession completed - local (%s)",
        LogTag.c_str(),
        FormatError(args.Error).c_str());

    CreateSessionsInFollowers(
        ctx,
        std::move(args.RequestInfo),
        std::move(args.Request),
        std::move(response),
        std::move(followerIds));
}

void TIndexTabletActor::HandleSyncFollowerSessions(
    const TEvIndexTabletPrivate::TEvSyncFollowerSessionsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    THashSet<TString> filter;
    for (auto& s: *ev->Get()->Sessions.MutableSessions()) {
        filter.insert(*s.MutableSessionId());
    }
    TEvIndexTabletPrivate::TFollowerSessionsInfo info;
    info.FollowerId = std::move(ev->Get()->FollowerId);
    for (auto& request: BuildCreateSessionRequests(filter)) {
        CreateSessionsInFollowers(
            ctx,
            nullptr, // requestInfo
            std::move(request),
            nullptr, // response
            {info.FollowerId});

        ++info.SessionCount;
    }

    using TResponse = TEvIndexTabletPrivate::TEvSyncFollowerSessionsResponse;
    auto response = std::make_unique<TResponse>();
    response->Info = std::move(info);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::CreateSessionsInFollowers(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProtoPrivate::TCreateSessionRequest request,
    std::unique_ptr<TEvIndexTablet::TEvCreateSessionResponse> response,
    TVector<TString> followerIds)
{
    TString logTag = TStringBuilder() << LogTag
        << " s=" << request.GetHeaders().GetSessionId()
        << " c=" << request.GetHeaders().GetClientId();

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Creating follower sessions (%s)",
        logTag.c_str(),
        JoinSeq(",", followerIds).c_str());

    auto actor = std::make_unique<TCreateFollowerSessionsActor>(
        std::move(logTag),
        std::move(requestInfo),
        std::move(request),
        std::move(followerIds),
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));

    Y_UNUSED(actorId);
    // TODO(#1350): register actorId in WorkerActors, erase upon completion
}

}   // namespace NCloud::NFileStore::NStorage
