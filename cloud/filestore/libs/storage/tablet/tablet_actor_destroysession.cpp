#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyFollowerSessionsActor final
    : public TActorBootstrapped<TDestroyFollowerSessionsActor>
{
private:
    const TString LogTag;
    const TRequestInfoPtr RequestInfo;
    const NProtoPrivate::TDestroySessionRequest Request;
    const google::protobuf::RepeatedPtrField<TString> FollowerIds;
    std::unique_ptr<TEvIndexTablet::TEvDestroySessionResponse> Response;
    int DestroyedSessions = 0;

    NProto::TProfileLogRequestInfo ProfileLogRequest;

public:
    TDestroyFollowerSessionsActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TDestroySessionRequest request,
        google::protobuf::RepeatedPtrField<TString> followerIds,
        std::unique_ptr<TEvIndexTablet::TEvDestroySessionResponse> response);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);

    void HandleDestroySessionResponse(
        const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TDestroyFollowerSessionsActor::TDestroyFollowerSessionsActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        NProtoPrivate::TDestroySessionRequest request,
        google::protobuf::RepeatedPtrField<TString> followerIds,
        std::unique_ptr<TEvIndexTablet::TEvDestroySessionResponse> response)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , FollowerIds(std::move(followerIds))
    , Response(std::move(response))
{}

void TDestroyFollowerSessionsActor::Bootstrap(const TActorContext& ctx)
{
    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TDestroyFollowerSessionsActor::SendRequests(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& followerId: FollowerIds) {
        auto request =
            std::make_unique<TEvIndexTablet::TEvDestroySessionRequest>();
        request->Record = Request;
        request->Record.SetFileSystemId(followerId);

        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Sending DestroySessionRequest to follower %s",
            LogTag.c_str(),
            followerId.c_str());

        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            {}, // flags
            cookie++);
    }
}

void TDestroyFollowerSessionsActor::HandleDestroySessionResponse(
    const TEvIndexTablet::TEvDestroySessionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower session destruction  failed for %s with error %s",
            LogTag.c_str(),
            FollowerIds[ev->Cookie].c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Follower session destroyed for %s",
        LogTag.c_str(),
        FollowerIds[ev->Cookie].c_str());

    if (++DestroyedSessions == FollowerIds.size()) {
        ReplyAndDie(ctx, {});
    }
}

void TDestroyFollowerSessionsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TDestroyFollowerSessionsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        Response =
            std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>(error);
    }
    NCloud::Reply(ctx, *RequestInfo, std::move(Response));

    Die(ctx);
}

STFUNC(TDestroyFollowerSessionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDestroySessionResponse,
            HandleDestroySessionResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDestroySession(
    const TEvIndexTablet::TEvDestroySessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>(
                std::move(error)));

        return;
    }

    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const auto sessionSeqNo = GetSessionSeqNo(msg->Record);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s DestroySession c:%s, s:%s n:%lu",
        LogTag.c_str(),
        clientId.c_str(),
        sessionId.c_str(),
        sessionSeqNo);

    auto* session = FindSession(sessionId);
    if (!session) {
        auto response = std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>();

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (session->GetClientId() != clientId) {
        auto response = std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>(
            ErrorInvalidSession(clientId, sessionId, sessionSeqNo));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvIndexTablet::TDestroySessionMethod>(*requestInfo);

    ExecuteTx<TDestroySession>(
        ctx,
        std::move(requestInfo),
        sessionId,
        sessionSeqNo,
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DestroySession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroySession& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        return true;
    }

    if (!CheckSessionForDestroy(session, args.SessionSeqNo)) {
        return true;
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Wipe session s:%s n:%lu",
        LogTag.c_str(),
        args.SessionId.c_str(),
        args.SessionSeqNo);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    bool ready = true;
    auto commitId = GetCurrentCommitId();
    args.Nodes.reserve(session->Handles.Size());
    for (const auto& handle: session->Handles) {
        if (args.Nodes.contains(handle.GetNodeId())) {
            continue;
        }

        TMaybe<IIndexTabletDatabase::TNode> node;
        if (!ReadNode(db, handle.GetNodeId(), commitId, node)) {
            ready = false;
        } else {
            TABLET_VERIFY(node);
            if (node->Attrs.GetLinks() == 0) {
                // candidate to be removed
                args.Nodes.insert(*node);
            }
        }
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_DestroySession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroySession& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        return;
    }

    if (!CheckSessionForDestroy(session, args.SessionSeqNo) &&
        session->DeleteSubSession(args.SessionSeqNo))
    {
        db.WriteSession(*session);
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "DestroySession");
    }

    auto handle = session->Handles.begin();
    while (handle != session->Handles.end()) {
        auto nodeId = handle->GetNodeId();
        DestroyHandle(db, &*(handle++));

        auto it = args.Nodes.find(nodeId);
        if (it != args.Nodes.end() && !HasOpenHandles(nodeId)) {
            RemoveNode(
                db,
                *it,
                it->MinCommitId,
                commitId);
        }
    }

    RemoveSession(db, args.SessionId);

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_DestroySession(
    const TActorContext& ctx,
    TTxIndexTablet::TDestroySession& args)
{
    UpdateInMemoryIndexState(std::move(args.NodeUpdates));
    RemoveTransaction(*args.RequestInfo);

    auto response =
        std::make_unique<TEvIndexTablet::TEvDestroySessionResponse>();

    const auto& followerIds = GetFileSystem().GetFollowerFileSystemIds();
    if (followerIds.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s DestroySession completed",
            LogTag.c_str());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s DestroySession completed - local"
        ", destroying follower sessions (%s)",
        LogTag.c_str(),
        JoinSeq(",", GetFileSystem().GetFollowerFileSystemIds()).c_str());

    auto actor = std::make_unique<TDestroyFollowerSessionsActor>(
        LogTag,
        args.RequestInfo,
        args.Request,
        followerIds,
        std::move(response));

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
