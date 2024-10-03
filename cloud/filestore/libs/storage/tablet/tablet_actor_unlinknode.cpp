#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TUnlinkNodeRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (auto error = ValidateNodeName(request.GetName()); HasError(error)) {
        return error;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TUnlinkNodeInFollowerActor final
    : public TActorBootstrapped<TUnlinkNodeInFollowerActor>
{
private:
    const TString LogTag;
    TRequestInfoPtr RequestInfo;
    const TActorId ParentId;
    const NProto::TUnlinkNodeRequest Request;
    const ui64 RequestId;
    const ui64 OpLogEntryId;
    TUnlinkNodeInFollowerResult Result;

public:
    TUnlinkNodeInFollowerActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProto::TUnlinkNodeRequest request,
        ui64 requestId,
        ui64 opLogEntryId,
        TUnlinkNodeInFollowerResult result);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequest(const TActorContext& ctx);

    void HandleUnlinkNodeResponse(
        const TEvService::TEvUnlinkNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TUnlinkNodeInFollowerActor::TUnlinkNodeInFollowerActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProto::TUnlinkNodeRequest request,
        ui64 requestId,
        ui64 opLogEntryId,
        TUnlinkNodeInFollowerResult result)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , ParentId(parentId)
    , Request(std::move(request))
    , RequestId(requestId)
    , OpLogEntryId(opLogEntryId)
    , Result(std::move(result))
{}

void TUnlinkNodeInFollowerActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TUnlinkNodeInFollowerActor::SendRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvUnlinkNodeRequest>();
    request->Record = Request;

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending UnlinkNodeRequest to follower %s, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetName().c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release());
}

void TUnlinkNodeInFollowerActor::HandleUnlinkNodeResponse(
    const TEvService::TEvUnlinkNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->GetError().GetCode() == E_FS_NOENT) {
        // NOENT can arrive after a successful operation is retried, it's ok
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower node unlinking for %s, %s returned ENOENT %s",
            LogTag.c_str(),
            Request.GetFileSystemId().c_str(),
            Request.GetName().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        msg->Record.ClearError();
    }

    if (HasError(msg->GetError())) {
        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s Follower node unlinking failed for %s, %s with error %s"
                ", retrying",
                LogTag.c_str(),
                Request.GetFileSystemId().c_str(),
                Request.GetName().c_str(),
                FormatError(msg->GetError()).Quote().c_str());

            SendRequest(ctx);
            return;
        }

        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower node unlinking failed for %s, %s with error %s"
            ", will not retry",
            LogTag.c_str(),
            Request.GetFileSystemId().c_str(),
            Request.GetName().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Follower node unlinked for %s, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetName().c_str());

    ReplyAndDie(ctx, {});
}

void TUnlinkNodeInFollowerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TUnlinkNodeInFollowerActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        if (auto* x = std::get_if<NProto::TRenameNodeResponse>(&Result)) {
            *x->MutableError() = std::move(error);
        } else if (auto* x = std::get_if<NProto::TUnlinkNodeResponse>(&Result)) {
            *x->MutableError() = std::move(error);
        } else {
            TABLET_VERIFY_C(
                0,
                TStringBuilder() << "bad variant index: " << Result.index());
        }
    }

    using TResponse = TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower;
    ctx.Send(ParentId, std::make_unique<TResponse>(
        std::move(RequestInfo),
        Request.GetHeaders().GetSessionId(),
        RequestId,
        OpLogEntryId,
        std::move(Result)));

    Die(ctx);
}

STFUNC(TUnlinkNodeInFollowerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvUnlinkNodeResponse, HandleUnlinkNodeResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnlinkNode(
    const TEvService::TEvUnlinkNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvUnlinkNodeResponse>(
                std::move(error)));

        return;
    }

    auto* msg = ev->Get();

    // DupCache isn't needed for Create/UnlinkNode requests in shards
    if (!IsShard()) {
        auto* session = AcceptRequest<TEvService::TUnlinkNodeMethod>(ev, ctx, ValidateRequest);
        if (!session) {
            return;
        }

        const auto requestId = GetRequestId(msg->Record);
        if (const auto* e = session->LookupDupEntry(requestId)) {
            auto response = std::make_unique<TEvService::TEvUnlinkNodeResponse>();
            if (GetDupCacheEntry(e, response->Record)) {
                return NCloud::Reply(ctx, *ev, std::move(response));
            }

            session->DropDupEntry(requestId);
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TUnlinkNodeMethod>(*requestInfo);

    ExecuteTx<TUnlinkNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnlinkNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnlinkNode& args)
{
    Y_UNUSED(ctx);

    if (!IsShard()) {
        FILESTORE_VALIDATE_DUPTX_SESSION(UnlinkNode, args);
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    // validate parent node exists
    if (!ReadNode(db, args.ParentNodeId, args.CommitId, args.ParentNode)) {
        return false;   // not ready
    }

    if (!args.ParentNode) {
        args.Error = ErrorInvalidParent(args.ParentNodeId);
        return true;
    }

    // TODO: AccessCheck

    // validate target node exists
    if (!ReadNodeRef(db, args.ParentNodeId, args.CommitId, args.Name, args.ChildRef)) {
        return false;   // not ready
    }

    if (!args.ChildRef) {
        args.Error = ErrorInvalidTarget(args.ParentNodeId, args.Name);
        return true;
    }

    if (!ReadNode(db, args.ChildRef->ChildNodeId, args.CommitId, args.ChildNode)) {
        return false;   // not ready
    }

    if (args.ChildRef->FollowerId) {
        return true;
    }

    // TODO: AccessCheck

    if (!args.ChildNode) {
        auto message = ReportChildNodeIsNull(TStringBuilder()
            << "UnlinkNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return true;
    }

    if (args.ChildNode->Attrs.GetType() == NProto::E_DIRECTORY_NODE) {
        TVector<IIndexTabletDatabase::TNodeRef> refs;
        // 1 entry is enough to prevent deletion
        if (!ReadNodeRefs(db, args.ChildRef->ChildNodeId, args.CommitId, {}, refs, 1)) {
            return false;
        }

        if (!refs.empty()) {
            // cannot unlink non empty directory
            args.Error = ErrorIsNotEmpty(args.ParentNodeId);
            return true;
        }

        if (!args.Request.GetUnlinkDirectory()) {
            // should expliciltly unlink directory node
            args.Error = ErrorIsDirectory(args.ParentNodeId);
            return true;
        }
    } else if (args.Request.GetUnlinkDirectory()) {
        args.Error = ErrorIsNotDirectory(args.ParentNodeId);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_UnlinkNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnlinkNode& args)
{
    FILESTORE_VALIDATE_TX_ERROR(UnlinkNode, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "UnlinkNode");
    }

    if (args.ChildRef->FollowerId) {
        UnlinkExternalNode(
            db,
            args.ParentNodeId,
            args.Name,
            args.ChildRef->FollowerId,
            args.ChildRef->FollowerName,
            args.ChildRef->MinCommitId,
            args.CommitId);

        // OpLogEntryId doesn't have to be a CommitId - it's just convenient to
        // use CommitId here in order not to generate some other unique ui64
        args.OpLogEntry.SetEntryId(args.CommitId);
        auto* followerRequest = args.OpLogEntry.MutableUnlinkNodeRequest();
        followerRequest->CopyFrom(args.Request);
        followerRequest->SetFileSystemId(args.ChildRef->FollowerId);
        followerRequest->SetNodeId(RootNodeId);
        followerRequest->SetName(args.ChildRef->FollowerName);

        db.WriteOpLogEntry(args.OpLogEntry);
    } else {
        auto e = UnlinkNode(
            db,
            args.ParentNodeId,
            args.Name,
            *args.ChildNode,
            args.ChildRef->MinCommitId,
            args.CommitId);

        if (HasError(e)) {
            args.Error = std::move(e);
            return;
        }
    }

    if (!IsShard()) {
        auto* session = FindSession(args.SessionId);
        if (!session) {
            auto message = ReportSessionNotFoundInTx(TStringBuilder()
                << "UnlinkNode: " << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        }

        AddDupCacheEntry(
            db,
            session,
            args.RequestId,
            NProto::TUnlinkNodeResponse{},
            Config->GetDupCacheEntryCount());
    }

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_UnlinkNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnlinkNode& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s[%s] UnlinkNode completed (%s)",
        LogTag.c_str(),
        args.SessionId.c_str(),
        FormatError(args.Error).c_str());

    if (!HasError(args.Error) && !args.ChildRef) {
        auto message = ReportChildRefIsNull(TStringBuilder()
            << "UnlinkNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    if (!HasError(args.Error)) {
        if (args.ChildRef->FollowerId) {
            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s Unlinking node in follower upon UnlinkNode: %s, %s",
                LogTag.c_str(),
                args.ChildRef->FollowerId.c_str(),
                args.ChildRef->FollowerName.c_str());

            RegisterUnlinkNodeInFollowerActor(
                ctx,
                args.RequestInfo,
                args.OpLogEntry.GetUnlinkNodeRequest(),
                args.RequestId,
                args.OpLogEntry.GetEntryId(),
                std::move(args.Response));

            return;
        }

        if (!IsShard()) {
            CommitDupCacheEntry(args.SessionId, args.RequestId);
        }

        // TODO(#1350): support session events for external nodes
        NProto::TSessionEvent sessionEvent;
        {
            auto* unlinked = sessionEvent.AddNodeUnlinked();
            unlinked->SetParentNodeId(args.ParentNodeId);
            unlinked->SetChildNodeId(args.ChildRef->ChildNodeId);
            unlinked->SetName(args.Name);
        }
        NotifySessionEvent(ctx, sessionEvent);
    }

    RemoveTransaction(*args.RequestInfo);
    EnqueueBlobIndexOpIfNeeded(ctx);

    auto response =
        std::make_unique<TEvService::TEvUnlinkNodeResponse>(args.Error);
    CompleteResponse<TEvService::TUnlinkNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleNodeUnlinkedInFollower(
    const TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& res = msg->Result;

    if (msg->RequestInfo) {
        RemoveTransaction(*msg->RequestInfo);
        CommitDupCacheEntry(msg->SessionId, msg->RequestId);

        if (auto* x = std::get_if<NProto::TRenameNodeResponse>(&res)) {
            auto response =
                std::make_unique<TEvService::TEvRenameNodeResponse>();
            response->Record = std::move(*x);

            CompleteResponse<TEvService::TRenameNodeMethod>(
                response->Record,
                msg->RequestInfo->CallContext,
                ctx);

            NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        } else if (auto* x = std::get_if<NProto::TUnlinkNodeResponse>(&res)) {
            auto response =
                std::make_unique<TEvService::TEvUnlinkNodeResponse>();
            response->Record = std::move(*x);

            CompleteResponse<TEvService::TUnlinkNodeMethod>(
                response->Record,
                msg->RequestInfo->CallContext,
                ctx);

            NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        } else {
            TABLET_VERIFY_C(
                0,
                TStringBuilder() << "bad variant index: " << res.index());
        }
    }

    WorkerActors.erase(ev->Sender);
    ExecuteTx<TDeleteOpLogEntry>(ctx, msg->OpLogEntryId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterUnlinkNodeInFollowerActor(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProto::TUnlinkNodeRequest request,
    ui64 requestId,
    ui64 opLogEntryId,
    TUnlinkNodeInFollowerResult result)
{
    auto actor = std::make_unique<TUnlinkNodeInFollowerActor>(
        LogTag,
        std::move(requestInfo),
        ctx.SelfID,
        std::move(request),
        requestId,
        opLogEntryId,
        std::move(result));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
