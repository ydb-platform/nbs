#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TCreateNodeRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId ||
        (request.HasLink()
         && request.GetLink().GetTargetNode() == InvalidNodeId) ||
        (request.HasSymLink() && request.GetSymLink().GetTargetPath().empty()))
    {
        return ErrorInvalidArgument();
    }

    if (auto error = ValidateNodeName(request.GetName()); HasError(error)) {
        return error;
    }

    if (request.HasSymLink()) {
        const auto& path = request.GetSymLink().GetTargetPath();
        if (path.size() > MaxSymlink) {
            return ErrorNameTooLong(path);
        }
    }

    return {};
}

void InitAttrs(NProto::TNode& attrs, const NProto::TCreateNodeRequest& request)
{
    if (request.HasDirectory()) {
        const auto& dir = request.GetDirectory();
        attrs = CreateDirectoryAttrs(
            dir.GetMode(),
            request.GetUid(),
            request.GetGid());
    } else if (request.HasFile()) {
        const auto& file = request.GetFile();
        attrs = CreateRegularAttrs(
            file.GetMode(),
            request.GetUid(),
            request.GetGid());
    } else if (request.HasSymLink()) {
        const auto& link = request.GetSymLink();
        attrs = CreateLinkAttrs(
            link.GetTargetPath(),
            request.GetUid(),
            request.GetGid());
    } else if (request.HasSocket()) {
        const auto& sock = request.GetSocket();
        attrs = CreateSocketAttrs(
            sock.GetMode(),
            request.GetUid(),
            request.GetGid());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCreateNodeInFollowerActor final
    : public TActorBootstrapped<TCreateNodeInFollowerActor>
{
private:
    const TString LogTag;
    TRequestInfoPtr RequestInfo;
    const TActorId ParentId;
    const NProto::TCreateNodeRequest Request;
    const ui64 RequestId;
    const ui64 OpLogEntryId;
    TCreateNodeInFollowerResult Result;

public:
    TCreateNodeInFollowerActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProto::TCreateNodeRequest request,
        ui64 requestId,
        ui64 opLogEntryId,
        TCreateNodeInFollowerResult result);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequest(const TActorContext& ctx);

    void HandleCreateNodeResponse(
        const TEvService::TEvCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TCreateNodeInFollowerActor::TCreateNodeInFollowerActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProto::TCreateNodeRequest request,
        ui64 requestId,
        ui64 opLogEntryId,
        TCreateNodeInFollowerResult result)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , ParentId(parentId)
    , Request(std::move(request))
    , RequestId(requestId)
    , OpLogEntryId(opLogEntryId)
    , Result(std::move(result))
{}

void TCreateNodeInFollowerActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TCreateNodeInFollowerActor::SendRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();
    request->Record = Request;

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending CreateNodeRequest to follower %s, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetName().c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release());
}

void TCreateNodeInFollowerActor::HandleCreateNodeResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->GetError().GetCode() == E_FS_EXIST) {
        // EXIST can arrive after a successful operation is retried, it's ok
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower node creation for %s, %s returned EEXIST %s",
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
                "%s Follower node creation failed for %s, %s with error %s"
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
            "%s Follower node creation failed for %s, %s with error %s"
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
        "%s Follower node created for %s, %s",
        LogTag.c_str(),
        Request.GetFileSystemId().c_str(),
        Request.GetName().c_str());

    if (auto* x = std::get_if<NProto::TCreateNodeResponse>(&Result)) {
        *x->MutableNode() = std::move(*msg->Record.MutableNode());
    } else if (auto* x = std::get_if<NProto::TCreateHandleResponse>(&Result)) {
        *x->MutableNodeAttr() = std::move(*msg->Record.MutableNode());
    } else {
        TABLET_VERIFY_C(
            0,
            TStringBuilder() << "bad variant index: " << Result.index());
    }

    ReplyAndDie(ctx, {});
}

void TCreateNodeInFollowerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TCreateNodeInFollowerActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        if (auto* x = std::get_if<NProto::TCreateNodeResponse>(&Result)) {
            *x->MutableError() = std::move(error);
        } else if (auto* x =
                std::get_if<NProto::TCreateHandleResponse>(&Result))
        {
            *x->MutableError() = std::move(error);
        } else {
            TABLET_VERIFY_C(
                0,
                TStringBuilder() << "bad variant index: " << Result.index());
        }
    }

    using TResponse = TEvIndexTabletPrivate::TEvNodeCreatedInFollower;
    ctx.Send(ParentId, std::make_unique<TResponse>(
        std::move(RequestInfo),
        Request.GetHeaders().GetSessionId(),
        RequestId,
        OpLogEntryId,
        std::move(Result)));

    Die(ctx);
}

STFUNC(TCreateNodeInFollowerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvCreateNodeResponse, HandleCreateNodeResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCreateNode(
    const TEvService::TEvCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* session =
        AcceptRequest<TEvService::TCreateNodeMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();
    if (const auto* e = session->LookupDupEntry(GetRequestId(msg->Record))) {
        auto response = std::make_unique<TEvService::TEvCreateNodeResponse>();
        GetDupCacheEntry(e, response->Record);
        if (response->Record.GetNode().GetId() == 0) {
            // it's an external node which is not yet created in follower
            // this check is needed for the case of leader reboot
            *response->Record.MutableError() = MakeError(
                E_REJECTED,
                "node not yet created in follower");
        }
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    ui64 parentNodeId = msg->Record.GetNodeId();
    ui64 targetNodeId = InvalidNodeId;
    if (msg->Record.HasLink()) {
        targetNodeId = msg->Record.GetLink().GetTargetNode();
    }

    NProto::TNode attrs;
    InitAttrs(attrs, msg->Record);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TCreateNodeMethod>(*requestInfo);

    ExecuteTx<TCreateNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record),
        parentNodeId,
        targetNodeId,
        std::move(attrs));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CreateNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateNode& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(CreateNode, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    // validate there are enough free inodes
    if (GetUsedNodesCount() >= GetNodesCount()) {
        args.Error = ErrorNoSpaceLeft();
        return true;
    }

    // validate parent node exists
    if (!ReadNode(db, args.ParentNodeId, args.CommitId, args.ParentNode)) {
        return false;   // not ready
    }

    if (!args.ParentNode) {
        args.Error = ErrorInvalidParent(args.ParentNodeId);
        return true;
    }

    if (args.ParentNode->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
        args.Error = ErrorIsNotDirectory(args.ParentNodeId);
        return true;
    }

    // TODO: AccessCheck

    // validate target node doesn't exist
    TMaybe<IIndexTabletDatabase::TNodeRef> childRef;
    if (!ReadNodeRef(db, args.ParentNodeId, args.CommitId, args.Name, childRef)) {
        return false;   // not ready
    }

    if (childRef) {
        // mknod, mkdir, link nor symlink does not overwrite existing files
        args.Error = ErrorAlreadyExists(args.Name);
        return true;
    }

    if (args.ChildNode) {
        auto message = ReportChildNodeWithoutRef(TStringBuilder()
            << "CreateNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return true;
    }

    if (args.TargetNodeId != InvalidNodeId) {
        // hard link: validate link target
        //
        // Note: for the cases where the FollowerId is set, the target node
        // already exists and its link count is updated, no need to validate it
        if (!ReadNode(db, args.TargetNodeId, args.CommitId, args.ChildNode)) {
            return false;   // not ready
        }

        if (args.FollowerId.Empty()) {
            args.ChildNodeId = args.TargetNodeId;
            if (!args.ChildNode) {
                // should exist
                args.Error = ErrorInvalidTarget(args.ChildNodeId);
                return true;
            }

            if (args.ChildNode->Attrs.GetType() == NProto::E_DIRECTORY_NODE) {
                // should not be a directory
                args.Error = ErrorIsDirectory(args.ChildNodeId);
                return true;
            }

            if (args.ChildNode->Attrs.GetLinks() + 1 > MaxLink) {
                // should not have too many links
                args.Error = ErrorMaxLink(args.ChildNodeId);
                return true;
            }
        }

        // TODO: AccessCheck
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_CreateNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateNode& args)
{
    FILESTORE_VALIDATE_TX_ERROR(CreateNode, args);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        auto message = ReportSessionNotFoundInTx(TStringBuilder()
            << "CreateNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "CreateNode");
    }

    if (args.TargetNodeId == InvalidNodeId) {
        if (args.FollowerId.Empty()) {
            args.ChildNodeId = CreateNode(
                db,
                args.CommitId,
                args.Attrs);

            args.ChildNode = IIndexTabletDatabase::TNode {
                args.ChildNodeId,
                args.Attrs,
                args.CommitId,
                InvalidCommitId
            };
        } else {
            // OpLogEntryId doesn't have to be a CommitId - it's just convenient to
            // use CommitId here in order not to generate some other unique ui64
            args.OpLogEntry.SetEntryId(args.CommitId);
            args.OpLogEntry.SetSessionId(args.SessionId);
            args.OpLogEntry.SetRequestId(args.RequestId);
            auto* followerRequest = args.OpLogEntry.MutableCreateNodeRequest();
            followerRequest->CopyFrom(args.Request);
            followerRequest->SetFileSystemId(args.FollowerId);
            followerRequest->SetNodeId(RootNodeId);
            followerRequest->SetName(args.FollowerName);
            followerRequest->ClearFollowerFileSystemId();

            db.WriteOpLogEntry(args.OpLogEntry);
        }
    } else {
        // hard link

        // If the follower is set, no need to update the child node since it is
        // an external node
        if (args.FollowerId.Empty()) {
            auto attrs =
                CopyAttrs(args.ChildNode->Attrs, E_CM_CMTIME | E_CM_REF);
            UpdateNode(
                db,
                args.ChildNodeId,
                args.ChildNode->MinCommitId,
                args.CommitId,
                attrs,
                args.ChildNode->Attrs);

            args.ChildNode->Attrs = std::move(attrs);
        }
    }

    // update parents cmtime
    auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
    UpdateNode(
        db,
        args.ParentNode->NodeId,
        args.ParentNode->MinCommitId,
        args.CommitId,
        parent,
        args.ParentNode->Attrs);

    CreateNodeRef(
        db,
        args.ParentNodeId,
        args.CommitId,
        args.Name,
        args.ChildNodeId,
        args.FollowerId,
        args.FollowerName);

    if (args.FollowerId.Empty()) {
        if (args.ChildNodeId == InvalidNodeId) {
            auto message = ReportInvalidNodeIdForLocalNode(TStringBuilder()
                << "CreateNode: " << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        }

        ConvertNodeFromAttrs(
            *args.Response.MutableNode(),
            args.ChildNodeId,
            args.ChildNode->Attrs);
    }

    // followers shouldn't commit CreateNode DupCache entries since:
    // 1. there will be no duplicates - node name is generated by the leader
    // 2. the leader serves all file creation operations and has its own
    //  dupcache
    if (!GetFileSystem().GetShardNo()) {
        AddDupCacheEntry(
            db,
            session,
            args.RequestId,
            args.Response,
            Config->GetDupCacheEntryCount());
    }
}

void TIndexTabletActor::CompleteTx_CreateNode(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateNode& args)
{
    if (args.OpLogEntry.HasCreateNodeRequest() && !HasError(args.Error)) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s Creating node in follower upon CreateNode: %s, %s",
            LogTag.c_str(),
            args.FollowerId.c_str(),
            args.FollowerName.c_str());

        RegisterCreateNodeInFollowerActor(
            ctx,
            args.RequestInfo,
            std::move(*args.OpLogEntry.MutableCreateNodeRequest()),
            args.RequestId,
            args.OpLogEntry.GetEntryId(),
            std::move(args.Response));

        return;
    }

    RemoveTransaction(*args.RequestInfo);

    auto response =
        std::make_unique<TEvService::TEvCreateNodeResponse>(args.Error);
    if (!HasError(args.Error)) {
        // followers shouldn't commit CreateNode DupCache entries since:
        // 1. there will be no duplicates - node name is generated by the leader
        // 2. the leader serves all file creation operations and has its own
        //  dupcache
        if (!GetFileSystem().GetShardNo()) {
            CommitDupCacheEntry(args.SessionId, args.RequestId);
        }

        if (!args.ChildNode &&
            // A ChildNode can also be empty for a hard link to an external
            // node, and this is a valid case
            !(args.FollowerId && args.TargetNodeId != InvalidNodeId))
        {
            auto message = ReportChildNodeIsNull(TStringBuilder()
                << "CreateNode: " << args.Request.ShortDebugString());
            *args.Response.MutableError() =
                MakeError(E_INVALID_STATE, std::move(message));
        }
        response->Record = std::move(args.Response);

        NProto::TSessionEvent sessionEvent;
        {
            auto* linked = sessionEvent.AddNodeLinked();
            linked->SetParentNodeId(args.ParentNodeId);
            linked->SetChildNodeId(args.ChildNodeId);
            linked->SetName(args.Name);
        }

        NotifySessionEvent(ctx, sessionEvent);
    }

    CompleteResponse<TEvService::TCreateNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleNodeCreatedInFollower(
    const TEvIndexTabletPrivate::TEvNodeCreatedInFollower::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& res = msg->Result;

    if (msg->RequestInfo) {
        RemoveTransaction(*msg->RequestInfo);
    }

    WorkerActors.erase(ev->Sender);

    if (auto* x = std::get_if<NProto::TCreateNodeResponse>(&res)) {
        if (msg->RequestInfo) {
            auto response =
                std::make_unique<TEvService::TEvCreateNodeResponse>();
            response->Record = *x;

            CompleteResponse<TEvService::TCreateNodeMethod>(
                response->Record,
                msg->RequestInfo->CallContext,
                ctx);

            // replying before DupCacheEntry is committed to reduce response
            // latency
            NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        }

        ExecuteTx<TCommitNodeCreationInFollower>(
            ctx,
            msg->SessionId,
            msg->RequestId,
            std::move(*x),
            msg->OpLogEntryId);
    } else if (auto* x = std::get_if<NProto::TCreateHandleResponse>(&res)) {
        CommitDupCacheEntry(msg->SessionId, msg->RequestId);

        if (msg->RequestInfo) {
            auto response =
                std::make_unique<TEvService::TEvCreateHandleResponse>();
            response->Record = std::move(*x);

            CompleteResponse<TEvService::TCreateHandleMethod>(
                response->Record,
                msg->RequestInfo->CallContext,
                ctx);

            NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        }

        ExecuteTx<TDeleteOpLogEntry>(ctx, msg->OpLogEntryId);
    } else {
        TABLET_VERIFY_C(
            0,
            TStringBuilder() << "bad variant index: " << res.index());
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CommitNodeCreationInFollower(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCommitNodeCreationInFollower& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_CommitNodeCreationInFollower(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCommitNodeCreationInFollower& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    PatchDupCacheEntry(
        db,
        args.SessionId,
        args.RequestId,
        std::move(args.Response));
    db.DeleteOpLogEntry(args.EntryId);
}

void TIndexTabletActor::CompleteTx_CommitNodeCreationInFollower(
    const TActorContext& ctx,
    TTxIndexTablet::TCommitNodeCreationInFollower& args)
{
    CommitDupCacheEntry(args.SessionId, args.RequestId);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s CommitNodeCreationInFollower completed (%lu): %s, %lu",
        LogTag.c_str(),
        args.EntryId,
        args.SessionId.c_str(),
        args.RequestId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterCreateNodeInFollowerActor(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProto::TCreateNodeRequest request,
    ui64 requestId,
    ui64 opLogEntryId,
    TCreateNodeInFollowerResult result)
{
    auto actor = std::make_unique<TCreateNodeInFollowerActor>(
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
