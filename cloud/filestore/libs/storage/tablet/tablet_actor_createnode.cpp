#include "tablet_actor.h"

#include "helpers.h"

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
    const TString FollowerId;
    const TString FollowerName;
    const TActorId ParentId;
    NProto::TCreateNodeRequest Request;
    NProto::TCreateNodeResponse Response;

public:
    TCreateNodeInFollowerActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        TString followerId,
        TString followerName,
        const TActorId& parentId,
        NProto::TCreateNodeRequest request,
        NProto::TCreateNodeResponse response);

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
        TString followerId,
        TString followerName,
        const TActorId& parentId,
        NProto::TCreateNodeRequest request,
        NProto::TCreateNodeResponse response)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , FollowerId(std::move(followerId))
    , FollowerName(std::move(followerName))
    , ParentId(parentId)
    , Request(std::move(request))
    , Response(std::move(response))
{}

void TCreateNodeInFollowerActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TCreateNodeInFollowerActor::SendRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();
    request->Record = std::move(Request);
    request->Record.SetFileSystemId(FollowerId);
    request->Record.SetNodeId(RootNodeId);
    request->Record.SetName(FollowerName);
    request->Record.ClearFollowerFileSystemId();

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending CreateNodeRequest to follower %s, %s",
        LogTag.c_str(),
        FollowerId.c_str(),
        FollowerName.c_str());

    ctx.Send(
        MakeIndexTabletProxyServiceId(),
        request.release());
}

void TCreateNodeInFollowerActor::HandleCreateNodeResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "%s Follower node creation failed for %s, %s with error %s",
            LogTag.c_str(),
            FollowerId.c_str(),
            FollowerName.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Follower node created for %s, %s",
        LogTag.c_str(),
        FollowerId.c_str(),
        FollowerName.c_str());

    *Response.MutableNode() = std::move(*msg->Record.MutableNode());

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
        // TODO(#1350): properly retry node creation via the leader fs
        // don't forget to properly handle EEXIST after a retry
        *Response.MutableError() = std::move(error);
    }

    using TResponse = TEvIndexTabletPrivate::TEvNodeCreatedInFollower;
    ctx.Send(ParentId, std::make_unique<TResponse>(
        std::move(RequestInfo),
        std::move(Response)));

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
        msg->Record,
        parentNodeId,
        targetNodeId,
        attrs);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CreateNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateNode& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(CreateNode, args);

    TIndexTabletDatabase db(tx.DB);

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
    TABLET_VERIFY(args.ParentNode);

    // validate target node doesn't exist
    TMaybe<TIndexTabletDatabase::TNodeRef> childRef;
    if (!ReadNodeRef(db, args.ParentNodeId, args.CommitId, args.Name, childRef)) {
        return false;   // not ready
    }

    if (childRef) {
        // mknod, mkdir, link nor symlink does not overwrite existing files
        args.Error = ErrorAlreadyExists(args.Name);
        return true;
    }

    TABLET_VERIFY(!args.ChildNode);
    if (args.TargetNodeId != InvalidNodeId) {
        // hard link: validate link target
        args.ChildNodeId = args.TargetNodeId;
        if (!ReadNode(db, args.ChildNodeId, args.CommitId, args.ChildNode)) {
            return false;   // not ready
        }

        if (!args.ChildNode){
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

        // TODO: AccessCheck
        TABLET_VERIFY(args.ChildNode);
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
    TABLET_VERIFY(session);

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "CreateNode");
    }

    if (!args.ChildNode) {
        if (args.FollowerId.Empty()) {
            args.ChildNodeId = CreateNode(
                db,
                args.CommitId,
                args.Attrs);

            args.ChildNode = TIndexTabletDatabase::TNode {
                args.ChildNodeId,
                args.Attrs,
                args.CommitId,
                InvalidCommitId
            };
        }
    } else {
        // hard link
        auto attrs = CopyAttrs(args.ChildNode->Attrs, E_CM_CMTIME | E_CM_REF);
        UpdateNode(
            db,
            args.ChildNodeId,
            args.ChildNode->MinCommitId,
            args.CommitId,
            attrs,
            args.ChildNode->Attrs);

        args.ChildNode->Attrs = std::move(attrs);
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
        TABLET_VERIFY(args.ChildNodeId != InvalidNodeId);

        ConvertNodeFromAttrs(
            *args.Response.MutableNode(),
            args.ChildNodeId,
            args.ChildNode->Attrs);

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

    // TODO(#1350): support DupCache for external nodes
}

void TIndexTabletActor::CompleteTx_CreateNode(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateNode& args)
{
    RemoveTransaction(*args.RequestInfo);

    if (args.FollowerId && !HasError(args.Error)) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Creating node in follower upon CreateNode: %s, %s",
            LogTag.c_str(),
            args.FollowerId.c_str(),
            args.FollowerName.c_str());

        auto actor = std::make_unique<TCreateNodeInFollowerActor>(
            LogTag,
            args.RequestInfo,
            args.FollowerId,
            args.FollowerName,
            ctx.SelfID,
            std::move(args.Request),
            std::move(args.Response));

        auto actorId = NCloud::Register(ctx, std::move(actor));
        WorkerActors.insert(actorId);

        return;
    }

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

        TABLET_VERIFY(args.ChildNode);
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

    auto response = std::make_unique<TEvService::TEvCreateNodeResponse>();
    response->Record = std::move(msg->CreateNodeResponse);

    CompleteResponse<TEvService::TCreateNodeMethod>(
        response->Record,
        msg->RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));

    WorkerActors.erase(ev->Sender);
}

}   // namespace NCloud::NFileStore::NStorage
