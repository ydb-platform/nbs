#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TCreateHandleRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (!request.GetName().empty()) {
        if (auto error = ValidateNodeName(request.GetName()); HasError(error)) {
            return error;
        }
    }

    if (HasFlag(request.GetFlags(), NProto::TCreateHandleRequest::E_TRUNCATE) &&
        !HasFlag(request.GetFlags(), NProto::TCreateHandleRequest::E_WRITE))
    {
        // POSIX: The result of using O_TRUNC without either O_RDWR o O_WRONLY
        // is undefined
        return ErrorInvalidArgument();
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TCreateNodeInFollowerUponCreateHandleActor final
    : public TActorBootstrapped<TCreateNodeInFollowerUponCreateHandleActor>
{
private:
    const TString LogTag;
    TRequestInfoPtr RequestInfo;
    const TString FollowerId;
    const TString FollowerName;
    const TActorId ParentId;
    const NProto::TNode Attrs;
    NProto::THeaders Headers;
    NProto::TCreateHandleResponse Response;

public:
    TCreateNodeInFollowerUponCreateHandleActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        TString followerId,
        TString followerName,
        const TActorId& parentId,
        NProto::TNode attrs,
        NProto::THeaders headers,
        NProto::TCreateHandleResponse response);

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

TCreateNodeInFollowerUponCreateHandleActor::TCreateNodeInFollowerUponCreateHandleActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        TString followerId,
        TString followerName,
        const TActorId& parentId,
        NProto::TNode attrs,
        NProto::THeaders headers,
        NProto::TCreateHandleResponse response)
    : LogTag(std::move(logTag))
    , RequestInfo(std::move(requestInfo))
    , FollowerId(std::move(followerId))
    , FollowerName(std::move(followerName))
    , ParentId(parentId)
    , Attrs(std::move(attrs))
    , Headers(std::move(headers))
    , Response(std::move(response))
{}

void TCreateNodeInFollowerUponCreateHandleActor::Bootstrap(const TActorContext& ctx)
{
    SendRequest(ctx);
    Become(&TThis::StateWork);
}

void TCreateNodeInFollowerUponCreateHandleActor::SendRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();
    *request->Record.MutableHeaders() = std::move(Headers);
    request->Record.MutableFile()->SetMode(Attrs.GetMode());
    request->Record.SetUid(Attrs.GetUid());
    request->Record.SetGid(Attrs.GetGid());
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

void TCreateNodeInFollowerUponCreateHandleActor::HandleCreateNodeResponse(
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

    *Response.MutableNodeAttr() = std::move(*msg->Record.MutableNode());

    ReplyAndDie(ctx, {});
}

void TCreateNodeInFollowerUponCreateHandleActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TCreateNodeInFollowerUponCreateHandleActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        // TODO(#1350): properly retry node creation via the leader fs
        *Response.MutableError() = std::move(error);
    }

    // TODO(#1350): reply directly to the client (not to the tablet) upon
    // poisoning
    // or keep this RequestInfo in the ActiveTransactions list until this event
    // gets processed by the tablet
    using TResponse =
        TEvIndexTabletPrivate::TEvNodeCreatedInFollowerUponCreateHandle;
    ctx.Send(ParentId, std::make_unique<TResponse>(
        std::move(RequestInfo),
        std::move(Response)));

    Die(ctx);
}

STFUNC(TCreateNodeInFollowerUponCreateHandleActor::StateWork)
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

void TIndexTabletActor::HandleCreateHandle(
    const TEvService::TEvCreateHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvService::TEvCreateHandleResponse;

    auto* session =
        AcceptRequest<TEvService::TCreateHandleMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();
    if (const auto* e = session->LookupDupEntry(GetRequestId(msg->Record))) {
        auto response = std::make_unique<TResponse>();
        GetDupCacheEntry(e, response->Record);
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (msg->Record.GetFollowerFileSystemId()) {
        bool found = false;
        for (const auto& f: GetFileSystem().GetFollowerFileSystemIds()) {
            if (f == msg->Record.GetFollowerFileSystemId()) {
                found = true;
                break;
            }
        }

        if (!found) {
            auto error = MakeError(E_ARGUMENT, TStringBuilder() <<
                "invalid follower id: "
                << msg->Record.GetFollowerFileSystemId());
            LOG_ERROR(
                ctx,
                TFileStoreComponents::TABLET,
                "%s Can't create handle: %s",
                LogTag.c_str(),
                FormatError(error).Quote().c_str());

            auto response = std::make_unique<TResponse>(std::move(error));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TCreateHandleMethod>(*requestInfo);

    ExecuteTx<TCreateHandle>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CreateHandle(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateHandle& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(CreateHandle, args);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);
    TABLET_VERIFY(session);

    args.ReadCommitId = GetReadCommitId(session->GetCheckpointId());
    if (args.ReadCommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(session->GetCheckpointId());
        return true;
    }

    TIndexTabletDatabase db(tx.DB);

    // There could be two cases:
    // * access by parentId/name
    // * access by nodeId
    if (args.Name) {
        // check that parent exists and is the directory;
        // TODO: what if symlink?
        if (!ReadNode(db, args.NodeId, args.ReadCommitId, args.ParentNode)) {
            return false;
        }

        if (!args.ParentNode || args.ParentNode->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
            args.Error = ErrorIsNotDirectory(args.NodeId);
            return true;
        }

        // check whether child node exists
        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        if (!ReadNodeRef(db, args.NodeId, args.ReadCommitId, args.Name, ref)) {
            return false;   // not ready
        }

        if (!ref) {
            // if not check whether we should create one
            if (!HasFlag(args.Flags, NProto::TCreateHandleRequest::E_CREATE)) {
                args.Error = ErrorInvalidTarget(args.NodeId, args.Name);
                return true;
            }

            // validate there are enough free inodes
            if (GetUsedNodesCount() >= GetNodesCount()) {
                args.Error = ErrorNoSpaceLeft();
                return true;
            }

            if (args.RequestFollowerId) {
                args.FollowerId = args.RequestFollowerId;
                args.FollowerName = CreateGuidAsString();
                args.IsNewFollowerNode = true;
            }
        } else {
            // if yes check whether O_EXCL was specified, assume O_CREAT is also specified
            if (HasFlag(args.Flags, NProto::TCreateHandleRequest::E_EXCLUSIVE)) {
                args.Error = ErrorAlreadyExists(args.Name);
                return true;
            }

            args.TargetNodeId = ref->ChildNodeId;
            args.FollowerId = ref->FollowerId;
            args.FollowerName = ref->FollowerName;
        }
    } else {
        args.TargetNodeId = args.NodeId;
        if (args.RequestFollowerId) {
            args.Error = MakeError(
                E_ARGUMENT,
                TStringBuilder() << "CreateHandle request without Name and with"
                " FollowerId is not supported");
            return true;
        }
    }

    if (args.TargetNodeId != InvalidNodeId) {
        if (!ReadNode(db, args.TargetNodeId, args.ReadCommitId, args.TargetNode)) {
            return false;   // not ready
        }

        if (!args.TargetNode) {
            args.Error = ErrorInvalidTarget(args.TargetNodeId);
            return true;
        }

        // TODO: support for O_DIRECTORY
        if (args.TargetNode->Attrs.GetType() != NProto::E_REGULAR_NODE) {
            args.Error = ErrorIsDirectory(args.TargetNodeId);
            return true;
        }

        // TODO: AccessCheck
        TABLET_VERIFY(args.TargetNode);
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_CreateHandle(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateHandle& args)
{
    FILESTORE_VALIDATE_TX_ERROR(CreateHandle, args);

    auto* session = FindSession(args.SessionId);
    TABLET_VERIFY(session);

    // TODO: check if session is read only
    args.WriteCommitId = GenerateCommitId();
    if (args.WriteCommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "CreateHandle");
    }

    TIndexTabletDatabase db(tx.DB);

    if (args.TargetNodeId == InvalidNodeId
            && (args.FollowerId.Empty() || args.IsNewFollowerNode))
    {
        TABLET_VERIFY(!args.TargetNode);
        TABLET_VERIFY(args.ParentNode);

        if (args.FollowerId.Empty()) {
            NProto::TNode attrs =
                CreateRegularAttrs(args.Mode, args.Uid, args.Gid);
            args.TargetNodeId = CreateNode(
                db,
                args.WriteCommitId,
                attrs);

            args.TargetNode = IIndexTabletDatabase::TNode {
                args.TargetNodeId,
                attrs,
                args.WriteCommitId,
                InvalidCommitId
            };
        }

        // TODO: support for O_TMPFILE
        CreateNodeRef(
            db,
            args.NodeId,
            args.WriteCommitId,
            args.Name,
            args.TargetNodeId,
            args.FollowerId,
            args.FollowerName);

        // update parent cmtime as we created a new entry
        auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
        UpdateNode(
            db,
            args.ParentNode->NodeId,
            args.ParentNode->MinCommitId,
            args.WriteCommitId,
            parent,
            args.ParentNode->Attrs);

    } else if (args.FollowerId.Empty()
        && HasFlag(args.Flags, NProto::TCreateHandleRequest::E_TRUNCATE))
    {
        Truncate(
            db,
            args.TargetNodeId,
            args.WriteCommitId,
            args.TargetNode->Attrs.GetSize(),
            0);

        auto attrs = CopyAttrs(args.TargetNode->Attrs, E_CM_CMTIME);
        attrs.SetSize(0);

        UpdateNode(
            db,
            args.TargetNodeId,
            args.TargetNode->MinCommitId,
            args.WriteCommitId,
            attrs,
            args.TargetNode->Attrs);
    }

    if (args.FollowerId.Empty()) {
        auto* handle = CreateHandle(
            db,
            session,
            args.TargetNodeId,
            session->GetCheckpointId() ? args.ReadCommitId : InvalidCommitId,
            args.Flags);

        TABLET_VERIFY(handle);
        args.Response.SetHandle(handle->GetHandle());
        auto* node = args.Response.MutableNodeAttr();
        ConvertNodeFromAttrs(*node, args.TargetNodeId, args.TargetNode->Attrs);
    } else {
        args.Response.SetFollowerFileSystemId(args.FollowerId);
        args.Response.SetFollowerNodeName(args.FollowerName);
    }

    AddDupCacheEntry(
        db,
        session,
        args.RequestId,
        args.Response,
        Config->GetDupCacheEntryCount());

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_CreateHandle(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateHandle& args)
{
    RemoveTransaction(*args.RequestInfo);

    if (args.Error.GetCode() == E_ARGUMENT) {
        // service actor sent something inappropriate, we'd better log it
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Can't create handle: %s",
            LogTag.c_str(),
            FormatError(args.Error).Quote().c_str());
    }

    if (!HasError(args.Error)) {
        CommitDupCacheEntry(args.SessionId, args.RequestId);
    }

    if (args.IsNewFollowerNode && !HasError(args.Error)) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s Creating node in follower upon CreateHandle: %s, %s",
            LogTag.c_str(),
            args.FollowerId.c_str(),
            args.FollowerName.c_str());

        auto actor = std::make_unique<TCreateNodeInFollowerUponCreateHandleActor>(
            LogTag,
            args.RequestInfo,
            args.FollowerId,
            args.FollowerName,
            ctx.SelfID,
            CreateRegularAttrs(args.Mode, args.Uid, args.Gid),
            std::move(args.Headers),
            std::move(args.Response));

        auto actorId = NCloud::Register(ctx, std::move(actor));
        WorkerActors.insert(actorId);

        return;
    }

    auto response =
        std::make_unique<TEvService::TEvCreateHandleResponse>(args.Error);

    if (!HasError(args.Error)) {
        response->Record = std::move(args.Response);
    }

    CompleteResponse<TEvService::TCreateHandleMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleNodeCreatedInFollowerUponCreateHandle(
    const TEvIndexTabletPrivate::TEvNodeCreatedInFollowerUponCreateHandle::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto response = std::make_unique<TEvService::TEvCreateHandleResponse>();
    response->Record = std::move(msg->CreateHandleResponse);

    CompleteResponse<TEvService::TCreateHandleMethod>(
        response->Record,
        msg->RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));

    WorkerActors.erase(ev->Sender);
}

}   // namespace NCloud::NFileStore::NStorage
