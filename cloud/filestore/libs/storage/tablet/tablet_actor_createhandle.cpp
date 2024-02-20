#include "tablet_actor.h"

#include "helpers.h"

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
        // POSIX: The result of using O_TRUNC without either O_RDWR o O_WRONLY is undefined
        return ErrorInvalidArgument();
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCreateHandle(
    const TEvService::TEvCreateHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* session = AcceptRequest<TEvService::TCreateHandleMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();
    if (auto entry = session->LookupDupEntry(GetRequestId(msg->Record))) {
        auto response = std::make_unique<TEvService::TEvCreateHandleResponse>();
        GetDupCacheEntry(entry, response->Record);
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    auto requestInfo = CreateRequestInfo<TEvService::TCreateHandleMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction(*requestInfo);

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
        TMaybe<TIndexTabletDatabase::TNodeRef> ref;
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
        } else {
            // if yes check whether O_EXCL was specified, assume O_CREAT is also specified
            if (HasFlag(args.Flags, NProto::TCreateHandleRequest::E_EXCLUSIVE)) {
                args.Error = ErrorAlreadyExists(args.Name);
                return true;
            }

            args.TargetNodeId = ref->ChildNodeId;
        }
    } else {
        args.TargetNodeId = args.NodeId;
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

    if (args.TargetNodeId == InvalidNodeId) {
        TABLET_VERIFY(!args.TargetNode);
        TABLET_VERIFY(args.ParentNode);

        NProto::TNode attrs = CreateRegularAttrs(args.Mode, args.Uid, args.Gid);
        args.TargetNodeId = CreateNode(
            db,
            args.WriteCommitId,
            attrs);

        args.TargetNode = TIndexTabletDatabase::TNode {
            args.TargetNodeId,
            attrs,
            args.WriteCommitId,
            InvalidCommitId
        };

        // TODO: support for O_TMPFILE
        CreateNodeRef(
            db,
            args.NodeId,
            args.WriteCommitId,
            args.Name,
            args.TargetNodeId);

        // update parent cmtime as we created a new entry
        auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
        UpdateNode(
            db,
            args.ParentNode->NodeId,
            args.ParentNode->MinCommitId,
            args.WriteCommitId,
            parent,
            args.ParentNode->Attrs);

    } else if (HasFlag(args.Flags, NProto::TCreateHandleRequest::E_TRUNCATE)) {
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

    auto response = std::make_unique<TEvService::TEvCreateHandleResponse>(args.Error);
    if (SUCCEEDED(args.Error.GetCode())) {
        CommitDupCacheEntry(args.SessionId, args.RequestId);
        response->Record = std::move(args.Response);
    }

    CompleteResponse<TEvService::TCreateHandleMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
