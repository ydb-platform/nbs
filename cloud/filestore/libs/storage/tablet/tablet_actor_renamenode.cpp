#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TRenameNodeRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId
            || request.GetNewParentId() == InvalidNodeId)
    {
        return ErrorInvalidArgument();
    }

    // either part
    if (auto error = ValidateNodeName(request.GetName()); HasError(error)) {
        return error;
    }

    if (auto error = ValidateNodeName(request.GetNewName()); HasError(error)) {
        return error;
    }

    if (HasFlag(request.GetFlags(), NProto::TRenameNodeRequest::F_EXCHANGE) &&
        HasFlag(request.GetFlags(), NProto::TRenameNodeRequest::F_NOREPLACE))
    {
        return ErrorInvalidArgument();
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleRenameNode(
    const TEvService::TEvRenameNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* session =
        AcceptRequest<TEvService::TRenameNodeMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();
    if (const auto* e = session->LookupDupEntry(GetRequestId(msg->Record))) {
        auto response = std::make_unique<TEvService::TEvRenameNodeResponse>();
        GetDupCacheEntry(e, response->Record);
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TRenameNodeMethod>(*requestInfo);

    ExecuteTx<TRenameNode>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_RenameNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRenameNode& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(RenameNode, args);

    TIndexTabletDatabase db(tx.DB);

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
    TABLET_VERIFY(args.ParentNode);
    // validate old ref exists
    if (!ReadNodeRef(
            db,
            args.ParentNodeId,
            args.CommitId,
            args.Name,
            args.ChildRef))
    {
        return false;   // not ready
    }

    // read old node
    if (!args.ChildRef) {
        args.Error = ErrorInvalidTarget(args.ParentNodeId);
        return true;
    }

    if (args.ChildRef->FollowerId.Empty()) {
        if (!ReadNode(
                db,
                args.ChildRef->ChildNodeId,
                args.CommitId,
                args.ChildNode))
        {
            return false;
        }

        // TODO: AccessCheck
        TABLET_VERIFY(args.ChildNode);
    }

    // validate new parent node exists
    if (!ReadNode(
            db,
            args.NewParentNodeId,
            args.CommitId,
            args.NewParentNode))
    {
        return false;   // not ready
    }

    if (!args.NewParentNode) {
        args.Error = ErrorInvalidTarget(args.NewParentNodeId, args.NewName);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.NewParentNode);
    // check if new ref exists
    if (!ReadNodeRef(
            db,
            args.NewParentNodeId,
            args.CommitId,
            args.NewName,
            args.NewChildRef))
    {
        return false;   // not ready
    }

    if (args.NewChildRef) {
        if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_NOREPLACE)) {
            args.Error = ErrorAlreadyExists(args.NewName);
            return true;
        }

        if (args.NewChildRef->FollowerId.Empty()) {
            // read new child node to unlink it
            if (!ReadNode(
                    db,
                    args.NewChildRef->ChildNodeId,
                    args.CommitId,
                    args.NewChildNode))
            {
                return false;
            }

            // TODO: AccessCheck
            TABLET_VERIFY(args.NewChildNode);
        }

        // oldpath and newpath are existing hard links to the same file, then
        // rename() does nothing
        const bool isSameNode = args.ChildNode && args.NewChildNode
            && args.ChildNode->NodeId == args.NewChildNode->NodeId;
        const bool isSameExternalNode = args.ChildRef->FollowerId
            && args.NewChildRef->FollowerId
            && args.ChildRef->FollowerId == args.NewChildRef->FollowerId;
        if (isSameNode || isSameExternalNode) {
            args.Error = MakeError(S_ALREADY, "is the same file");
            return true;
        }

        // EXCHANGE allows to rename any nodes
        if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
            return true;
        }

        // oldpath directory: newpath must either not exist, or it must specify
        // an empty directory.
        if (args.ChildNode
                && args.ChildNode->Attrs.GetType() == NProto::E_DIRECTORY_NODE)
        {
            if (!args.NewChildNode || args.NewChildNode->Attrs.GetType()
                    != NProto::E_DIRECTORY_NODE)
            {
                args.Error = ErrorIsNotDirectory(args.NewChildNode->NodeId);
                return true;
            }
        }

        if (args.NewChildNode && args.NewChildNode->Attrs.GetType()
                == NProto::E_DIRECTORY_NODE)
        {
            if (!args.ChildNode || args.ChildNode->Attrs.GetType()
                    != NProto::E_DIRECTORY_NODE)
            {
                args.Error = ErrorIsDirectory(args.NewChildNode->NodeId);
                return true;
            }

            // 1 entry is enough to prevent rename
            TVector<IIndexTabletDatabase::TNodeRef> refs;
            if (!ReadNodeRefs(
                    db,
                    args.NewChildNode->NodeId,
                    args.CommitId,
                    {},
                    refs,
                    1))
            {
                return false;
            }

            if (!refs.empty()) {
                args.Error = ErrorIsNotEmpty(args.NewChildNode->NodeId);
                return true;
            }
        }
    } else if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
        args.Error = ErrorInvalidTarget(args.NewParentNodeId, args.NewName);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_RenameNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRenameNode& args)
{
    FILESTORE_VALIDATE_TX_ERROR(RenameNode, args);
    if (args.Error.GetCode() == S_ALREADY) {
        return; // nothing to do
    }

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "RenameNode");
    }

    // remove existing source ref
    RemoveNodeRef(
        db,
        args.ParentNodeId,
        args.ChildRef->MinCommitId,
        args.CommitId,
        args.Name,
        args.ChildRef->ChildNodeId,
        args.ChildRef->FollowerId,
        args.ChildRef->FollowerName);

    if (args.NewChildRef) {
        if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
            // remove existing target ref
            RemoveNodeRef(
                db,
                args.NewParentNodeId,
                args.NewChildRef->MinCommitId,
                args.CommitId,
                args.NewName,
                args.NewChildRef->NodeId,
                args.NewChildRef->FollowerId,
                args.NewChildRef->FollowerName);

            // create source ref to target node
            CreateNodeRef(
                db,
                args.ParentNodeId,
                args.CommitId,
                args.Name,
                args.NewChildRef->ChildNodeId,
                args.NewChildRef->FollowerId,
                args.NewChildRef->FollowerName);
        } else if (args.NewChildRef->FollowerId.Empty()) {
            TABLET_VERIFY(args.NewChildNode);

            // remove target ref and unlink target node
            UnlinkNode(
                db,
                args.NewParentNode->NodeId,
                args.NewName,
                *args.NewChildNode,
                args.NewChildRef->MinCommitId,
                args.CommitId);
        } else {
            // remove target ref
            UnlinkExternalNode(
                db,
                args.NewParentNode->NodeId,
                args.NewName,
                args.NewChildRef->FollowerId,
                args.NewChildRef->FollowerName,
                args.NewChildRef->MinCommitId,
                args.CommitId);

            args.FollowerIdForUnlink = args.NewChildRef->FollowerId;
            args.FollowerNameForUnlink = args.NewChildRef->FollowerName;
        }
    }

    // update old parent timestamps
    auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
    UpdateNode(
        db,
        args.ParentNode->NodeId,
        args.ParentNode->MinCommitId,
        args.CommitId,
        parent,
        args.ParentNode->Attrs);

    // create target ref to source node
    CreateNodeRef(
        db,
        args.NewParentNodeId,
        args.CommitId,
        args.NewName,
        args.ChildRef->ChildNodeId,
        args.ChildRef->FollowerId,
        args.ChildRef->FollowerName);

    auto newParent = CopyAttrs(args.NewParentNode->Attrs, E_CM_CMTIME);
    UpdateNode(
        db,
        args.NewParentNode->NodeId,
        args.NewParentNode->MinCommitId,
        args.CommitId,
        newParent,
        args.NewParentNode->Attrs);

    auto* session = FindSession(args.SessionId);
    TABLET_VERIFY(session);

    AddDupCacheEntry(
        db,
        session,
        args.RequestId,
        NProto::TRenameNodeResponse{},
        Config->GetDupCacheEntryCount());

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_RenameNode(
    const TActorContext& ctx,
    TTxIndexTablet::TRenameNode& args)
{
    RemoveTransaction(*args.RequestInfo);

    if (SUCCEEDED(args.Error.GetCode())) {
        TABLET_VERIFY(args.ChildRef);

        CommitDupCacheEntry(args.SessionId, args.RequestId);

        if (args.FollowerIdForUnlink) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s Unlinking node in follower upon RenameNode: %s, %s",
                LogTag.c_str(),
                args.FollowerIdForUnlink.c_str(),
                args.FollowerNameForUnlink.c_str());

            NProto::TUnlinkNodeRequest request;
            request.MutableHeaders()->CopyFrom(args.Headers);

            RegisterUnlinkNodeInFollowerActor(
                ctx,
                args.RequestInfo,
                std::move(args.FollowerIdForUnlink),
                std::move(args.FollowerNameForUnlink),
                std::move(request),
                std::move(args.Response));

            return;
        }

        // TODO(#1350): support session events for external nodes
        NProto::TSessionEvent sessionEvent;
        {
            auto* unlinked = sessionEvent.AddNodeUnlinked();
            unlinked->SetParentNodeId(args.ParentNodeId);
            unlinked->SetChildNodeId(args.ChildRef->ChildNodeId);
            unlinked->SetName(args.Name);
        }
        {
            auto* linked = sessionEvent.AddNodeLinked();
            linked->SetParentNodeId(args.NewParentNodeId);
            linked->SetChildNodeId(args.ChildRef->ChildNodeId);
            linked->SetName(args.NewName);
        }
        NotifySessionEvent(ctx, sessionEvent);
    }

    auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(args.Error);
    CompleteResponse<TEvService::TRenameNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
