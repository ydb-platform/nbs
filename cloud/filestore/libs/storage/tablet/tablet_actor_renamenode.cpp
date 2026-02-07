#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

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

NProto::TError TIndexTabletActor::HandleCrossShardRenameNodeImpl(
    TRequestInfoPtr& requestInfo,
    NProto::TRenameNodeRequest& record,
    NProto::TProfileLogRequestInfo profileLogRequest,
    const TActorContext& ctx)
{
    //
    // We have separate logic for cross-shard move ops, see the following link
    // for more details:
    // https://github.com/ydb-platform/nbs/issues/2674#issuecomment-2578785398
    //

    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    if (shardIds.empty()) {
        //
        // No shards => this RenameNode op is local.
        //

        return MakeError(S_FALSE);
    }

    const auto shardNo = GetFileSystem().GetShardNo();
    const auto parentShardNo = ExtractShardNo(record.GetNodeId());
    if (parentShardNo != shardNo) {
        //
        // Each RenameNode op must be coordinated by the shard that's in charge
        // of the source node ref.
        //

        auto message = ReportRenameNodeRequestSentToWrongShard(
            TStringBuilder() << "RenameNode: "
                << record.ShortDebugString() << " received by shard "
                << shardNo << ", expected: " << parentShardNo);
        return MakeError(E_ARGUMENT, std::move(message));
    }

    const auto newParentShardNo = ExtractShardNo(record.GetNewParentId());
    if (newParentShardNo == GetFileSystem().GetShardNo()) {
        return MakeError(S_FALSE);
    }

    //
    // At this point we know that src and dst node refs are managed by different
    // shards.
    //

    if (newParentShardNo > static_cast<ui32>(shardIds.size())) {
        auto message = ReportInvalidShardNo(
            TStringBuilder() << "RenameNode: "
                << record.ShortDebugString() << " newParentShardNo"
                << ": " << newParentShardNo << ", shard count: "
                << shardIds.size());
        return MakeError(E_ARGUMENT, std::move(message));
    }

    if (newParentShardNo == 0 && record.GetNewParentId() != RootNodeId) {
        auto message = ReportInvalidShardNo(
            TStringBuilder() << "RenameNode: "
                << record.ShortDebugString() << " newParentShardNo"
                << ": " << newParentShardNo << ", NewParentId: "
                << record.GetNewParentId());
        return MakeError(E_ARGUMENT, std::move(message));
    }

    ExecuteTx<TPrepareRenameNodeInSource>(
        ctx,
        std::move(requestInfo),
        std::move(record),
        std::move(profileLogRequest),
        newParentShardNo
            ? shardIds[newParentShardNo - 1]
            : GetMainFileSystemId(),
        false /* isExplicitRequest */);

    return MakeError(S_OK);
}

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

    NProto::TProfileLogRequestInfo profileLogRequest;
    InitTabletProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreRequest::RenameNode,
        msg->Record,
        ctx.Now());

    auto onReply = [&] (const NProto::TError& error) {
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);
    };

    const auto requestId = GetRequestId(msg->Record);
    if (const auto* e = session->LookupDupEntry(requestId)) {
        auto response = std::make_unique<TEvService::TEvRenameNodeResponse>();
        if (GetDupCacheEntry(e, response->Record)) {
            NCloud::Reply(ctx, *ev, std::move(response));
            onReply(MakeError(S_ALREADY));
            return;
        }

        session->DropDupEntry(requestId);
    }

    if (!TryLockNodeRef({msg->Record.GetNodeId(), msg->Record.GetName()})) {
        auto error = MakeError(E_REJECTED, TStringBuilder() << "node ref "
            << msg->Record.GetNodeId() << " " << msg->Record.GetName()
            << " is locked for RenameNode");
        auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
            error);
        NCloud::Reply(ctx, *ev, std::move(response));
        onReply(error);
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvService::TRenameNodeMethod>(*requestInfo);

    auto error = HandleCrossShardRenameNodeImpl(
        requestInfo,
        msg->Record,
        profileLogRequest,
        ctx);
    if (HasError(error)) {
        RemoveInFlightRequest(*requestInfo);
        UnlockNodeRef({msg->Record.GetNodeId(), msg->Record.GetName()});
        auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
            error);
        NCloud::Reply(ctx, *ev, std::move(response));
        onReply(error);
        return;
    }

    if (error.GetCode() != S_FALSE) {
        //
        // Processed by HandleCrossShardRenameNodeImpl.
        //

        return;
    }

    //
    // NewParent is also managed by this shard, we need to lock the destination
    // NodeRef as well.
    //

    if (!TryLockNodeRef({
            msg->Record.GetNewParentId(),
            msg->Record.GetNewName()}))
    {
        UnlockNodeRef({msg->Record.GetNodeId(), msg->Record.GetName()});
        auto error = MakeError(E_REJECTED, TStringBuilder() << "new node ref "
            << msg->Record.GetNewParentId() << " " << msg->Record.GetNewName()
            << " is locked for RenameNode");
        auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(
            error);
        NCloud::Reply(ctx, *ev, std::move(response));
        onReply(error);
        return;
    }

    ExecuteTx<TRenameNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record),
        std::move(profileLogRequest));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDoRenameNode(
    const TEvIndexTabletPrivate::TEvDoRenameNode::TPtr& ev,
    const TActorContext& ctx)
{
    WorkerActors.erase(ev->Sender);

    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        UnlockNodeRef({
            msg->Request.GetOriginalRequest().GetNodeId(),
            msg->Request.GetOriginalRequest().GetName()});
        UnlockNodeRef({
            msg->Request.GetOriginalRequest().GetNewParentId(),
            msg->Request.GetOriginalRequest().GetNewName()});

        using TMethod = TEvService::TRenameNodeMethod;
        auto response =
            std::make_unique<TMethod::TResponse>(std::move(msg->Error));
        NCloud::Reply(ctx, *msg->RequestInfo, std::move(response));
        return;
    }

    ExecuteTx<TRenameNode>(
        ctx,
        std::move(msg->RequestInfo),
        std::move(*msg->Request.MutableOriginalRequest()),
        std::move(msg->ProfileLogRequest),
        std::move(msg->SourceNodeAttr),
        std::move(msg->DestinationNodeAttr),
        msg->OpLogEntryId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_RenameNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRenameNode& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_DUPTX_SESSION(RenameNode, args);

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

    if (!args.ChildRef->IsExternal()) {
        if (!ReadNode(
                db,
                args.ChildRef->ChildNodeId,
                args.CommitId,
                args.ChildNode))
        {
            return false;
        }

        // TODO: AccessCheck

        if (!args.ChildNode) {
            auto message = ReportChildNodeIsNull(TStringBuilder()
                << "RenameNode: " << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return true;
        }
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

    if (args.NewParentNode->Attrs.GetIsPreparedForUnlink()) {
        args.Error = ErrorIsPreparedForUnlink(args.NewParentNode->NodeId);
        return true;
    }

    // TODO: AccessCheck

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

        if (!args.NewChildRef->IsExternal()) {
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

            if (!args.NewChildNode) {
                auto message = ReportNewChildNodeIsNull(TStringBuilder()
                    << "RenameNode: " << args.Request.ShortDebugString());
                args.Error = MakeError(E_INVALID_STATE, std::move(message));
                return true;
            }
        }

        // oldpath and newpath are existing hard links to the same file, then
        // rename() does nothing
        const bool isSameNode = args.ChildNode && args.NewChildNode
            && args.ChildNode->NodeId == args.NewChildNode->NodeId;
        const bool isSameExternalNode = args.ChildRef->ShardId
            && args.NewChildRef->ShardId
            && args.ChildRef->ShardId == args.NewChildRef->ShardId
            && args.ChildRef->ShardNodeName == args.NewChildRef->ShardNodeName;
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

        bool childIsDir = false;
        bool newChildIsDir = false;
        bool newChildIsEmpty = false;
        ui64 newChildNodeId = InvalidNodeId;
        if (args.NewChildRef->IsExternal()) {
            if (args.IsSecondPass) {
                newChildNodeId = args.DestinationNodeAttr.GetId();
                childIsDir =
                    args.SourceNodeAttr.GetType() == NProto::E_DIRECTORY_NODE;
                newChildIsDir = args.DestinationNodeAttr.GetType()
                    == NProto::E_DIRECTORY_NODE;

                //
                // If we are here, it means that we have already successfully
                // prepared dst node for unlink which means that it's empty
                // and is going to stay empty.
                //

                newChildIsEmpty = true;
            } else {
                args.SecondPassRequired = true;
            }
        } else {
            newChildNodeId = args.NewChildNode->NodeId;
            childIsDir =
                args.ChildNode->Attrs.GetType() == NProto::E_DIRECTORY_NODE;
            newChildIsDir =
                args.NewChildNode->Attrs.GetType() == NProto::E_DIRECTORY_NODE;
            if (newChildIsDir) {
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

                newChildIsEmpty = refs.empty();
            }
        }

        if (newChildNodeId != InvalidNodeId && newChildIsDir) {
            if (!childIsDir) {
                args.Error = ErrorIsDirectory(newChildNodeId);
                return true;
            }

            if (!newChildIsEmpty) {
                args.Error = ErrorIsNotEmpty(newChildNodeId);
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

    if (args.SecondPassRequired) {
        return;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    // remove existing source ref
    RemoveNodeRef(
        db,
        args.ParentNodeId,
        args.ChildRef->MinCommitId,
        args.CommitId,
        args.Name,
        args.ChildRef->ChildNodeId,
        args.ChildRef->ShardId,
        args.ChildRef->ShardNodeName);

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
                args.NewChildRef->ShardId,
                args.NewChildRef->ShardNodeName);

            // create source ref to target node
            CreateNodeRef(
                db,
                args.ParentNodeId,
                args.CommitId,
                args.Name,
                args.NewChildRef->ChildNodeId,
                args.NewChildRef->ShardId,
                args.NewChildRef->ShardNodeName);
        } else if (!args.NewChildRef->IsExternal()) {
            if (!args.NewChildNode) {
                auto message = ReportNewChildNodeIsNull(TStringBuilder()
                    << "RenameNode: " << args.Request.ShortDebugString());
                args.Error = MakeError(E_INVALID_STATE, std::move(message));
                return;
            }

            // remove target ref and unlink target node
            auto e = UnlinkNode(
                db,
                args.NewParentNode->NodeId,
                args.NewName,
                *args.NewChildNode,
                args.NewChildRef->MinCommitId,
                args.CommitId,
                /* removeNodeRef */ true);

            if (HasError(e)) {
                const auto nodeId = args.NewChildNode->NodeId;
                WriteOrphanNode(db, TStringBuilder()
                    << "RenameNode: " << args.SessionId
                    << ", ParentNodeId: " << args.NewParentNode->NodeId
                    << ", NodeId: " << nodeId
                    << ", Error: " << FormatError(e), nodeId);
            }
        } else {
            if (args.AbortUnlinkOpLogEntryId) {
                db.DeleteOpLogEntry(args.AbortUnlinkOpLogEntryId);
            }

            // remove target ref
            UnlinkExternalNode(
                db,
                args.NewParentNode->NodeId,
                args.NewName,
                args.NewChildRef->ShardId,
                args.NewChildRef->ShardNodeName,
                args.NewChildRef->MinCommitId,
                args.CommitId);

            // OpLogEntryId doesn't have to be a CommitId - it's just convenient
            // to use CommitId here in order not to generate some other unique
            // ui64
            args.OpLogEntry.SetEntryId(args.CommitId);
            auto* shardRequest =
                args.OpLogEntry.MutableUnlinkNodeInShardRequest();
            shardRequest->MutableHeaders()->CopyFrom(
                args.Request.GetHeaders());
            shardRequest->SetFileSystemId(args.NewChildRef->ShardId);
            shardRequest->SetNodeId(RootNodeId);
            shardRequest->SetName(args.NewChildRef->ShardNodeName);
            shardRequest->SetUnlinkDirectory(
                args.DestinationNodeAttr.GetType() == NProto::E_DIRECTORY_NODE);

            db.WriteOpLogEntry(args.OpLogEntry);
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
        args.ChildRef->ShardId,
        args.ChildRef->ShardNodeName);

    auto newParent = CopyAttrs(args.NewParentNode->Attrs, E_CM_CMTIME);
    UpdateNode(
        db,
        args.NewParentNode->NodeId,
        args.NewParentNode->MinCommitId,
        args.CommitId,
        newParent,
        args.NewParentNode->Attrs);

    auto* session = FindSession(args.SessionId);
    if (!session) {
        auto message = ReportSessionNotFoundInTx(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return;
    }

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
    if (args.SecondPassRequired) {
        if (args.NewChildRef) {
            TABLET_VERIFY(args.ChildRef);
            RegisterGetNodeInfoAndPrepareUnlinkActor(
                ctx,
                args.RequestInfo,
                MakeRenameNodeInDestinationRequest(
                    args.Request,
                    args.ChildRef->ShardId,
                    args.ChildRef->ShardNodeName,
                    args.NewChildRef->ShardId),
                std::move(args.ProfileLogRequest),
                std::move(args.NewChildRef->ShardId),
                std::move(args.NewChildRef->ShardNodeName),
                true /* isLocalRename */);
            return;
        }

        auto message = ReportChildRefIsNull(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    if (HasError(args.Error)
            && args.IsSecondPass
            && args.DestinationNodeAttr.GetType() == NProto::E_DIRECTORY_NODE)
    {
        if (args.AbortUnlinkOpLogEntryId) {
            TABLET_VERIFY_C(args.ChildRef, args.Request.Utf8DebugString());
            TABLET_VERIFY(args.NewChildRef);
            RegisterAbortUnlinkDirectoryInShardActor(
                ctx,
                args.RequestInfo,
                MakeRenameNodeInDestinationRequest(
                    args.Request,
                    args.ChildRef->ShardId,
                    args.ChildRef->ShardNodeName,
                    args.NewChildRef->ShardId),
                std::move(args.ProfileLogRequest),
                args.Error,
                args.NewChildRef->ShardId,
                args.DestinationNodeAttr.GetId(),
                args.AbortUnlinkOpLogEntryId,
                true /* isLocalRename */);
            return;
        }

        auto message = ReportMissingOpLogEntryId(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString()
            << ", original error: " << FormatError(args.Error));
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    UnlockNodeRef({args.ParentNodeId, args.Name});
    UnlockNodeRef({args.NewParentNodeId, args.NewName});

    InvalidateNodeCaches(args.ParentNodeId);
    InvalidateNodeCaches(args.NewParentNodeId);
    if (args.ChildRef) {
        InvalidateNodeCaches(args.ChildRef->ChildNodeId);
    }
    if (args.NewChildRef) {
        InvalidateNodeCaches(args.NewChildRef->ChildNodeId);
    }
    if (args.ParentNode) {
        InvalidateNodeCaches(args.ParentNode->NodeId);
    }
    if (args.NewParentNode) {
        InvalidateNodeCaches(args.NewParentNode->NodeId);
    }

    if (!HasError(args.Error) && !args.ChildRef) {
        auto message = ReportChildRefIsNull(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    if (!HasError(args.Error)) {
        auto& op = args.OpLogEntry;
        if (op.HasUnlinkNodeInShardRequest()) {
            // rename + unlink is pretty rare so let's keep INFO level here
            LOG_INFO(
                ctx,
                TFileStoreComponents::TABLET,
                "%s Unlinking node in shard upon RenameNode: %s, %s",
                LogTag.c_str(),
                op.GetUnlinkNodeInShardRequest().GetFileSystemId().c_str(),
                op.GetUnlinkNodeInShardRequest().GetName().c_str());

            RegisterUnlinkNodeInShardActor(
                ctx,
                args.RequestInfo,
                std::move(*op.MutableUnlinkNodeInShardRequest()),
                std::move(args.ProfileLogRequest),
                args.RequestId,
                args.OpLogEntry.GetEntryId(),
                std::move(args.Response),
                false);

            return;
        }

        CommitDupCacheEntry(args.SessionId, args.RequestId);

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

    RemoveInFlightRequest(*args.RequestInfo);

    Metrics.RenameNode.Update(
        1,
        0,
        ctx.Now() - args.RequestInfo->StartedTs);

    auto response =
        std::make_unique<TEvService::TEvRenameNodeResponse>(args.Error);
    CompleteResponse<TEvService::TRenameNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        args.Error,
        ProfileLog);
}

}   // namespace NCloud::NFileStore::NStorage
