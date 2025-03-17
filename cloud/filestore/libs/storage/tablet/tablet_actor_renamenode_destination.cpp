#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(
    const NProtoPrivate::TRenameNodeInDestinationRequest& request)
{
    Y_UNUSED(request);

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleRenameNodeInDestination(
    const TEvIndexTablet::TEvRenameNodeInDestinationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TMethod = TEvIndexTablet::TRenameNodeInDestinationMethod;
    auto* session = AcceptRequest<TMethod>(ev, ctx, ValidateRequest);
    if (!session) {
        return;
    }

    auto* msg = ev->Get();
    // TODO(#2674): DupCache
    // DupCache is necessary not only to implement the non-idempotent checks
    // but to prevent the following race:
    // 1. source directory shard sends RenameNodeInDestinationRequest
    // 2. the request is successfully processed
    // 3. network connectivity breaks, source directory shard gets E_REJECTED
    // 4. meanwhile <NewParentNodeId, NewChildName> pair gets unlinked in the
    //  destination shard and then a completely new file is created under the
    //  same name in NewParentId
    // 5. source directory shard retries the RenameNodeInDestinationRequest
    //  leading to the deletion of the file created in p.4 and the
    //  <NewParentNodeId, NewChildName> now points to the file unlinked in p.4
    //  So both the new and the old file are now deleted plus we have a NodeRef
    //  pointing to a deleted file
    //
    //  A more reliable way to prevent such a race is to lock the affected
    //  NodeRef in destination shard and unlock it after
    //  CommitRenameNodeInSource
    /*
    const auto requestId = GetRequestId(msg->Record);
    if (const auto* e = session->LookupDupEntry(requestId)) {
        auto response = std::make_unique<TMethod::TResponse>();
        if (GetDupCacheEntry(e, response->Record)) {
            return NCloud::Reply(ctx, *ev, std::move(response));
        }

        session->DropDupEntry(requestId);
    }
    */

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TMethod>(*requestInfo);

    ExecuteTx<TRenameNodeInDestination>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_RenameNodeInDestination(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRenameNodeInDestination& args)
{
    Y_UNUSED(ctx);

    // TODO(#2674): DupCache
    // FILESTORE_VALIDATE_DUPTX_SESSION(RenameNode, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

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
            if (args.NewChildRef->ShardId == args.Request.GetSourceNodeShardId()
                    && args.NewChildRef->ShardNodeName
                    == args.Request.GetSourceNodeShardNodeName())
            {
                // just an extra precaution for the case of DupCache-related
                // bugs
                args.Error = MakeError(S_ALREADY);
            } else {
                args.Error = ErrorAlreadyExists(args.NewName);
            }
            return true;
        }

        if (!args.NewChildRef->IsExternal()) {
            auto message = ReportRenameNodeRequestForLocalNode(TStringBuilder()
                << "RenameNodeInDestination: "
                << args.Request.ShortDebugString());
            args.Error = MakeError(E_ARGUMENT, std::move(message));
            return true;
        }

        // oldpath and newpath are existing hard links to the same file, then
        // rename() does nothing
        const bool isSameExternalNode =
            args.Request.GetSourceNodeShardId() == args.NewChildRef->ShardId
            && args.Request.GetSourceNodeShardNodeName()
                == args.NewChildRef->ShardNodeName;
        if (isSameExternalNode) {
            args.Error = MakeError(S_ALREADY, "is the same file");
            return true;
        }

        // EXCHANGE allows to rename any nodes
        if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
            return true;
        }

        // TODO(#2674): implement this check - will need to make a GetNodeAttr
        // call to find out node types
        // oldpath directory: newpath must either not exist, or it must specify
        // an empty directory.
    } else if (HasFlag(args.Flags, NProto::TRenameNodeRequest::F_EXCHANGE)) {
        args.Error = ErrorInvalidTarget(args.NewParentNodeId, args.NewName);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_RenameNodeInDestination(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TRenameNodeInDestination& args)
{
    FILESTORE_VALIDATE_TX_ERROR(RenameNodeInDestination, args);
    if (args.Error.GetCode() == S_ALREADY) {
        return; // nothing to do
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "RenameNodeInDestination");
    }

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

            args.Response.SetOldTargetNodeShardId(args.NewChildRef->ShardId);
            args.Response.SetOldTargetNodeShardNodeName(
                args.NewChildRef->ShardNodeName);
        } else if (!args.NewChildRef->IsExternal()) {
            auto message = ReportUnexpectedLocalNode(TStringBuilder()
                << "RenameNodeInDestination: "
                << args.Request.ShortDebugString());
            // here the code is E_INVALID_STATE since we should've checked this
            // thing before - in HandleRenameNodeInDestination
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
            return;
        } else {
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

            db.WriteOpLogEntry(args.OpLogEntry);
        }
    }

    // create target ref to source node
    CreateNodeRef(
        db,
        args.NewParentNodeId,
        args.CommitId,
        args.NewName,
        InvalidNodeId, // ChildNodeId
        args.Request.GetSourceNodeShardId(),
        args.Request.GetSourceNodeShardNodeName());

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

    // TODO(#2674): DupCache
    /*
    AddDupCacheEntry(
        db,
        session,
        args.RequestId,
        NProto::TRenameNodeResponse{},
        Config->GetDupCacheEntryCount());
        */

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_RenameNodeInDestination(
    const TActorContext& ctx,
    TTxIndexTablet::TRenameNodeInDestination& args)
{
    InvalidateNodeCaches(args.NewParentNodeId);
    if (args.NewChildRef) {
        InvalidateNodeCaches(args.NewChildRef->ChildNodeId);
    }
    if (args.NewParentNode) {
        InvalidateNodeCaches(args.NewParentNode->NodeId);
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
                args.RequestId,
                args.OpLogEntry.GetEntryId(),
                std::move(args.Response),
                // TODO(debnatkh): reconsider following line
                false);

            return;
        }

        CommitDupCacheEntry(args.SessionId, args.RequestId);

        // TODO(#1350): support session events for external nodes
    }

    RemoveTransaction(*args.RequestInfo);

    Metrics.RenameNode.Update(
        1,
        0,
        ctx.Now() - args.RequestInfo->StartedTs);

    using TMethod = TEvIndexTablet::TRenameNodeInDestinationMethod;
    auto response = std::make_unique<TMethod::TResponse>(args.Error);
    CompleteResponse<TMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
