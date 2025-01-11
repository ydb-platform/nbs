#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_PrepareRenameNodeInSource(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TPrepareRenameNodeInSource& args)
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

    if (args.ChildRef->ShardId.empty()) {
        auto message = ReportRenameNodeRequestForLocalNode(TStringBuilder()
            << "PrepareRenameNodeInSource: "
            << args.Request.ShortDebugString());
        args.Error = MakeError(E_ARGUMENT, std::move(message));
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_PrepareRenameNodeInSource(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TPrepareRenameNodeInSource& args)
{
    FILESTORE_VALIDATE_TX_ERROR(RenameNode, args);
    if (args.Error.GetCode() == S_ALREADY) {
        return; // nothing to do
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "RenameNode");
    }

    // TODO(#2674): lock NodeRef for any modifications

    // OpLogEntryId doesn't have to be a CommitId - it's just convenient
    // to use CommitId here in order not to generate some other unique
    // ui64
    args.OpLogEntry.SetEntryId(args.CommitId);
    auto* shardRequest =
        args.OpLogEntry.MutableRenameNodeInDestinationRequest();
    shardRequest->MutableHeaders()->CopyFrom(
        args.Request.GetHeaders());
    shardRequest->SetFileSystemId(args.NewParentShardId);
    shardRequest->SetNewParentId(args.Request.GetNewParentId());
    shardRequest->SetNewName(args.Request.GetNewName());
    shardRequest->SetFlags(args.Request.GetFlags());
    shardRequest->SetSourceNodeShardId(args.ChildRef->ShardId);
    shardRequest->SetSourceNodeShardNodeName(args.ChildRef->ShardNodeName);
    shardRequest->SetSourceNodeShardNodeName(args.ChildRef->ShardNodeName);

    db.WriteOpLogEntry(args.OpLogEntry);

    // update old parent timestamps
    auto parent = CopyAttrs(args.ParentNode->Attrs, E_CM_CMTIME);
    UpdateNode(
        db,
        args.ParentNode->NodeId,
        args.ParentNode->MinCommitId,
        args.CommitId,
        parent,
        args.ParentNode->Attrs);

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

void TIndexTabletActor::CompleteTx_PrepareRenameNodeInSource(
    const TActorContext& ctx,
    TTxIndexTablet::TPrepareRenameNodeInSource& args)
{
    InvalidateNodeCaches(args.ParentNodeId);
    if (args.ChildRef) {
        InvalidateNodeCaches(args.ChildRef->ChildNodeId);
    }
    if (args.ParentNode) {
        InvalidateNodeCaches(args.ParentNode->NodeId);
    }

    if (!HasError(args.Error) && !args.ChildRef) {
        auto message = ReportChildRefIsNull(TStringBuilder()
            << "RenameNode: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
    }

    RemoveTransaction(*args.RequestInfo);

    if (!HasError(args.Error)) {
        auto& op = args.OpLogEntry;

        if (!op.HasRenameNodeInDestinationRequest()) {
            auto message = ReportNoRenameNodeInDestinationRequest(
                TStringBuilder() << "RenameNode: "
                    << args.Request.ShortDebugString());
            args.Error = MakeError(E_INVALID_STATE, std::move(message));
        } else {
            auto& shardRequest = *op.MutableRenameNodeInDestinationRequest();
            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s Renaming node in dst shard upon RenameNode: %s, %lu",
                LogTag.c_str(),
                shardRequest.GetFileSystemId().c_str(),
                shardRequest.GetNewParentId());

            RegisterRenameNodeInDestinationActor(
                ctx,
                args.RequestInfo,
                std::move(shardRequest),
                args.RequestId,
                args.OpLogEntry.GetEntryId());
            return;
        }
    }

    Metrics.RenameNode.Update(
        1,
        0,
        ctx.Now() - args.RequestInfo->StartedTs);

    auto response = std::make_unique<TEvService::TEvRenameNodeResponse>(args.Error);
    CompleteResponse<TEvService::TRenameNodeMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
