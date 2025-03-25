#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ReplayOpLog(
    const NActors::TActorContext& ctx,
    const TVector<NProto::TOpLogEntry>& opLog)
{
    // TODO(#1350): requests to shards should be ordered, otherwise weird
    // races are possible, e.g. create + unlink ops for the same node can arrive
    // in opposite order (original requests won't come in opposite order, but
    // if one of those requests gets rejected and is retried, the final order
    // may change)
    for (const auto& op: opLog) {
        if (op.HasCreateNodeRequest()) {
            RegisterCreateNodeInShardActor(
                ctx,
                nullptr, // requestInfo
                op.GetCreateNodeRequest(),
                op.GetRequestId(), // needed for DupCache update (only for
                                   // shard requests originating from
                                   // CreateNode requests)
                op.GetEntryId(),
                {} // response
            );
        } else if (op.HasUnlinkNodeRequest()) {
            // This case is kept for backward compatibility with old oplog
            // entries
            NProtoPrivate::TUnlinkNodeInShardRequest request;
            request.MutableHeaders()->CopyFrom(
                op.GetUnlinkNodeRequest().GetHeaders());
            request.SetFileSystemId(
                op.GetUnlinkNodeRequest().GetFileSystemId());
            request.SetNodeId(op.GetUnlinkNodeRequest().GetNodeId());
            request.SetName(op.GetUnlinkNodeRequest().GetName());
            request.SetUnlinkDirectory(
                op.GetUnlinkNodeRequest().GetUnlinkDirectory());

            RegisterUnlinkNodeInShardActor(
                ctx,
                nullptr,   // requestInfo
                std::move(request),
                0,   // requestId
                op.GetEntryId(),
                {},     // result
                false   // shouldUnlockUponCompletion
            );
        } else if (op.HasUnlinkNodeInShardRequest()) {
            bool shouldUnlockUponCompletion =
                op.GetUnlinkNodeInShardRequest().GetUnlinkDirectory() &&
                GetFileSystem().GetDirectoryCreationInShardsEnabled();
            if (shouldUnlockUponCompletion) {
                // There is a need to unlock the node ref after the operation is
                // completed, because the node ref should have been locked
                const auto& originalRequest =
                    op.GetUnlinkNodeInShardRequest().GetOriginalRequest();
                const bool locked = TryLockNodeRef(
                    {originalRequest.GetNodeId(), originalRequest.GetName()});
                if (!locked) {
                    ReportFailedToLockNodeRef(
                        TStringBuilder()
                        << "Request: " << op.ShortUtf8DebugString());
                }
            }

            RegisterUnlinkNodeInShardActor(
                ctx,
                nullptr,   // requestInfo
                op.GetUnlinkNodeInShardRequest(),
                0,   // requestId
                op.GetEntryId(),
                {},   // result
                shouldUnlockUponCompletion);
        } else if (op.HasRenameNodeInDestinationRequest()) {
            const auto& request = op.GetRenameNodeInDestinationRequest();
            const bool locked = TryLockNodeRef({
                request.GetOriginalRequest().GetNodeId(),
                request.GetOriginalRequest().GetName()});
            if (!locked) {
                ReportFailedToLockNodeRef(TStringBuilder() << "Request: "
                    << request.GetOriginalRequest().ShortUtf8DebugString());
            }
            RegisterRenameNodeInDestinationActor(
                ctx,
                nullptr, // requestInfo
                request,
                0, // requestId (TODO(#2674): either idempotency or use real
                   // requestId)
                op.GetEntryId());
        } else {
            const TString message = ReportUnknownOpLogEntry(
                TStringBuilder() << "OpLogEntry: " << op.DebugString().Quote());

            LOG_ERROR(
                ctx,
                TFileStoreComponents::TABLET,
                "%s %s",
                LogTag.c_str(),
                message.c_str());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    db.DeleteOpLogEntry(args.EntryId);
}

void TIndexTabletActor::CompleteTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DeleteOpLogEntry completed (%lu)",
        LogTag.c_str(),
        args.EntryId);
}

}   // namespace NCloud::NFileStore::NStorage
