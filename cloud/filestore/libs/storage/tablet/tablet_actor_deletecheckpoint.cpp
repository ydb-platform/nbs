#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

constexpr size_t RemoveCheckpointNodesBatchSize = 100;
constexpr size_t RemoveCheckpointBlobsBatchSize = 100;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDeleteCheckpoint(
    const TEvIndexTabletPrivate::TEvDeleteCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvIndexTabletPrivate::TEvDeleteCheckpointResponse>(
                std::move(error)));

        return;
    }

    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DeleteCheckpoint started (checkpointId: %s, mode: %s)",
        LogTag.c_str(),
        msg->CheckpointId.c_str(),
        ToString(msg->Mode).c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTabletPrivate::TDeleteCheckpointMethod>(*requestInfo);

    ExecuteTx<TDeleteCheckpoint>(
        ctx,
        std::move(requestInfo),
        msg->CheckpointId,
        msg->Mode,
        GetCurrentCommitId());
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteCheckpoint& args)
{
    Y_UNUSED(ctx);

    auto* checkpoint = FindCheckpoint(args.CheckpointId);
    if (!checkpoint) {
        args.Error = ErrorInvalidCheckpoint(args.CheckpointId);
        return true;
    }

    args.CommitId = checkpoint->GetCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    bool ready = true;
    switch (args.Mode) {
        case EDeleteCheckpointMode::MarkCheckpointDeleted:
            // nothing to do
            break;

        case EDeleteCheckpointMode::RemoveCheckpointNodes: {
            ready = db.ReadCheckpointNodes(
                args.CommitId,
                args.NodeIds,
                RemoveCheckpointNodesBatchSize);

            if (ready) {
                for (ui64 nodeId: args.NodeIds) {
                    TMaybe<IIndexTabletDatabase::TNode> node;
                    if (!db.ReadNodeVer(nodeId, args.CommitId, node)) {
                        ready = false;
                    }

                    if (ready && node) {
                        args.Nodes.emplace_back(node.GetRef());
                    }

                    if (!db.ReadNodeAttrVers(nodeId, args.CommitId, args.NodeAttrs)) {
                        ready = false;
                    }

                    if (!db.ReadNodeRefVers(nodeId, args.CommitId, args.NodeRefs)) {
                        ready = false;
                    }
                }
            }
            break;
        }

        case EDeleteCheckpointMode::RemoveCheckpointBlobs: {
            ready = db.ReadCheckpointBlobs(
                args.CommitId,
                args.Blobs,
                RemoveCheckpointBlobsBatchSize);

            if (ready) {
                for (const auto& blob: args.Blobs) {
                    if (!args.MixedBlocksRanges.count(blob.RangeId)) {
                        if (!LoadMixedBlocks(db, blob.RangeId)) {
                            ready = false;
                        } else {
                            args.MixedBlocksRanges.insert(blob.RangeId);
                        }
                    }

                    TMaybe<IIndexTabletDatabase::TMixedBlob> mixedBlob;
                    if (!db.ReadMixedBlocks(
                            blob.RangeId,
                            blob.BlobId,
                            mixedBlob,
                            GetAllocator(EAllocatorTag::BlockList)))
                    {
                        ready = false;
                    }

                    if (ready) {
                        TABLET_VERIFY(mixedBlob);
                        args.MixedBlobs.emplace_back(std::move(mixedBlob.GetRef()));
                    }
                }
            }
            break;
        }

        case EDeleteCheckpointMode::RemoveCheckpoint:
            // nothing to do
            break;
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_DeleteCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteCheckpoint& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_ERROR(DeleteCheckpoint, args);

    // needed to prevent the blobs updated during this tx from being deleted
    // before this tx completes
    AcquireCollectBarrier(args.CollectBarrier);

    auto* checkpoint = FindCheckpoint(args.CheckpointId);
    TABLET_VERIFY(checkpoint);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    switch (args.Mode) {
        case EDeleteCheckpointMode::MarkCheckpointDeleted:
            MarkCheckpointDeleted(db, checkpoint);
            break;

        case EDeleteCheckpointMode::RemoveCheckpointNodes: {
            if (!args.NodeIds) {
                args.Error = MakeError(S_FALSE, "no more nodes");
                return;
            }

            for (const auto& node: args.Nodes) {
                RewriteNode(
                    db,
                    node.NodeId,
                    node.MinCommitId,
                    node.MaxCommitId,
                    node.Attrs);
            }

            for (const auto& attr: args.NodeAttrs) {
                RewriteNodeAttr(
                    db,
                    attr.NodeId,
                    attr.MinCommitId,
                    attr.MaxCommitId,
                    attr);
            }

            for (const auto& ref: args.NodeRefs) {
                RewriteNodeRef(
                    db,
                    ref.NodeId,
                    ref.MinCommitId,
                    ref.MaxCommitId,
                    ref.Name,
                    ref.ChildNodeId,
                    ref.ShardId,
                    ref.ShardNodeName);
            }

            RemoveCheckpointNodes(db, checkpoint, args.NodeIds);
            break;
        }

        case EDeleteCheckpointMode::RemoveCheckpointBlobs: {
            if (!args.Blobs) {
                args.Error = MakeError(S_FALSE, "no more blobs");
                return;
            }

            TABLET_VERIFY(args.Blobs.size() == args.MixedBlobs.size());
            for (size_t i = 0; i < args.Blobs.size(); ++i) {
                TABLET_VERIFY(args.Blobs[i].BlobId == args.MixedBlobs[i].BlobId);

                const auto rangeId = args.Blobs[i].RangeId;

                TMixedBlobMeta blob {
                    args.MixedBlobs[i].BlobId,
                    args.MixedBlobs[i].BlockList.DecodeBlocks(),
                };

                TMixedBlobStats stats {
                    args.MixedBlobs[i].GarbageBlocks,
                    args.MixedBlobs[i].CheckpointBlocks,
                };

                RewriteMixedBlocks(db, rangeId, blob, stats);
                RemoveCheckpointBlob(db, checkpoint, rangeId, blob.BlobId);
            }
            break;
        }

        case EDeleteCheckpointMode::RemoveCheckpoint:
            RemoveCheckpoint(db, checkpoint);
            break;
    }
}

void TIndexTabletActor::CompleteTx_DeleteCheckpoint(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteCheckpoint& args)
{
    // TODO(#1146) checkpoint-related tables are not yet supported
    RemoveTransaction(*args.RequestInfo);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DeleteCheckpoint completed (%s)",
        LogTag.c_str(),
        FormatError(args.Error).c_str());

    ReleaseMixedBlocks(args.MixedBlocksRanges);
    TABLET_VERIFY(TryReleaseCollectBarrier(args.CollectBarrier));

    using TResponse = TEvIndexTabletPrivate::TEvDeleteCheckpointResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
