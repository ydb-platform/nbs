#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::DestroyDeferredNodes(const TActorContext& ctx)
{
    if (IsInUnconfirmedCreateHandleGracePeriod(ctx)) {
        return;
    }

    if (HasError(IsDataOperationAllowed())) {
        return;
    }

    auto nodeIds = GetDeferredNodeDestructionIds(
        Config->GetMaxDeferredNodeDestructionsPerTx());
    if (nodeIds.empty()) {
        return;
    }

    ExecuteTx<TDestroyDeferredNodes>(ctx, std::move(nodeIds));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DestroyDeferredNodes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroyDeferredNodes& args)
{
    Y_UNUSED(ctx);

    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);
    const ui64 commitId = GetCurrentCommitId();

    bool ready = true;
    args.Nodes.resize(args.NodeIds.size());
    for (ui32 i = 0; i < args.NodeIds.size(); ++i) {
        if (!ReadNode(*db, args.NodeIds[i], commitId, args.Nodes[i])) {
            ready = false;   // not ready
        }
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_DestroyDeferredNodes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDestroyDeferredNodes& args)
{
    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    const ui64 commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return ScheduleRebootTabletOnCommitIdOverflow(
            ctx,
            "DestroyDeferredNodes");
    }

    for (ui32 i = 0; i < args.NodeIds.size(); ++i) {
        const ui64 nodeId = args.NodeIds[i];
        const auto& node = args.Nodes[i];

        // The node is owned by the regular destruction paths from now on: it is
        // either already destroyed, or referenced again, or opened by a handle
        // that has been confirmed in the meantime.
        RemoveDeferredNodeDestruction(*db, nodeId);

        if (!node) {
            continue;
        }

        if (node->Attrs.GetLinks() || HasOpenHandles(nodeId)) {
            ++args.CancelledNodeCount;
            continue;
        }

        auto e = RemoveNode(*db, *node, node->MinCommitId, commitId);
        if (HasError(e)) {
            WriteOrphanNode(*db, TStringBuilder()
                << "DestroyDeferredNodes: RemoveNode: " << nodeId
                << ", Error: " << FormatError(e), nodeId);
            continue;
        }

        ++args.DestroyedNodeCount;
    }

    EnqueueTruncateIfNeeded(ctx);
}

void TIndexTabletActor::CompleteTx_DestroyDeferredNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TDestroyDeferredNodes& args)
{
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Destroyed %lu deferred nodes, %lu nodes are still in use,"
        " %lu nodes left in the queue",
        LogTag.c_str(),
        args.DestroyedNodeCount,
        args.CancelledNodeCount,
        GetDeferredNodeDestructionCount());

    Metrics->DeferredNodeDestructionsCompleted.fetch_add(
        args.DestroyedNodeCount,
        std::memory_order_relaxed);
    Metrics->DeferredNodeDestructionsCancelled.fetch_add(
        args.CancelledNodeCount,
        std::memory_order_relaxed);

    for (const ui64 nodeId: args.NodeIds) {
        InvalidateReadAheadCache(nodeId);
    }

    EnqueueBlobIndexOpIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
