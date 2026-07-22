#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::IsAsyncCreateHandleRecoveryWindowActive(
    const TActorContext& ctx) const
{
    return AsyncCreateHandleRecoveryDeadline.GetValue() &&
        ctx.Now() < AsyncCreateHandleRecoveryDeadline;
}

void TIndexTabletActor::ScheduleDeferredZeroLinkNodesCleanup(
    const TActorContext& ctx)
{
    if (!AsyncCreateHandleRecoveryDeadline.GetValue() ||
        DeferredZeroLinkNodesCleanupScheduled)
    {
        return;
    }

    const auto delay = AsyncCreateHandleRecoveryDeadline > ctx.Now()
        ? AsyncCreateHandleRecoveryDeadline - ctx.Now()
        : TDuration::Zero();
    ctx.Schedule(
        delay,
        new TEvIndexTabletPrivate::TEvCleanupDeferredZeroLinkNodes());
    DeferredZeroLinkNodesCleanupScheduled = true;
}

void TIndexTabletActor::HandleCleanupDeferredZeroLinkNodes(
    const TEvIndexTabletPrivate::TEvCleanupDeferredZeroLinkNodes::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    DeferredZeroLinkNodesCleanupScheduled = false;
    ExecuteTx<TCleanupDeferredZeroLinkNodes>(ctx, GetOrphanNodeIds());
}

bool TIndexTabletActor::PrepareTx_CleanupDeferredZeroLinkNodes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCleanupDeferredZeroLinkNodes& args)
{
    Y_UNUSED(ctx);

    auto db = CreateIndexTabletDatabase(tx.DB);
    args.Nodes.resize(args.NodeIds.size());
    const auto commitId = GetCurrentCommitId();
    for (size_t i = 0; i < args.NodeIds.size(); ++i) {
        if (!ReadNode(*db, args.NodeIds[i], commitId, args.Nodes[i])) {
            return false;
        }
    }
    return true;
}

void TIndexTabletActor::ExecuteTx_CleanupDeferredZeroLinkNodes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCleanupDeferredZeroLinkNodes& args)
{
    auto db = CreateIndexTabletDatabase(tx.DB);

    bool needCommitId = false;
    for (const auto& node: args.Nodes) {
        needCommitId = needCommitId ||
            (node && node->Attrs.GetLinks() == 0 &&
             !HasOpenHandles(node->NodeId));
    }

    ui64 commitId = GetCurrentCommitId();
    if (needCommitId) {
        commitId = GenerateCommitId();
        if (commitId == InvalidCommitId) {
            return;
        }
    }

    for (size_t i = 0; i < args.NodeIds.size(); ++i) {
        const auto nodeId = args.NodeIds[i];
        const auto& node = args.Nodes[i];

        if (!node) {
            DeleteOrphanNode(*db, nodeId);
            continue;
        }

        if (node->Attrs.GetLinks() == 0 && !HasOpenHandles(nodeId)) {
            auto e = RemoveNode(*db, *node, node->MinCommitId, commitId);
            if (HasError(e)) {
                // Retain the record so the next tablet incarnation retries it.
                WriteOrphanNode(
                    *db,
                    TStringBuilder()
                        << "CleanupDeferredZeroLinkNodes: RemoveNode: "
                        << nodeId << ", Error: " << FormatError(e),
                    nodeId);
                continue;
            }

            DeleteOrphanNode(*db, nodeId);
            ++args.Cleaned;
            args.RemovedNodes = true;
            continue;
        }

        if (node->Attrs.GetLinks() == 0) {
            ++args.Skipped;
        } else {
            ReportGeneratedOrphanNode(TStringBuilder()
                << "CleanupDeferredZeroLinkNodes: node " << nodeId
                << " has " << node->Attrs.GetLinks() << " links");
        }
        DeleteOrphanNode(*db, nodeId);
    }

    if (args.RemovedNodes) {
        EnqueueBlobIndexOpIfNeeded(ctx);
        EnqueueTruncateIfNeeded(ctx);
    }
}

void TIndexTabletActor::CompleteTx_CleanupDeferredZeroLinkNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TCleanupDeferredZeroLinkNodes& args)
{
    Y_UNUSED(ctx);

    Metrics.DeferredZeroLinkNodesCleaned.fetch_add(
        args.Cleaned,
        std::memory_order_relaxed);
    Metrics.DeferredZeroLinkNodesSkipped.fetch_add(
        args.Skipped,
        std::memory_order_relaxed);
}

}   // namespace NCloud::NFileStore::NStorage
