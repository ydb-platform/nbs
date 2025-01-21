#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_LoadNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TLoadNodes& args)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadingNodes (nodeId: %lu, maxNodes: %lu)",
        LogTag.c_str(),
        args.NodeId,
        args.MaxNodes);
    return true;
}

bool TIndexTabletActor::PrepareTx_LoadNodes(
    const TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TLoadNodes& args)
{
    Y_UNUSED(ctx, db, args);
    TVector<TIndexTabletDatabase::TNode> nodes;

    bool ready =
        db.ReadNodes(args.NodeId, args.MaxNodes, args.NextNodeId, nodes);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadingNodes (nodeId: %lu, maxNodes: %lu), read %lu nodes: %s",
        LogTag.c_str(),
        args.NodeId,
        args.MaxNodes,
        nodes.size(),
        ready ? "finished" : "restarted");

    return ready;
}

void TIndexTabletActor::CompleteTx_LoadNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TLoadNodes& args)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodes iteration completed, next nodeId: %lu",
        LogTag.c_str(),
        args.NextNodeId);

    if (args.NextNodeId) {
        ctx.Send(
            SelfId(),
            new TEvIndexTabletPrivate::TEvLoadNodesRequest(
                args.NextNodeId,
                args.MaxNodes,
                args.ScheduleTimeout));
    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s LoadNodes completed",
            LogTag.c_str());
    }
}

// ////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleLoadNodesRequest(
    const TEvIndexTabletPrivate::TEvLoadNodesRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodes iteration started (nodeId: %lu, maxNodes: %lu)",
        LogTag.c_str(),
        msg->NodeId,
        msg->MaxNodes);

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    ExecuteTx<TLoadNodes>(
        ctx,
        std::move(requestInfo),
        msg->NodeId,
        msg->MaxNodes,
        msg->ScheduleTimeout);
}

}   // namespace NCloud::NFileStore::NStorage
