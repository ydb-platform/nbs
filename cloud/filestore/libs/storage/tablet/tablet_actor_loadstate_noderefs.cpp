#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_LoadNodeRefs(
    const TActorContext& ctx,
    TTxIndexTablet::TLoadNodeRefs& args)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadingNodeRefs (nodeId: %lu, name: %s, maxNodeRefs: %lu)",
        LogTag.c_str(),
        args.NodeId,
        args.Cookie.c_str(),
        args.MaxNodeRefs);
    return true;
}

bool TIndexTabletActor::PrepareTx_LoadNodeRefs(
    const TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TLoadNodeRefs& args)
{
    TVector<TIndexTabletDatabase::TNodeRef> nodeRefs;

    if (!PrechargeNodeRefs(
            db,
            args.NodeId,
            args.Cookie,
            args.MaxNodeRefs,
            Max<ui64>()))
    {
        return false;   // not ready
    }

    bool ready = db.ReadNodeRefs(
        args.NodeId,
        args.Cookie,
        args.MaxNodeRefs,
        nodeRefs,
        args.NextNodeId,
        args.NextCookie);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadingNodeRefs (nodeId: %lu, name: %s, maxNodeRefs: %lu), read "
        "%lu nodeRefs: %s",
        LogTag.c_str(),
        args.NodeId,
        args.Cookie.c_str(),
        args.MaxNodeRefs,
        nodeRefs.size(),
        ready ? "finished" : "restarted");

    return ready;
}

void TIndexTabletActor::CompleteTx_LoadNodeRefs(
    const TActorContext& ctx,
    TTxIndexTablet::TLoadNodeRefs& args)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodeRefs iteration completed, next nodeId: %lu, next cookie: "
        "%s",
        LogTag.c_str(),
        args.NextNodeId,
        args.NextCookie.c_str());

    if (args.NextCookie || args.NextNodeId) {
        ctx.Schedule(
            args.SchedulePeriod,
            new TEvIndexTabletPrivate::TEvLoadNodeRefsRequest(
                args.NextNodeId,
                args.NextCookie,
                args.MaxNodeRefs,
                args.SchedulePeriod));
    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s LoadNodeRefs completed",
            LogTag.c_str());

        MarkNodeRefsLoadComplete();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleLoadNodeRefsRequest(
    const TEvIndexTabletPrivate::TEvLoadNodeRefsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodeRefs iteration started (nodeId: %lu, name: %s, "
        "maxNodeRefs: %lu)",
        LogTag.c_str(),
        msg->NodeId,
        msg->Cookie.c_str(),
        msg->MaxNodeRefs);

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    ExecuteTx<TLoadNodeRefs>(
        ctx,
        std::move(requestInfo),
        msg->NodeId,
        msg->Cookie,
        msg->MaxNodeRefs,
        msg->SchedulePeriod);
}

}   // namespace NCloud::NFileStore::NStorage
