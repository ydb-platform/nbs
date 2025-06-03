#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_ListNodeRefs(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodeRefs& args)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ListingNodeRefs (StartNodeId: %lu, StartName: %s, Limit: %lu)",
        LogTag.c_str(),
        args.NodeId,
        args.Cookie.c_str(),
        args.Limit);
    return true;
}

bool TIndexTabletActor::PrepareTx_ListNodeRefs(
    const TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TListNodeRefs& args)
{
    TVector<TIndexTabletDatabase::TNodeRef> nodeRefs;

    bool ready = db.ReadNodeRefs(
        args.NodeId,
        args.Cookie,
        args.Limit,
        args.Refs,
        args.NextNodeId,
        args.NextCookie);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ListingNodeRefs (nodeId: %lu, name: %s, maxNodeRefs: %lu), read "
        "%lu nodeRefs: %s",
        LogTag.c_str(),
        args.NodeId,
        args.Cookie.c_str(),
        args.Limit,
        nodeRefs.size(),
        ready ? "finished" : "restarted");

    return ready;
}

void TIndexTabletActor::CompleteTx_ListNodeRefs(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodeRefs& args)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ListNodeRefs iteration completed, next nodeId: %lu, next cookie: "
        "%s",
        LogTag.c_str(),
        args.NextNodeId,
        args.NextCookie.c_str());
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleListNodeRefsRequest(
    const TEvIndexTablet::TEvListNodeRefsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ListNodeRefs iteration started (nodeId: %lu, name: %s, "
        "maxNodeRefs: %lu)",
        LogTag.c_str(),
        msg->Record.GetNodeId(),
        msg->Record.GetCookie().c_str(),
        msg->Record.GetLimit());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TListNodeRefsMethod>(*requestInfo);

    ExecuteTx<TListNodeRefs>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

}   // namespace NCloud::NFileStore::NStorage
