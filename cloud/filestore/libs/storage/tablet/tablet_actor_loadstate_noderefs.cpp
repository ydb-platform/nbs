#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::LoadNodeRefs(
    const NActors::TActorContext& ctx,
    ui64 nodeId,
    const TString& name)
{
    const ui64 maxNodeRefs = Config->GetInMemoryIndexCacheLoadOnTabletStartRowsPerTx();

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodeRefs iteration started (nodeId: %lu, name: %s, "
        "maxNodeRefs: %lu)",
        LogTag.c_str(),
        nodeId,
        name.c_str(),
        maxNodeRefs);

    ExecuteTx<TLoadNodeRefs>(
        ctx,
        nodeId,
        name,
        maxNodeRefs);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_LoadNodeRefs(
    const TActorContext& ctx,
    TTxIndexTablet::TLoadNodeRefs& args)
{
    LOG_INFO(
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

    bool ready = db.ReadNodeRefs(
        args.NodeId,
        args.Cookie,
        args.MaxNodeRefs,
        nodeRefs,
        args.NextNodeId,
        args.NextCookie);

    LOG_INFO(
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
    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodeRefs iteration completed, next nodeId: %lu, next cookie: "
        "%s",
        LogTag.c_str(),
        args.NextNodeId,
        args.NextCookie.c_str());

    if (args.NextCookie || args.NextNodeId) {
        LoadNodeRefs(ctx, args.NextNodeId, args.NextCookie);
    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s LoadNodeRefs completed",
            LogTag.c_str());

        MarkNodeRefsLoadComplete();
    }
}

}   // namespace NCloud::NFileStore::NStorage
