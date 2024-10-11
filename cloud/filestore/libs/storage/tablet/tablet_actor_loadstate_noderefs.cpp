#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::LoadNodeRefsIfNeeded(
    const NActors::TActorContext& ctx,
    ui64 nodeId,
    const TString& name)
{
    LoadNodeRefsStatus.State = TLoadNodeRefsStatus::EState::LOADING;
    ctx.Send(
        SelfId(),
        new TEvIndexTabletPrivate::TEvLoadNodeRefsRequest(
            nodeId,
            name,
            Config->GetInMemoryIndexCacheLoadOnTabletStartRowsPerTx()));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleLoadNodeRefs(
    const TEvIndexTabletPrivate::TEvLoadNodeRefsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodeRefs iteration started (nodeId: %lu, name: %s, "
        "maxNodeRefs: %lu)",
        LogTag.c_str(),
        msg->NodeId,
        msg->Name.c_str(),
        msg->MaxNodeRefs);

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    ExecuteTx<TLoadNodeRefs>(
        ctx,
        std::move(requestInfo),
        msg->NodeId,
        msg->Name,
        msg->MaxNodeRefs);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleLoadNodeRefsCompleted(
    const TEvIndexTabletPrivate::TEvLoadNodeRefsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s LoadNodeRefs iteration completed, next nodeId: %lu, next cookie: "
        "%s",
        LogTag.c_str(),
        msg->NextNodeId,
        msg->NextCookie.c_str());

    if (msg->NextCookie || msg->NextNodeId) {
        LoadNodeRefsIfNeeded(ctx, msg->NextNodeId, msg->NextCookie);
    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s LoadNodeRefs completed",
            LogTag.c_str());

        MarkNodeRefsLoadComplete();
        LoadNodeRefsStatus.State = TLoadNodeRefsStatus::EState::COMPLETED;
    }
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
        &args.NextNodeId,
        &args.NextCookie);

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
    NCloud::Send(
        ctx,
        SelfId(),
        std::make_unique<TEvIndexTabletPrivate::TEvLoadNodeRefsCompleted>(
            args.NextNodeId,
            args.NextCookie));
}

}   // namespace NCloud::NFileStore::NStorage
