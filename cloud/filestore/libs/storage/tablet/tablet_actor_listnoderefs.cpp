#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_ReadNodeRefs(
    const TActorContext& ctx,
    TTxIndexTablet::TReadNodeRefs& args)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ReadNodeRefs (StartNodeId: %lu, StartName: %s, Limit: %lu)",
        LogTag.c_str(),
        args.NodeId,
        args.Cookie.c_str(),
        args.Limit);
    return true;
}

bool TIndexTabletActor::PrepareTx_ReadNodeRefs(
    const TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TReadNodeRefs& args)
{
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
        "%s ReadNodeRefs (nodeId: %lu, name: %s, maxNodeRefs: %lu), read "
        "%lu nodeRefs: %s",
        LogTag.c_str(),
        args.NodeId,
        args.Cookie.c_str(),
        args.Limit,
        args.Refs.size(),
        ready ? "finished" : "restarted");

    return ready;
}

void TIndexTabletActor::CompleteTx_ReadNodeRefs(
    const TActorContext& ctx,
    TTxIndexTablet::TReadNodeRefs& args)
{
    RemoveTransaction(*args.RequestInfo);
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s ReadNodeRefs iteration completed, next nodeId: %lu, next cookie: "
        "%s",
        LogTag.c_str(),
        args.NextNodeId,
        args.NextCookie.c_str());
    auto response = std::make_unique<TEvIndexTablet::TEvReadNodeRefsResponse>();
    response->Record.SetNextNodeId(args.NextNodeId);
    response->Record.SetNextCookie(args.NextCookie);
    response->Record.MutableNodeRefs()->Reserve(args.Refs.size());
    for (const auto& ref : args.Refs) {
        auto* nodeRef = response->Record.AddNodeRefs();
        nodeRef->SetNodeId(ref.NodeId);
        nodeRef->SetName(ref.Name);
        nodeRef->SetChildId(ref.ChildNodeId);
        nodeRef->SetShardId(ref.ShardId);
        nodeRef->SetShardNodeName(ref.ShardNodeName);
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleReadNodeRefs(
    const TEvIndexTablet::TEvReadNodeRefsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TReadNodeRefsMethod>(*requestInfo);

    ExecuteTx<TReadNodeRefs>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

}   // namespace NCloud::NFileStore::NStorage
