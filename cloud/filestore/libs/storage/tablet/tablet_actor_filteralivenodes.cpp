#include "tablet_actor.h"

#include <util/string/builder.h>
#include <util/string/join.h>

#include <ranges>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <std::ranges::range TRange>
TString DescribeNodes(const TRange& nodes)
{
    return TStringBuilder() << "[" << JoinSeq(", ", nodes) << "]";
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleFilterAliveNodes(
    const TEvIndexTabletPrivate::TEvFilterAliveNodesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s FilterAliveNodes %s",
        LogTag.c_str(),
        DescribeNodes(msg->Nodes).c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        msg->CallContext,
        "FilterAliveNodes");

    ExecuteTx<TFilterAliveNodes>(
        ctx,
        std::move(requestInfo),
        msg->Nodes);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_FilterAliveNodes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TFilterAliveNodes& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GetCurrentCommitId();
    TMaybe<IIndexTabletDatabase::TNode> node;

    bool ready = true;

    for (const auto nodeId: args.Nodes) {
        if (!ReadNode(db, nodeId, args.CommitId, node)) {
            ready = false;
            continue;
        }
        if (!node.Defined()) {
            continue;
        }

        args.AliveNodes.emplace(nodeId);
        node.Clear();
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_FilterAliveNodes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TFilterAliveNodes& args)
{
    Y_UNUSED(ctx, tx, args);
}

void TIndexTabletActor::CompleteTx_FilterAliveNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TFilterAliveNodes& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s FilterAliveNodes %s completed. Result: %s",
        LogTag.c_str(),
        DescribeNodes(args.Nodes).c_str(),
        DescribeNodes(args.AliveNodes).c_str());

    auto response =
        std::make_unique<TEvIndexTabletPrivate::TEvFilterAliveNodesResponse>();
    response->Nodes = std::move(args.AliveNodes);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
