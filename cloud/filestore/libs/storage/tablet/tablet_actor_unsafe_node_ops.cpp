#include "tablet_actor.h"

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeDeleteNode(
    const TEvIndexTablet::TEvUnsafeDeleteNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TUnsafeDeleteNodeMethod>(*requestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeDeleteNode: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeDeleteNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnsafeDeleteNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeDeleteNode& args)
{
    Y_UNUSED(ctx);

    auto commitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    return ReadNode(db, args.Request.GetId(), commitId, args.Node);
}

void TIndexTabletActor::ExecuteTx_UnsafeDeleteNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeDeleteNode& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "UnsafeDeleteNode");
    }

    if (!args.Node) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeDeleteNode: %s - node not found",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        return;
    }

    args.Error = RemoveNode(db, *args.Node, args.Node->MinCommitId, commitId);
}

void TIndexTabletActor::CompleteTx_UnsafeDeleteNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeDeleteNode& args)
{
    RemoveTransaction(*args.RequestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeDeleteNode: %s, status: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeUpdateNode(
    const TEvIndexTablet::TEvUnsafeUpdateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TUnsafeUpdateNodeMethod>(*requestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeUpdateNode: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeUpdateNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnsafeUpdateNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeUpdateNode& args)
{
    Y_UNUSED(ctx);

    auto commitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    return ReadNode(db, args.Request.GetNode().GetId(), commitId, args.Node);
}

void TIndexTabletActor::ExecuteTx_UnsafeUpdateNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeUpdateNode& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "UnsafeUpdateNode");
    }

    NProto::TNode prevNode;
    NProto::TNode node;
    ui64 nodeCommitId = commitId;

    if (args.Node) {
        prevNode = args.Node->Attrs;
        node = args.Node->Attrs;
        nodeCommitId = args.Node->MinCommitId;
    } else {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeUpdateNode: %s - node not found, creating",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());

        // creation here is still done via UpdateNode since CreateNode generates
        // a new nodeId
    }

    ConvertAttrsToNode(args.Request.GetNode(), &node);
    UpdateNode(
        db,
        args.Request.GetNode().GetId(),
        nodeCommitId,
        commitId,
        node,
        prevNode);
}

void TIndexTabletActor::CompleteTx_UnsafeUpdateNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeUpdateNode& args)
{
    RemoveTransaction(*args.RequestInfo);

    TString oldNode;
    if (args.Node) {
        oldNode = args.Node->Attrs.Utf8DebugString();
    }
    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeUpdateNode: %s, old node: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        oldNode.Quote().c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeUpdateNodeResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeGetNode(
    const TEvIndexTablet::TEvUnsafeGetNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TUnsafeGetNodeMethod>(*requestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeGetNode: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeGetNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_UnsafeGetNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeGetNode& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    return true;
}

bool TIndexTabletActor::PrepareTx_UnsafeGetNode(
    const NActors::TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TUnsafeGetNode& args)
{
    Y_UNUSED(ctx);

    return ReadNode(
        db,
        args.Request.GetId(),
        GetCurrentCommitId(),
        args.Node);
}

void TIndexTabletActor::CompleteTx_UnsafeGetNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeGetNode& args)
{
    RemoveTransaction(*args.RequestInfo);

    TString node;
    if (args.Node) {
        node = args.Node->Attrs.Utf8DebugString();
    }
    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeGetNode: %s, node: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        node.Quote().c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeGetNodeResponse>();
    if (args.Node) {
        auto& attrs = *response->Record.MutableNode();
        ConvertNodeFromAttrs(attrs, args.Node->NodeId, args.Node->Attrs);
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
