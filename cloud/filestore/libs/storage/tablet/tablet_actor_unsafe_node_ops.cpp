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
        return args.OnCommitIdOverflow();
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
        return args.OnCommitIdOverflow();
    }

    NProto::TNode prevNode;
    NProto::TNode node;
    ui64 nodeCommitId = commitId;

    if (args.Node) {
        prevNode = args.Node->Attrs;
        node = args.Node->Attrs;
        nodeCommitId = args.Node->MinCommitId;

        ConvertAttrsToNode(args.Request.GetNode(), &node);
        UpdateNode(
            db,
            args.Request.GetNode().GetId(),
            nodeCommitId,
            commitId,
            node,
            prevNode);
    } else {
        // Create new node with specified ID
        LOG_WARN(
            ctx,
            TFileStoreComponents::TABLET,
            "%s UnsafeUpdateNode: %s - node not found, creating",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());

        ConvertAttrsToNode(args.Request.GetNode(), &node);
        CreateNodeWithId(db, args.Request.GetNode().GetId(), commitId, node);
    }
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
        std::make_unique<TEvIndexTablet::TEvUnsafeUpdateNodeResponse>(
            std::move(args.Error));

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

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeGetNodeResponse>();

    if (args.Node) {
        auto& attrs = *response->Record.MutableNode();
        ConvertNodeFromAttrs(attrs, args.Node->NodeId, args.Node->Attrs);
    } else {
        *response->Record.MutableError() =
            ErrorInvalidTarget(args.Request.GetId());
    }

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeGetNode: %s, result: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        response->Record.ShortUtf8DebugString().Quote().c_str());

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeCreateNodeRef(
    const TEvIndexTablet::TEvUnsafeCreateNodeRefRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TUnsafeCreateNodeRefMethod>(*requestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeCreateNodeRef: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeCreateNodeRef>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnsafeCreateNodeRef(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeCreateNodeRef& args)
{
    Y_UNUSED(ctx);

    auto commitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    return ReadNodeRef(
        db,
        args.Request.GetParentId(),
        commitId,
        args.Request.GetName(),
        args.NodeRef);
}

void TIndexTabletActor::ExecuteTx_UnsafeCreateNodeRef(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeCreateNodeRef& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    if (args.NodeRef) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeCreateNodeRef: %s - node ref exists",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        args.Error = ErrorAlreadyExists(args.Request.GetName());
        return;
    }

    CreateNodeRef(
        db,
        args.Request.GetParentId(),
        commitId,
        args.Request.GetName(),
        args.Request.GetChildId(),
        args.Request.GetShardId(),
        args.Request.GetShardNodeName());
}

void TIndexTabletActor::CompleteTx_UnsafeCreateNodeRef(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeCreateNodeRef& args)
{
    RemoveTransaction(*args.RequestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeCreateNodeRef: %s, status: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRefResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeDeleteNodeRef(
    const TEvIndexTablet::TEvUnsafeDeleteNodeRefRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TUnsafeDeleteNodeRefMethod>(*requestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeDeleteNodeRef: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeDeleteNodeRef>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnsafeDeleteNodeRef(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeDeleteNodeRef& args)
{
    Y_UNUSED(ctx);

    auto commitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    return ReadNodeRef(
        db,
        args.Request.GetParentId(),
        commitId,
        args.Request.GetName(),
        args.NodeRef);
}

void TIndexTabletActor::ExecuteTx_UnsafeDeleteNodeRef(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeDeleteNodeRef& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    if (!args.NodeRef) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeDeleteNodeRef: %s - node ref not found",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        args.Error = ErrorInvalidTarget(
            args.Request.GetParentId(),
            args.Request.GetName());
        return;
    }

    RemoveNodeRef(
        db,
        args.Request.GetParentId(),
        args.NodeRef->MinCommitId,
        commitId,
        args.Request.GetName(),
        args.NodeRef->ChildNodeId,
        args.NodeRef->ShardId,
        args.NodeRef->ShardNodeName);
}

void TIndexTabletActor::CompleteTx_UnsafeDeleteNodeRef(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeDeleteNodeRef& args)
{
    RemoveTransaction(*args.RequestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeDeleteNodeRef: %s, status: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        FormatError(args.Error).c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeUpdateNodeRef(
    const TEvIndexTablet::TEvUnsafeUpdateNodeRefRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TUnsafeUpdateNodeRefMethod>(*requestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeUpdateNodeRef: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeUpdateNodeRef>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnsafeUpdateNodeRef(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeUpdateNodeRef& args)
{
    Y_UNUSED(ctx);

    auto commitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    return ReadNodeRef(
        db,
        args.Request.GetParentId(),
        commitId,
        args.Request.GetName(),
        args.NodeRef);
}

void TIndexTabletActor::ExecuteTx_UnsafeUpdateNodeRef(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeUpdateNodeRef& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    if (!args.NodeRef) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeUpdateNodeRef: %s - node ref not found",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        args.Error = ErrorInvalidTarget(
            args.Request.GetParentId(),
            args.Request.GetName());
        return;
    }

    RemoveNodeRef(
        db,
        args.Request.GetParentId(),
        args.NodeRef->MinCommitId,
        commitId,
        args.Request.GetName(),
        args.NodeRef->ChildNodeId,
        args.NodeRef->ShardId,
        args.NodeRef->ShardNodeName);

    CreateNodeRef(
        db,
        args.Request.GetParentId(),
        commitId,
        args.Request.GetName(),
        args.Request.GetChildId(),
        args.Request.GetShardId(),
        args.Request.GetShardNodeName());
}

void TIndexTabletActor::CompleteTx_UnsafeUpdateNodeRef(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeUpdateNodeRef& args)
{
    RemoveTransaction(*args.RequestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeUpdateNodeRef: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeUpdateNodeRefResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeGetNodeRef(
    const TEvIndexTablet::TEvUnsafeGetNodeRefRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TUnsafeGetNodeRefMethod>(*requestInfo);

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeGetNodeRef: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeGetNodeRef>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_UnsafeGetNodeRef(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeGetNodeRef& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    return true;
}

bool TIndexTabletActor::PrepareTx_UnsafeGetNodeRef(
    const NActors::TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TUnsafeGetNodeRef& args)
{
    Y_UNUSED(ctx);

    return ReadNodeRef(
        db,
        args.Request.GetParentId(),
        GetCurrentCommitId(),
        args.Request.GetName(),
        args.NodeRef);
}

void TIndexTabletActor::CompleteTx_UnsafeGetNodeRef(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeGetNodeRef& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeGetNodeRefResponse>();

    if (args.NodeRef) {
        response->Record.SetChildId(args.NodeRef->NodeId);
        response->Record.SetShardId(args.NodeRef->ShardId);
        response->Record.SetShardNodeName(args.NodeRef->ShardNodeName);
    } else {
        *response->Record.MutableError() = ErrorInvalidTarget(
            args.Request.GetParentId(),
            args.Request.GetName());
    }

    LOG_WARN(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeGetNodeRef: %s, result: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        response->Record.ShortUtf8DebugString().Quote().c_str());

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
