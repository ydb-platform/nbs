#include "tablet_actor.h"

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeCreateNode(
    const TEvIndexTablet::TEvUnsafeCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvIndexTablet::TUnsafeCreateNodeMethod>(*requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeCreateNode: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeCreateNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnsafeCreateNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeCreateNode& args)
{
    Y_UNUSED(ctx);

    auto commitId = GetCurrentCommitId();

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);
    return ReadNode(db, args.Request.GetNode().GetId(), commitId, args.Node);
}

void TIndexTabletActor::ExecuteTx_UnsafeCreateNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeCreateNode& args)
{
    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    if (args.Node) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeCreateNode: %s - node exists",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        args.Error = ErrorAlreadyExists(args.Request.GetNode().GetId());
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    NProto::TNode node;
    ConvertAttrsToNode(args.Request.GetNode(), &node);
    CreateNodeWithId(db, args.Request.GetNode().GetId(), commitId, node);
}

void TIndexTabletActor::CompleteTx_UnsafeCreateNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeCreateNode& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    TString oldNode;
    if (args.Node) {
        oldNode = args.Node->Attrs.Utf8DebugString();
    }
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeCreateNode: %s, old node: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        oldNode.Quote().c_str());

    using TResponse = TEvIndexTablet::TEvUnsafeCreateNodeResponse;
    auto response = std::make_unique<TResponse>(std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}


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

    AddInFlightRequest<TEvIndexTablet::TUnsafeDeleteNodeMethod>(*requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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

    if (!args.Node) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeDeleteNode: %s - node not found",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        args.Error = ErrorInvalidTarget(args.Request.GetId());
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    args.Error = RemoveNode(db, *args.Node, args.Node->MinCommitId, commitId);
}

void TIndexTabletActor::CompleteTx_UnsafeDeleteNode(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeDeleteNode& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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

    AddInFlightRequest<TEvIndexTablet::TUnsafeUpdateNodeMethod>(*requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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

    if (!args.Node) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeUpdateNode: %s - node not found",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        args.Error = ErrorInvalidTarget(
            args.Request.GetNode().GetId());
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    auto prevNode = args.Node->Attrs;
    auto node = args.Node->Attrs;
    ui64 nodeCommitId = args.Node->MinCommitId;

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
    RemoveInFlightRequest(*args.RequestInfo);

    TString oldNode;
    if (args.Node) {
        oldNode = args.Node->Attrs.Utf8DebugString();
    }
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeUpdateNode: %s, old node: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        oldNode.Quote().c_str());

    using TResponse = TEvIndexTablet::TEvUnsafeUpdateNodeResponse;
    auto response = std::make_unique<TResponse>(std::move(args.Error));

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

    AddInFlightRequest<TEvIndexTablet::TUnsafeGetNodeMethod>(*requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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
    RemoveInFlightRequest(*args.RequestInfo);

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeGetNodeResponse>();

    if (args.Node) {
        auto& attrs = *response->Record.MutableNode();
        ConvertNodeFromAttrs(attrs, args.Node->NodeId, args.Node->Attrs);
    } else {
        *response->Record.MutableError() =
            ErrorInvalidTarget(args.Request.GetId());
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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

    AddInFlightRequest<TEvIndexTablet::TUnsafeCreateNodeRefMethod>(
        *requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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

    if (args.NodeRef) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s UnsafeCreateNodeRef: %s - node ref exists",
            LogTag.c_str(),
            args.Request.DebugString().Quote().c_str());
        args.Error = ErrorAlreadyExists(args.Request.GetName());
        return;
    }

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
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
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeCreateNodeRef: %s, status: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        FormatError(args.Error).c_str());

    using TResponse = TEvIndexTablet::TEvUnsafeCreateNodeRefResponse;
    auto response = std::make_unique<TResponse>(std::move(args.Error));

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

    AddInFlightRequest<TEvIndexTablet::TUnsafeDeleteNodeRefMethod>(
        *requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
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
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeDeleteNodeRef: %s, status: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        FormatError(args.Error).c_str());

    using TResponse = TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse;
    auto response = std::make_unique<TResponse>(std::move(args.Error));

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

    AddInFlightRequest<TEvIndexTablet::TUnsafeUpdateNodeRefMethod>(
        *requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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

    auto commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
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
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeUpdateNodeRef: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str());

    using TResponse = TEvIndexTablet::TEvUnsafeUpdateNodeRefResponse;
    auto response = std::make_unique<TResponse>(std::move(args.Error));

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

    AddInFlightRequest<TEvIndexTablet::TUnsafeGetNodeRefMethod>(*requestInfo);

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
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
    RemoveInFlightRequest(*args.RequestInfo);

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

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s UnsafeGetNodeRef: %s, result: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        response->Record.ShortUtf8DebugString().Quote().c_str());

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUnsafeCreateHandle(
    const TEvIndexTablet::TEvUnsafeCreateHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvIndexTablet::TUnsafeCreateHandleMethod>(*requestInfo);

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s UnsafeCreateHandle: %s",
        LogTag.c_str(),
        msg->Record.DebugString().Quote().c_str());

    ExecuteTx<TUnsafeCreateHandle>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UnsafeCreateHandle(
        const TActorContext& ctx,
        TTransactionContext& tx,
        TTxIndexTablet::TUnsafeCreateHandle& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_UnsafeCreateHandle(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUnsafeCreateHandle& args)
{
    Y_UNUSED(ctx);

    TSession* session = FindSession(args.Request.GetSessionId());
    if (!session) {
        auto message = ReportSessionNotFoundInTx(
            TStringBuilder()
            << "CreateHandle: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return;
    }

    TIndexTabletDatabase db(tx.DB);

    TSessionHandle* handle = UnsafeCreateHandle(
        db,
        session,
        args.Request.GetHandle(),
        args.Request.GetNodeId(),
        args.Request.GetCommitId(),
        args.Request.GetFlags());

    if (!handle) {
        TString message = ReportFailedToCreateHandle(
            TStringBuilder()
            << "UnsafeCreateHandle: " << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return;
    }
}

void TIndexTabletActor::CompleteTx_UnsafeCreateHandle(
    const TActorContext& ctx,
    TTxIndexTablet::TUnsafeCreateHandle& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    auto response =
        std::make_unique<TEvIndexTablet::TEvUnsafeCreateHandleResponse>(
            args.Error);

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s UnsafeCreateHandle: %s, result: %s",
        LogTag.c_str(),
        args.Request.DebugString().Quote().c_str(),
        response->Record.ShortUtf8DebugString().Quote().c_str());

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
