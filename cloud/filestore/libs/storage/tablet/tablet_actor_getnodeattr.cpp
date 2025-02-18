#include "tablet_actor.h"

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateRequest(const NProto::TGetNodeAttrRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    if (!request.GetName().empty()) {
        if (auto error = ValidateNodeName(request.GetName()); HasError(error)) {
            return error;
        }
    }

    return {};
}

NProto::TError ValidateBatchRequest(
    const NProtoPrivate::TGetNodeAttrBatchRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    for (const auto& name: request.GetNames()) {
        if (auto error = ValidateNodeName(name); HasError(error)) {
            return error;
        }
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGetNodeAttr(
    const TEvService::TEvGetNodeAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (!AcceptRequest<TEvService::TGetNodeAttrMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    if (msg->Record.GetName()) {
        // access by parentId/name is a more common case. Try to get the result
        // from cache.
        NProto::TNodeAttr result;

        if (TryFillGetNodeAttrResult(
                msg->Record.GetNodeId(),
                msg->Record.GetName(),
                &result))
        {
            auto response =
                std::make_unique<TEvService::TEvGetNodeAttrResponse>();
            response->Record.MutableNode()->Swap(&result);

            CompleteResponse<TEvService::TGetNodeAttrMethod>(
                response->Record,
                msg->CallContext,
                ctx);

            Metrics.NodeIndexCacheHitCount.fetch_add(
                1,
                std::memory_order_relaxed);
            Metrics.GetNodeAttr.Update(1, 0, TDuration::Zero());

            NCloud::Reply(ctx, *requestInfo, std::move(response));
            return;
        }
    }

    AddTransaction<TEvService::TGetNodeAttrMethod>(*requestInfo);

    ExecuteTx<TGetNodeAttr>(
        ctx,
        std::move(requestInfo),
        msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_GetNodeAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TGetNodeAttr& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);
    if (!session) {
        args.Error = ErrorInvalidSession(
            args.ClientId,
            args.SessionId,
            args.SessionSeqNo);
        return false;
    }

    args.CommitId = GetReadCommitId(session->GetCheckpointId());
    if (args.CommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(session->GetCheckpointId());
        return false;
    }

    return true;
}

bool TIndexTabletActor::PrepareTx_GetNodeAttr(
    const NActors::TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TGetNodeAttr& args)
{
    Y_UNUSED(ctx);

    // There could be two cases:
    // * access by parentId/name
    // * access by nodeId

    if (args.Name) {
        // validate parent node exists
        if (!ReadNode(db, args.NodeId, args.CommitId, args.ParentNode)) {
            return false;   // not ready
        }

        if (!args.ParentNode) {
            args.Error = ErrorInvalidParent(args.NodeId);
            return true;
        }

        // TODO: access check
        TABLET_VERIFY(args.ParentNode);

        // validate target node exists
        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        if (!ReadNodeRef(db, args.NodeId, args.CommitId, args.Name, ref)) {
            return false;   // not ready
        }

        if (!ref) {
            args.Error = ErrorInvalidTarget(args.NodeId, args.Name);
            return true;
        }

        args.TargetNodeId = ref->ChildNodeId;
        args.ShardId = ref->ShardId;
        args.ShardNodeName = ref->ShardNodeName;
    } else {
        args.TargetNodeId = args.NodeId;
    }

    if (args.ShardId) {
        return true;
    }

    if (!ReadNode(db, args.TargetNodeId, args.CommitId, args.TargetNode)) {
        return false;   // not ready
    }

    if (!args.TargetNode) {
        args.Error = ErrorInvalidTarget(args.TargetNodeId, args.Name);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.TargetNode);

    return true;
}

void TIndexTabletActor::CompleteTx_GetNodeAttr(
    const TActorContext& ctx,
    TTxIndexTablet::TGetNodeAttr& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvGetNodeAttrResponse>(args.Error);
    if (SUCCEEDED(args.Error.GetCode())) {
        auto* node = response->Record.MutableNode();
        if (args.ShardId) {
            node->SetShardFileSystemId(args.ShardId);
            node->SetShardNodeName(args.ShardNodeName);
        } else {
            TABLET_VERIFY(args.TargetNode);
            ConvertNodeFromAttrs(
                *node,
                args.TargetNodeId,
                args.TargetNode->Attrs);
        }

        if (args.Name) {
            // cache the result for future access
            RegisterGetNodeAttrResult(
                args.ParentNode->NodeId,
                args.Name,
                *node);
        }

        Metrics.GetNodeAttr.Update(
            1,
            0,
            ctx.Now() - args.RequestInfo->StartedTs);
    }

    CompleteResponse<TEvService::TGetNodeAttrMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGetNodeAttrBatch(
    const TEvIndexTablet::TEvGetNodeAttrBatchRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    bool accepted = AcceptRequest<TEvIndexTablet::TGetNodeAttrBatchMethod>(
        ev,
        ctx,
        ValidateBatchRequest);
    if (!accepted) {
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    ui32 cacheHits = 0;
    NProtoPrivate::TGetNodeAttrBatchResponse result;
    for (ui32 i = 0; i < msg->Record.NamesSize(); ++i) {
        auto* nodeResult = result.AddResponses();

        if (TryFillGetNodeAttrResult(
                msg->Record.GetNodeId(),
                msg->Record.GetNames(i),
                nodeResult->MutableNode()))
        {
            ++cacheHits;
        }
    }

    Metrics.NodeIndexCacheHitCount.fetch_add(
        cacheHits,
        std::memory_order_relaxed);

    if (cacheHits == msg->Record.NamesSize()) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvGetNodeAttrBatchResponse>();
        response->Record = std::move(result);

        CompleteResponse<TEvIndexTablet::TGetNodeAttrBatchMethod>(
            response->Record,
            msg->CallContext,
            ctx);

        Metrics.GetNodeAttr.Update(cacheHits, 0, TDuration::Zero());

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    AddTransaction<TEvService::TGetNodeAttrMethod>(*requestInfo);

    ExecuteTx<TGetNodeAttrBatch>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record),
        std::move(result));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_GetNodeAttrBatch(
    const TActorContext& ctx,
    TTxIndexTablet::TGetNodeAttrBatch& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);
    if (!session) {
        args.Error = ErrorInvalidSession(
            args.ClientId,
            args.SessionId,
            args.SessionSeqNo);
        return false;
    }

    args.CommitId = GetReadCommitId(session->GetCheckpointId());
    if (args.CommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(session->GetCheckpointId());
        return false;
    }

    return true;
}

bool TIndexTabletActor::PrepareTx_GetNodeAttrBatch(
    const NActors::TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TGetNodeAttrBatch& args)
{
    Y_UNUSED(ctx);

    // validate parent node exists
    const bool readParent =
        ReadNode(db, args.Request.GetNodeId(), args.CommitId, args.ParentNode);
    if (!readParent) {
        return false;   // not ready
    }

    if (!args.ParentNode) {
        args.Error = ErrorInvalidParent(args.Request.GetNodeId());
        return true;
    }

    // TODO: access check

    TVector<TMaybe<IIndexTabletDatabase::TNodeRef>> refs(
        args.Request.NamesSize());
    ui32 foundRefs = 0;
    for (ui32 i = 0; i < args.Request.NamesSize(); ++i) {
        if (args.Response.GetResponses(i).GetNode().GetId() != InvalidNodeId) {
            // found in cache
            ++foundRefs;
            continue;
        }

        foundRefs += ReadNodeRef(
            db,
            args.Request.GetNodeId(),
            args.CommitId,
            args.Request.GetNames(i),
            refs[i]);
    }

    if (foundRefs < refs.size()) {
        return false;   // not ready
    }

    TVector<TMaybe<IIndexTabletDatabase::TNode>> nodes(
        args.Request.NamesSize());
    bool ready = true;
    for (ui32 i = 0; i < args.Request.NamesSize(); ++i) {
        auto* nodeResult = args.Response.MutableResponses(i);
        if (nodeResult->GetNode().GetId() != InvalidNodeId) {
            // found in cache
            continue;
        }

        if (!refs[i]) {
            *nodeResult->MutableError() = ErrorInvalidTarget(
                args.Request.GetNodeId(),
                args.Request.GetNames(i));
            continue;
        }

        auto* nodeAttr = nodeResult->MutableNode();
        if (refs[i]->IsExternal()) {
            nodeAttr->SetShardFileSystemId(refs[i]->ShardId);
            nodeAttr->SetShardNodeName(refs[i]->ShardNodeName);
            continue;
        }

        if (!ReadNode(db, refs[i]->ChildNodeId, args.CommitId, nodes[i])) {
            ready = false;
        }
    }

    if (!ready) {
        return false;
    }

    for (ui32 i = 0; i < args.Request.NamesSize(); ++i) {
        auto* nodeResult = args.Response.MutableResponses(i);
        if (nodeResult->GetNode().GetId() != InvalidNodeId
                || HasError(nodeResult->GetError())
                || nodeResult->GetNode().GetShardFileSystemId())
        {
            continue;
        }

        if (nodes[i]) {
            ConvertNodeFromAttrs(
                *nodeResult->MutableNode(),
                refs[i]->ChildNodeId,
                nodes[i]->Attrs);
        } else {
            *nodeResult->MutableError() = ErrorInvalidTarget(
                refs[i]->NodeId,
                args.Request.GetNames(i));
        }
    }

    return true;
}

void TIndexTabletActor::CompleteTx_GetNodeAttrBatch(
    const TActorContext& ctx,
    TTxIndexTablet::TGetNodeAttrBatch& args)
{
    RemoveTransaction(*args.RequestInfo);

    using TResponse = TEvIndexTablet::TEvGetNodeAttrBatchResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    TABLET_VERIFY(args.Response.ResponsesSize() == args.Request.NamesSize());
    if (!HasError(args.Error)) {
        for (ui32 i = 0; i < args.Request.NamesSize(); ++i) {
            const auto& nodeResult = args.Response.GetResponses(i);
            if (!HasError(nodeResult.GetError())) {
                RegisterGetNodeAttrResult(
                    args.ParentNode->NodeId,
                    args.Request.GetNames(i),
                    nodeResult.GetNode());
            }
        }

        response->Record = std::move(args.Response);

        Metrics.GetNodeAttr.Update(
            args.Request.NamesSize(),
            0,
            ctx.Now() - args.RequestInfo->StartedTs);
    }

    CompleteResponse<TEvIndexTablet::TGetNodeAttrBatchMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
