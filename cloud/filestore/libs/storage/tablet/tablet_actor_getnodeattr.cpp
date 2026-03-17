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
    using TMethod = TEvService::TGetNodeAttrMethod;
    if (!AcceptRequest<TMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    auto& requestMetrics = BehaveAsShard(msg->Record.GetHeaders())
        ? Metrics.GetNodeAttrInShard : Metrics.GetNodeAttr;

    AddInFlightRequest<TMethod>(*requestInfo);

    ExecuteTx<TGetNodeAttr>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        requestMetrics);
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
        args.IsNodeRefLocked = IsNodeRefLocked({args.NodeId, args.Name});
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
    RemoveInFlightRequest(*args.RequestInfo);

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
        response->Record.SetIsNodeRefLocked(args.IsNodeRefLocked);

        args.RequestMetrics.Update(
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

    AddInFlightRequest<TEvIndexTablet::TGetNodeAttrBatchMethod>(*requestInfo);

    ExecuteTx<TGetNodeAttrBatch>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
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
    RemoveInFlightRequest(*args.RequestInfo);

    using TResponse = TEvIndexTablet::TEvGetNodeAttrBatchResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    TABLET_VERIFY(args.Response.ResponsesSize() == args.Request.NamesSize());
    if (!HasError(args.Error)) {
        response->Record = std::move(args.Response);

        Metrics.GetNodeAttrInShard.Update(
            args.Request.NamesSize(),
            0,
            ctx.Now() - args.RequestInfo->StartedTs);
        Metrics.GetNodeAttrBatch.Update(
            1,
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
