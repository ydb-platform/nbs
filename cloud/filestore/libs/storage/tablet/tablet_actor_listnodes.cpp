#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/core/helpers.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

void AddNode(
    NProto::TListNodesResponse& record,
    TString name,
    ui64 id,
    const NProto::TNode& attrs)
{
    record.AddNames(std::move(name));
    ConvertNodeFromAttrs(*record.AddNodes(), id, attrs);
}

void AddExternalNode(
    NProto::TListNodesResponse& record,
    TString name,
    TString shardId,
    TString shardNodeName)
{
    record.AddNames(std::move(name));
    auto* node = record.AddNodes();
    node->SetShardFileSystemId(std::move(shardId));
    node->SetShardNodeName(std::move(shardNodeName));
}

NProto::TError ValidateRequest(const NProto::TListNodesRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    return {};
}

NProto::TError ValidateInternalRequest(
    const NProtoPrivate::TListNodesInternalRequest& request)
{
    return ValidateRequest(request.GetOriginalRequest());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleListNodes(
    const TEvService::TEvListNodesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TMethod = TEvService::TListNodesMethod;
    auto* msg = ev->Get();

    const bool shouldValidateSession = !msg->Record.GetUnsafe();

    if (shouldValidateSession) {
        if (!AcceptRequest<TMethod>(ev, ctx, ValidateRequest)) {
            return;
        }
    } else if (!AcceptRequestNoSession<TMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvService::TListNodesMethod>(*requestInfo);

    auto maxBytes = Min(
        Config->GetMaxResponseEntries() * MaxName,
        Config->GetMaxResponseBytes());
    if (auto bytes = msg->Record.GetMaxBytes()) {
        maxBytes = Min(bytes, maxBytes);
    }

    // Set size calculation mode from config if not explicitly set in request.
    // TODO(#5148): explicitly pass the mode from client side.
    if (msg->Record.GetListNodesSizeMode() == NProto::LNSM_UNSPECIFIED)
    {
        msg->Record.SetListNodesSizeMode(Config->GetListNodesSizeMode());
    }

    ExecuteTx<TListNodes>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        maxBytes,
        Config->GetMaxBytesMultiplier(),
        false /* replyInternal */);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleListNodesInternal(
    const TEvIndexTablet::TEvListNodesInternalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TMethod = TEvIndexTablet::TListNodesInternalMethod;
    auto* msg = ev->Get();

    if (!AcceptRequestNoSession<TMethod>(ev, ctx, ValidateInternalRequest)) {
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TEvIndexTablet::TListNodesInternalMethod>(*requestInfo);

    auto maxBytes = Min(
        Config->GetMaxResponseEntries() * MaxName,
        Config->GetMaxResponseBytes());
    auto& originalRequest = *msg->Record.MutableOriginalRequest();
    if (auto bytes = originalRequest.GetMaxBytes()) {
        maxBytes = Min(bytes, maxBytes);
    }

    // Set size calculation mode from config if not explicitly set in request.
    // TODO(#5148): explicitly pass the mode from client side.
    if (originalRequest.GetListNodesSizeMode() == NProto::LNSM_UNSPECIFIED)
    {
        originalRequest.SetListNodesSizeMode(Config->GetListNodesSizeMode());
    }

    ExecuteTx<TListNodes>(
        ctx,
        std::move(requestInfo),
        std::move(originalRequest),
        maxBytes,
        Config->GetMaxBytesMultiplier(),
        true /* replyInternal */);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_ListNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodes& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);

    const TString& checkpointId = session ? session->GetCheckpointId() : "";
    args.CommitId = GetReadCommitId(checkpointId);
    if (args.CommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(checkpointId);
        return false;
    }

    return true;
}

bool TIndexTabletActor::PrepareTx_ListNodes(
    const NActors::TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TListNodes& args)
{
    Y_UNUSED(ctx);

    // validate target node exists
    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        return false;   // not ready
    }

    if (!args.Node) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    }

    if (args.Node->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
        args.Error = ErrorIsNotDirectory(args.NodeId);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);

    if (!PrechargeNodeRefs(
            db,
            args.NodeId,
            args.Cookie,
            Max<ui64>(),
            args.BytesToPrecharge))
    {
        return false; // not ready
    }

    bool ready = true;
    // list children refs
    if (!ReadNodeRefs(
        db,
        args.NodeId,
        args.CommitId,
        args.Cookie,
        args.ChildRefs,
        args.MaxBytes,
        &args.Next,
        Config->GetNodeRefsNoAutoPrecharge(),
        args.Request.GetListNodesSizeMode()))
    {
        ready = false;
    }

    // get actual nodes
    args.ChildNodes.reserve(args.ChildRefs.size());
    for (const auto& ref: args.ChildRefs) {
        if (ref.IsExternal()) {
            continue;
        }

        TMaybe<TIndexTabletDatabase::TNode> childNode;
        if (!ReadNode(db, ref.ChildNodeId, args.CommitId, childNode)) {
            ready = false;   // not ready
        }

        if (ready) {
            // TODO: AccessCheck
            TABLET_VERIFY(childNode);
            args.ChildNodes.emplace_back(std::move(childNode.GetRef()));
        }
    }

    return ready;
}

void TIndexTabletActor::ReplyListNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodes& args)
{
    auto response =
        std::make_unique<TEvService::TEvListNodesResponse>(args.Error);
    if (SUCCEEDED(args.Error.GetCode())) {
        auto& record = response->Record;
        record.MutableNames()->Reserve(args.ChildRefs.size());
        record.MutableNodes()->Reserve(args.ChildRefs.size());

        ui64 requestBytes = 0;

        size_t j = 0;
        for (size_t i = 0; i < args.ChildRefs.size(); ++i) {
            auto& ref = args.ChildRefs[i];
            requestBytes += ref.Name.size();
            if (ref.IsExternal()) {
                if (!HasPendingNodeCreateInShard(ref.ShardNodeName)) {
                    AddExternalNode(
                        record,
                        std::move(ref.Name),
                        std::move(ref.ShardId),
                        std::move(ref.ShardNodeName));
                }

                continue;
            }

            AddNode(
                record,
                std::move(ref.Name),
                ref.ChildNodeId,
                args.ChildNodes[j].Attrs);
            ++j;
        }

        if (args.Next) {
            record.SetCookie(args.Next);
        }

        Metrics.ListNodes.Update(
            1,
            requestBytes,
            ctx.Now() - args.RequestInfo->StartedTs);
        Metrics.ListNodesExtra.RequestedBytesPrecharge.fetch_add(
            args.BytesToPrecharge,
            std::memory_order_relaxed);
        Metrics.ListNodesExtra.PrepareAttempts.fetch_add(
            args.PrepareAttempts,
            std::memory_order_relaxed);
        Metrics.ListNodesExtra.ResponseNodeRefs.fetch_add(
            args.ChildRefs.size(),
            std::memory_order_relaxed);
    }

    CompleteResponse<TEvService::TListNodesMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

void TIndexTabletActor::ReplyListNodesInternal(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodes& args)
{
    using TResponse = TEvIndexTablet::TEvListNodesInternalResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    if (SUCCEEDED(args.Error.GetCode())) {
        //
        // Allocate memory.
        //

        ui64 nameBufferSize = 0;
        ui64 extRefBufferSize = 0;
        int extRefCount = 0;
        int skippedRefCount = 0;
        for (const auto& ref: args.ChildRefs) {
            if (ref.IsExternal()
                    && HasPendingNodeCreateInShard(ref.ShardNodeName))
            {
                ++skippedRefCount;
                continue;
            }

            nameBufferSize += ref.Name.size();
            if (!ref.IsExternal()) {
                continue;
            }

            extRefBufferSize += ref.ShardId.size();
            extRefBufferSize += ref.ShardNodeName.size();
            ++extRefCount;
        }

        auto& record = response->Record;
        TListNodesInternalResponseBuilder builder(
            record,
            nameBufferSize,
            extRefBufferSize,
            args.ChildRefs.size() - skippedRefCount,
            extRefCount);
        record.MutableNodes()->Reserve(
            args.ChildRefs.size() - extRefCount - skippedRefCount);

        //
        // Build response.
        //

        ui32 j = 0;
        for (auto& ref: args.ChildRefs) {
            if (ref.IsExternal()
                    && HasPendingNodeCreateInShard(ref.ShardNodeName))
            {
                continue;
            }

            const bool added = builder.AddNodeRef(
                ref.Name,
                ref.ShardId,
                ref.ShardNodeName);
            if (!added) {
                auto message = ReportListNodesInternalFailedToAddNodeRef(
                    TStringBuilder() << "builder index: " << builder.GetIndex());
                *response->Record.MutableError() =
                    MakeError(E_INVALID_STATE, std::move(message));
                break;
            }

            if (ref.IsExternal()) {
                continue;
            }

            ConvertNodeFromAttrs(
                *record.AddNodes(),
                ref.ChildNodeId,
                args.ChildNodes[j].Attrs);

            ++j;
        }

        if (args.Next) {
            record.SetCookie(args.Next);
        }

        Metrics.ListNodes.Update(
            1,
            nameBufferSize,
            ctx.Now() - args.RequestInfo->StartedTs);
        Metrics.ListNodesExtra.RequestedBytesPrecharge.fetch_add(
            args.BytesToPrecharge,
            std::memory_order_relaxed);
        Metrics.ListNodesExtra.PrepareAttempts.fetch_add(
            args.PrepareAttempts,
            std::memory_order_relaxed);
        Metrics.ListNodesExtra.ResponseNodeRefs.fetch_add(
            args.ChildRefs.size(),
            std::memory_order_relaxed);
    }

    CompleteResponse<TEvIndexTablet::TListNodesInternalMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

void TIndexTabletActor::CompleteTx_ListNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodes& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    if (args.ReplyInternal) {
        ReplyListNodesInternal(ctx, args);
        return;
    }

    ReplyListNodes(ctx, args);
}

}   // namespace NCloud::NFileStore::NStorage
