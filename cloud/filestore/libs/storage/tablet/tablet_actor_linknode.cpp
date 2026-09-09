#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleLinkNodeInShard(
    const TEvIndexTablet::TEvLinkNodeInShardRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TMethod = TEvIndexTablet::TLinkNodeInShardMethod;

    auto* msg = ev->Get();

    NProto::TProfileLogRequestInfo profileLogRequest;
    InitTabletProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreSystemRequest::LinkNodeInShard,
        msg->Record,
        ctx.Now());
    auto onReply = [&] (const NProto::TError& error) {
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);
    };

    const ui64 clientTabletId =
        msg->Record.GetHeaders().GetInternal().GetClientTabletId();
    const ui64 requestId = msg->Record.GetHeaders().GetRequestId();

    if (!clientTabletId || !requestId) {
        auto error = MakeError(E_ARGUMENT, TStringBuilder()
            << "both ClientTabletId and RequestId should be nonzero: "
            << clientTabletId << ", " << requestId);
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TMethod::TResponse>(error));
        onReply(error);
        return;
    }

    if (const auto* e = LookupResponseLogEntry(clientTabletId, requestId)) {
        auto response = std::make_unique<TMethod::TResponse>();
        if (e->HasLinkNodeInShardResponse()) {
            response->Record = e->GetLinkNodeInShardResponse();
        } else {
            auto message = ReportInvalidResponseLogEntry(TStringBuilder()
                << TMethod::Name << ": " << msg->Record.ShortUtf8DebugString()
                << ", entry: " << e->ShortUtf8DebugString());
            *response->Record.MutableError() =
                MakeError(E_INVALID_STATE, std::move(message));
        }
        auto error = response->GetError();
        NCloud::Reply(ctx, *ev, std::move(response));
        onReply(error);
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddInFlightRequest<TMethod>(*requestInfo);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s LinkNodeInShard: %s",
        LogTag.c_str(),
        msg->Record.ShortUtf8DebugString().Quote().c_str());

    ExecuteTx<TLinkNodeInShard>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record),
        std::move(profileLogRequest));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_LinkNodeInShard(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TLinkNodeInShard& args)
{
    Y_UNUSED(ctx);

    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    if (!ReadNode(*db, args.Request.GetNodeId(), args.CommitId, args.Node)) {
        return false;   // not ready
    }

    if (!args.Node) {
        args.Error = ErrorInvalidTarget(args.Request.GetNodeId());
        return true;
    }

    if (args.Node->Attrs.GetType() == NProto::E_DIRECTORY_NODE) {
        args.Error = ErrorIsDirectory(args.Request.GetNodeId());
        return true;
    }

    if (args.Node->Attrs.GetLinks() + 1 > MaxLink) {
        args.Error = ErrorMaxLink(args.Request.GetNodeId());
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_LinkNodeInShard(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TLinkNodeInShard& args)
{
    FILESTORE_VALIDATE_TX_ERROR(LinkNodeInShard, args);

    auto db = CreateIndexTabletDatabaseProxy(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    auto attrs = CopyAttrs(args.Node->Attrs, E_CM_CMTIME | E_CM_REF);
    UpdateNode(
        *db,
        args.Request.GetNodeId(),
        args.Node->MinCommitId,
        args.CommitId,
        attrs,
        args.Node->Attrs);

    ConvertNodeFromAttrs(
        *args.Response.MutableNode(),
        args.Request.GetNodeId(),
        attrs);

    args.Node->Attrs = std::move(attrs);

    // Persist the response so a resend of this (ClientTabletId, RequestId)
    // returns it without bumping the link count again. Only written on success -
    // an errored attempt made no durable change and can safely be re-run.
    args.ResponseLogEntry.SetClientTabletId(
        args.Request.GetHeaders().GetInternal().GetClientTabletId());
    args.ResponseLogEntry.SetRequestId(
        args.Request.GetHeaders().GetRequestId());
    args.ResponseLogEntry.SetTimestampMs(ctx.Now().MilliSeconds());
    *args.ResponseLogEntry.MutableLinkNodeInShardResponse() = args.Response;
    WriteResponseLogEntry(*db, args.ResponseLogEntry);
}

void TIndexTabletActor::CompleteTx_LinkNodeInShard(
    const TActorContext& ctx,
    TTxIndexTablet::TLinkNodeInShard& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    if (!HasError(args.Error)) {
        CommitResponseLogEntry(std::move(args.ResponseLogEntry));

        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s LinkNodeInShard completed: node %lu, links %u",
            LogTag.c_str(),
            args.Request.GetNodeId(),
            args.Response.GetNode().GetLinks());
    } else {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s LinkNodeInShard failed: node %lu, error %s",
            LogTag.c_str(),
            args.Request.GetNodeId(),
            FormatError(args.Error).Quote().c_str());
    }

    using TMethod = TEvIndexTablet::TLinkNodeInShardMethod;

    auto response = std::make_unique<TMethod::TResponse>(args.Error);
    if (!HasError(args.Error)) {
        response->Record = std::move(args.Response);
    }

    CompleteResponse<TMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        args.Error,
        ProfileLog);
}

}   // namespace NCloud::NFileStore::NStorage
