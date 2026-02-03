#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ReplayOpLog(
    const NActors::TActorContext& ctx,
    const TVector<NProto::TOpLogEntry>& opLog)
{
    // TODO(#1350): requests to shards should be ordered, otherwise weird
    // races are possible, e.g. create + unlink ops for the same node can arrive
    // in opposite order (original requests won't come in opposite order, but
    // if one of those requests gets rejected and is retried, the final order
    // may change)
    for (const auto& op: opLog) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s OpLog replay: %s",
            LogTag.c_str(),
            op.ShortUtf8DebugString().Quote().c_str());

        if (op.HasCreateNodeRequest()) {
            RegisterCreateNodeInShardActor(
                ctx,
                nullptr, // requestInfo
                op.GetCreateNodeRequest(),
                op.GetRequestId(), // needed for DupCache update (only for
                                   // shard requests originating from
                                   // CreateNode requests)
                op.GetEntryId(),
                {} // response
            );
        } else if (op.HasUnlinkNodeRequest()) {
            // This case is kept for backward compatibility with old oplog
            // entries
            NProtoPrivate::TUnlinkNodeInShardRequest request;
            request.MutableHeaders()->CopyFrom(
                op.GetUnlinkNodeRequest().GetHeaders());
            request.SetFileSystemId(
                op.GetUnlinkNodeRequest().GetFileSystemId());
            request.SetNodeId(op.GetUnlinkNodeRequest().GetNodeId());
            request.SetName(op.GetUnlinkNodeRequest().GetName());
            request.SetUnlinkDirectory(
                op.GetUnlinkNodeRequest().GetUnlinkDirectory());

            RegisterUnlinkNodeInShardActor(
                ctx,
                nullptr,   // requestInfo
                std::move(request),
                NProto::TProfileLogRequestInfo{},
                0,   // requestId
                op.GetEntryId(),
                {},     // result
                false   // shouldUnlockUponCompletion
            );
        } else if (op.HasUnlinkNodeInShardRequest()) {
            bool shouldUnlockUponCompletion =
                op.GetUnlinkNodeInShardRequest().GetUnlinkDirectory() &&
                GetFileSystem().GetDirectoryCreationInShardsEnabled();
            if (shouldUnlockUponCompletion) {
                // There is a need to unlock the node ref after the operation is
                // completed, because the node ref should have been locked
                const auto& originalRequest =
                    op.GetUnlinkNodeInShardRequest().GetOriginalRequest();
                const bool locked = TryLockNodeRef(
                    {originalRequest.GetNodeId(), originalRequest.GetName()});
                if (!locked) {
                    ReportFailedToLockNodeRef(
                        TStringBuilder()
                        << "Request: " << op.ShortUtf8DebugString());
                }
            }

            NProto::TProfileLogRequestInfo profileLogRequest;
            const auto& serializedRequest = op.GetProfileLogRequest();
            if (serializedRequest
                    && !profileLogRequest.ParseFromString(serializedRequest))
            {
                ReportBrokenProfileLogRequest(TStringBuilder() << "Replayed"
                    << " OpLogEntry: " << op.ShortUtf8DebugString().Quote());
            }

            RegisterUnlinkNodeInShardActor(
                ctx,
                nullptr,   // requestInfo
                op.GetUnlinkNodeInShardRequest(),
                std::move(profileLogRequest),
                0,   // requestId
                op.GetEntryId(),
                {},   // result
                shouldUnlockUponCompletion);
        } else if (op.HasRenameNodeInDestinationRequest()) {
            const auto& request = op.GetRenameNodeInDestinationRequest();
            const bool locked = TryLockNodeRef({
                request.GetOriginalRequest().GetNodeId(),
                request.GetOriginalRequest().GetName()});
            if (!locked) {
                ReportFailedToLockNodeRef(TStringBuilder() << "Request: "
                    << request.GetOriginalRequest().ShortUtf8DebugString());
            }

            NProto::TProfileLogRequestInfo profileLogRequest;
            InitTabletProfileLogRequestInfo(
                profileLogRequest,
                EFileStoreRequest::RenameNode,
                request.GetOriginalRequest(),
                ctx.Now());

            RegisterRenameNodeInDestinationActor(
                ctx,
                nullptr, // requestInfo
                request,
                std::move(profileLogRequest),
                0, // requestId (TODO(#2674): either idempotency or use real
                   // requestId)
                op.GetEntryId());
        } else if (op.HasAbortUnlinkDirectoryNodeInShardRequest()) {
            const auto& request =
                op.GetAbortUnlinkDirectoryNodeInShardRequest();
            const bool locked = TryLockNodeRef({
                request.GetOriginalRequest().GetNewParentId(),
                request.GetOriginalRequest().GetNewName()});
            if (!locked) {
                ReportFailedToLockNodeRef(TStringBuilder() << "Request: "
                    << request.GetOriginalRequest().ShortUtf8DebugString());
            }

            NProto::TProfileLogRequestInfo profileLogRequest;
            InitTabletProfileLogRequestInfo(
                profileLogRequest,
                EFileStoreRequest::RenameNode,
                request.GetOriginalRequest(),
                ctx.Now());

            RegisterAbortUnlinkDirectoryInShardActor(
                ctx,
                nullptr, // requestInfo
                request.GetOriginalRequest(),
                std::move(profileLogRequest),
                {}, // originalError, doesn't matter w/o requestInfo
                request.GetFileSystemId(),
                request.GetNodeId(),
                op.GetEntryId());
        } else {
            const TString message = ReportUnknownOpLogEntry(
                TStringBuilder() << "OpLogEntry: " << op.DebugString().Quote());

            LOG_ERROR(
                ctx,
                TFileStoreComponents::TABLET,
                "%s %s",
                LogTag.c_str(),
                message.c_str());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDeleteOpLogEntry(
    const TEvIndexTabletPrivate::TEvDeleteOpLogEntryRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddInFlightRequest<TEvIndexTabletPrivate::TDeleteOpLogEntryMethod>(
        *requestInfo);

    ExecuteTx<TDeleteOpLogEntry>(
        ctx,
        std::move(requestInfo),
        msg->OpLogEntryId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    db.DeleteOpLogEntry(args.EntryId);
}

void TIndexTabletActor::CompleteTx_DeleteOpLogEntry(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteOpLogEntry& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s DeleteOpLogEntry completed (%lu)",
        LogTag.c_str(),
        args.EntryId);

    if (args.RequestInfo) {
        RemoveInFlightRequest(*args.RequestInfo);
        using TResponse = TEvIndexTabletPrivate::TEvDeleteOpLogEntryResponse;
        auto response = std::make_unique<TResponse>();
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGetOpLogEntry(
    const TEvIndexTabletPrivate::TEvGetOpLogEntryRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddInFlightRequest<TEvIndexTabletPrivate::TGetOpLogEntryMethod>(
        *requestInfo);

    ExecuteTx<TGetOpLogEntry>(
        ctx,
        std::move(requestInfo),
        msg->OpLogEntryId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_GetOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TGetOpLogEntry& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);
    return db.ReadOpLogEntry(args.EntryId, args.Entry);
}

void TIndexTabletActor::ExecuteTx_GetOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TGetOpLogEntry& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TIndexTabletActor::CompleteTx_GetOpLogEntry(
    const TActorContext& ctx,
    TTxIndexTablet::TGetOpLogEntry& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s GetOpLogEntry completed (%lu): %s",
        LogTag.c_str(),
        args.EntryId,
        args.Entry
            ? args.Entry->ShortUtf8DebugString().Quote().c_str()
            : "<null>");

    auto response =
        std::make_unique<TEvIndexTabletPrivate::TEvGetOpLogEntryResponse>();
    response->OpLogEntry = std::move(args.Entry);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWriteOpLogEntry(
    const TEvIndexTabletPrivate::TEvWriteOpLogEntryRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddInFlightRequest<TEvIndexTabletPrivate::TWriteOpLogEntryMethod>(
        *requestInfo);

    ExecuteTx<TWriteOpLogEntry>(
        ctx,
        std::move(requestInfo),
        std::move(msg->OpLogEntry));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_WriteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteOpLogEntry& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_WriteOpLogEntry(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteOpLogEntry& args)
{
    Y_UNUSED(ctx);

    const ui64 commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    TIndexTabletDatabase db(tx.DB);
    args.Entry.SetEntryId(commitId);
    db.WriteOpLogEntry(args.Entry);
}

void TIndexTabletActor::CompleteTx_WriteOpLogEntry(
    const TActorContext& ctx,
    TTxIndexTablet::TWriteOpLogEntry& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TFileStoreComponents::TABLET,
            "%s WriteOpLogEntry failed: %s, error: %s",
            LogTag.c_str(),
            args.Entry.ShortUtf8DebugString().Quote().c_str(),
            FormatError(args.Error).Quote().c_str());
    } else {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s WriteOpLogEntry completed: %s",
            LogTag.c_str(),
            args.Entry.ShortUtf8DebugString().Quote().c_str());
    }

    using TResponse = TEvIndexTabletPrivate::TEvWriteOpLogEntryResponse;

    auto response = std::make_unique<TResponse>(std::move(args.Error));
    response->OpLogEntryId = args.Entry.GetEntryId();
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
