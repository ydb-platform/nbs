#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAbortUnlinkDirectoryNodeInShard(
    const TEvIndexTablet::TEvAbortUnlinkDirectoryNodeInShardRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TAbortUnlinkDirectoryNodeInShardMethod>(
        *requestInfo);

    ExecuteTx<TAbortUnlinkDirectoryNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AbortUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAbortUnlinkDirectoryNode& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    bool ready = ReadNode(
        db,
        args.Request.GetNodeId(),
        args.CommitId,
        args.Node);
    if (!ready) {
        return false;
    }

    if (!args.Node) {
        auto message = ReportNodeNotFoundInShard(TStringBuilder()
            << "AbortUnlinkDirectoryNode: "
            << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return true;
    }

    if (args.Node->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
        auto message = ReportIsNotDirectory(TStringBuilder()
            << "AbortUnlinkDirectoryNode: "
            << args.Request.ShortDebugString());
        args.Error = ErrorIsNotDirectory(args.Request.GetNodeId());
        return true;
    }

    if (!args.Node->Attrs.GetIsPreparedForUnlink()) {
        //
        // Should be safe to continue the operation but a crit event + a log
        // message will be useful.
        //

        auto message = ReportNotPreparedForUnlink(TStringBuilder()
            << "AbortUnlinkDirectoryNode: "
            << args.Request.ShortDebugString());

        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s[%s] %s",
            LogTag.c_str(),
            args.SessionId.c_str(),
            message.Quote().c_str());
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_AbortUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAbortUnlinkDirectoryNode& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_ERROR(UnlinkNode, args);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    NProto::TNode attrs = CopyAttrs(args.Node->Attrs, E_CM_CTIME);
    attrs.SetIsPreparedForUnlink(false);
    UpdateNode(
        db,
        args.Request.GetNodeId(),
        args.Node->MinCommitId,
        args.CommitId,
        attrs,
        args.Node->Attrs);
}

void TIndexTabletActor::CompleteTx_AbortUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTxIndexTablet::TAbortUnlinkDirectoryNode& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s[%s] AbortUnlinkDirectoryNode completed (%s)",
        LogTag.c_str(),
        args.SessionId.c_str(),
        FormatError(args.Error).c_str());

    InvalidateNodeCaches(args.Request.GetNodeId());

    RemoveTransaction(*args.RequestInfo);
    EnqueueBlobIndexOpIfNeeded(ctx);

    using TResponse =
        TEvIndexTablet::TEvAbortUnlinkDirectoryNodeInShardResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    CompleteResponse<TEvIndexTablet::TAbortUnlinkDirectoryNodeInShardMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
