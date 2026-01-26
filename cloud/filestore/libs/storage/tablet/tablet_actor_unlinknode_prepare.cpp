#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandlePrepareUnlinkDirectoryNodeInShard(
    const TEvIndexTablet::TEvPrepareUnlinkDirectoryNodeInShardRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TPrepareUnlinkDirectoryNodeInShardMethod>(
        *requestInfo);

    ExecuteTx<TPrepareUnlinkDirectoryNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_PrepareUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TPrepareUnlinkDirectoryNode& args)
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
        args.Error = ErrorInvalidTarget(args.Request.GetNodeId());
        return true;
    }

    if (!args.Node) {
        auto message = ReportNodeNotFoundInShard(TStringBuilder()
            << "PrepareUnlinkDirectoryNode: "
            << args.Request.ShortDebugString());
        args.Error = MakeError(E_INVALID_STATE, std::move(message));
        return true;
    }

    if (args.Node->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
        auto message = ReportIsNotDirectory(TStringBuilder()
            << "PrepareUnlinkDirectoryNode: "
            << args.Request.ShortDebugString());
        args.Error = ErrorIsNotDirectory(args.Request.GetNodeId());
        return true;
    }


    TVector<IIndexTabletDatabase::TNodeRef> refs;
    // 1 entry is enough to prevent deletion
    ready = ReadNodeRefs(
        db,
        args.Request.GetNodeId(),
        args.CommitId,
        {},
        refs,
        1);
    if (!ready) {
        return false;
    }

    if (!refs.empty()) {
        args.Error = ErrorIsNotEmpty(args.Request.GetNodeId());
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_PrepareUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TPrepareUnlinkDirectoryNode& args)
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
    attrs.SetIsPreparedForUnlink(true);
    UpdateNode(
        db,
        args.Request.GetNodeId(),
        args.Node->MinCommitId,
        args.CommitId,
        attrs,
        args.Node->Attrs);
}

void TIndexTabletActor::CompleteTx_PrepareUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTxIndexTablet::TPrepareUnlinkDirectoryNode& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s[%s] PrepareUnlinkDirectoryNode completed (%s)",
        LogTag.c_str(),
        args.SessionId.c_str(),
        FormatError(args.Error).c_str());

    InvalidateNodeCaches(args.Request.GetNodeId());

    RemoveTransaction(*args.RequestInfo);
    EnqueueBlobIndexOpIfNeeded(ctx);

    using TResponse =
        TEvIndexTablet::TEvPrepareUnlinkDirectoryNodeInShardResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    CompleteResponse<TEvIndexTablet::TPrepareUnlinkDirectoryNodeInShardMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
