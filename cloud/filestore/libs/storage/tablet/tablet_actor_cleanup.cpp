#include "tablet_actor.h"

#include "profile_log_events.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCleanup(
    const TEvIndexTabletPrivate::TEvCleanupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        msg->CallContext,
        "Cleanup",
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    auto replyError = [&] (const NProto::TError& error)
    {
        FILESTORE_TRACK(
            ResponseSent_Tablet,
            msg->CallContext,
            "Cleanup");

        if (ev->Sender == ctx.SelfID) {
            // nothing to do
            return;
        }

        auto response = std::make_unique<TEvIndexTabletPrivate::TEvCleanupResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (!CompactionStateLoadStatus.Finished) {
        if (BlobIndexOpState.GetOperationState() == EOperationState::Enqueued) {
            BlobIndexOpState.Complete();
        }

        replyError(MakeError(E_TRY_AGAIN, "compaction state not loaded yet"));
        return;
    }

    if (!BlobIndexOpState.Start()) {
        replyError(
            MakeError(E_TRY_AGAIN, "cleanup/compaction is in progress"));
        return;
    }

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Cleanup started (range: #%u)",
        LogTag.c_str(),
        msg->RangeId);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TCleanup>(
        ctx,
        std::move(requestInfo),
        msg->RangeId,
        GetCurrentCommitId());
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_Cleanup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCleanup& args)
{
    InitProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GetCurrentCommitId();

    return LoadMixedBlocks(db, args.RangeId);
}

void TIndexTabletActor::ExecuteTx_Cleanup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCleanup& args)
{
    Y_UNUSED(ctx);

    // needed to prevent the blobs updated during this tx from being deleted
    // before this tx completes
    AcquireCollectBarrier(args.CollectBarrier);

    TIndexTabletDatabase db(tx.DB);

    args.ProcessedDeletionMarkerCount =
        CleanupBlockDeletions(db, args.RangeId, args.ProfileLogRequest);
}

void TIndexTabletActor::CompleteTx_Cleanup(
    const TActorContext& ctx,
    TTxIndexTablet::TCleanup& args)
{
    // log event
    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        {},
        ProfileLog);

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Cleanup completed (range: #%u)",
        LogTag.c_str(), args.RangeId);

    BlobIndexOpState.Complete();
    ReleaseMixedBlocks(args.RangeId);
    TABLET_VERIFY(TryReleaseCollectBarrier(args.CollectBarrier));

    FILESTORE_TRACK(
        ResponseSent_Tablet,
        args.RequestInfo->CallContext,
        "Cleanup");

    if (args.RequestInfo->Sender != ctx.SelfID) {
        // reply to caller
        auto response = std::make_unique<TEvIndexTabletPrivate::TEvCleanupResponse>();
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }

    EnqueueBlobIndexOpIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);

    Metrics.Cleanup.Update(
        1,
        args.ProcessedDeletionMarkerCount * GetBlockSize(),
        ctx.Now() - args.RequestInfo->StartedTs);
}

}   // namespace NCloud::NFileStore::NStorage
