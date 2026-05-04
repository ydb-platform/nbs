#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AddDataUnconfirmed(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAddDataUnconfirmed& args)
{
    Y_UNUSED(ctx);

    if (!UnconfirmedDataInProgress.contains(args.CommitId)) {
        ReportUnconfirmedDataNotInProgress(
            TStringBuilder()
            << "tabletId: " << TabletID() << ", commitId: " << args.CommitId);
        args.Error = MakeError(E_FAIL, "Unconfirmed data not in progress");
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET,
            "%s AddDataUnconfirmed failed: commitId=%lu, unconfirmed data not "
            "in progress",
            LogTag.c_str(),
            args.CommitId);
        return true;
    }

    if (DeletionQueue.contains(args.CommitId)) {
        args.Error = MakeError(E_REJECTED, "Already deleted");
        LOG_WARN(
            ctx,
            TFileStoreComponents::TABLET,
            "%s AddDataUnconfirmed rejected: commitId=%lu, already deleted",
            LogTag.c_str(),
            args.CommitId);
        return true;
    }
    return ValidateAddDataRequest(tx, args);
}

void TIndexTabletActor::ExecuteTx_AddDataUnconfirmed(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAddDataUnconfirmed& args)
{
    if (HasError(args.Error)) {
        return;
    }

    TIndexTabletDatabase db(tx.DB);

    auto& data = UnconfirmedDataInProgress[args.CommitId].Data;

    data.SetNodeId(args.NodeId);

    // Write serialized proto to DB for crash recovery
    db.WriteUnconfirmedData(args.CommitId, data);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s AddDataUnconfirmed tx: commitId=%lu, nodeId=%lu, blobs=%d",
        LogTag.c_str(),
        args.CommitId,
        args.NodeId,
        data.BlobIdsSize());
}

void TIndexTabletActor::CompleteTx_AddDataUnconfirmed(
    const TActorContext& ctx,
    TTxIndexTablet::TAddDataUnconfirmed& args)
{
    auto inProgressIt = UnconfirmedDataInProgress.find(args.CommitId);
    TABLET_VERIFY(inProgressIt != UnconfirmedDataInProgress.end());

    const ui64 requestBytes = inProgressIt->second.Data.GetLength();
    const bool deletionInProgress = DeletionQueue.contains(args.CommitId);

    Y_DEFER
    {
        UnconfirmedDataInProgress.erase(inProgressIt);

        FinalizeProfileLogRequestInfo(
            std::move(args.ProfileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            args.Error,
            ProfileLog);

        if (!HasError(args.Error)) {
            Metrics.AddDataUnconfirmed.Update(
                1,
                requestBytes,
                ctx.Now() - args.RequestInfo->StartedTs);
        }
    };

    if (HasError(args.Error)) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::TABLET,
            "%s AddDataUnconfirmed failed: commitId=%lu, %s",
            LogTag.c_str(),
            args.CommitId,
            FormatError(args.Error).c_str());

        if (!deletionInProgress) {
            TABLET_VERIFY(TryReleaseCollectBarrier(args.CommitId));
            SendPendingConfirmAddDataResponse(ctx, args.CommitId, args.Error);
        }
    } else {
        LOG_TRACE(
            ctx,
            TFileStoreComponents::TABLET,
            "%s AddDataUnconfirmed complete: commitId=%lu, nodeId=%lu",
            LogTag.c_str(),
            args.CommitId,
            args.NodeId);

        // Keep deferred reply until DeleteUnconfirmedData completion if
        // deletion in progress
        if (!deletionInProgress) {
            UnconfirmedData.emplace(
                args.CommitId,
                std::move(inProgressIt->second));

            // Check if ConfirmAddData was received while we were executing.
            auto pendingIt = PendingConfirmation.find(args.CommitId);
            if (pendingIt != PendingConfirmation.end()) {
                // AddBlob Execute will send the deferred ConfirmAddData reply.
                ConfirmData(args.CommitId, ctx);
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
