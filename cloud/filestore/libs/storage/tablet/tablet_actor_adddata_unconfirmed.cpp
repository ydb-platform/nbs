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
        ReportUnconfirmedDataNotInProgress(TStringBuilder()
            << "tabletId: " << TabletID()
            << ", commitId: " << args.CommitId);
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
    ui64 requestBytes = UnconfirmedDataInProgress[args.CommitId].Data.GetLength();

    Y_DEFER
    {
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

    const bool deleteOnTxComplete = DeletionQueue.contains(args.CommitId);

    if (HasError(args.Error)) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::TABLET,
            "%s AddDataUnconfirmed failed: commitId=%lu, %s",
            LogTag.c_str(),
            args.CommitId,
            FormatError(args.Error).c_str());

        DeletionQueue.erase(args.CommitId);

        TABLET_VERIFY(TryReleaseCollectBarrier(args.CommitId));
    } else {
        auto inProgressIt = UnconfirmedDataInProgress.find(args.CommitId);
        TABLET_VERIFY(inProgressIt != UnconfirmedDataInProgress.end());
        UnconfirmedData.emplace(
            args.CommitId,
            std::move(inProgressIt->second));
        UnconfirmedDataInProgress.erase(inProgressIt);

        if (deleteOnTxComplete) {
            ExecuteTx<TDeleteUnconfirmedData>(
                ctx,
                CreateRequestInfo(SelfId(), 0, MakeIntrusive<TCallContext>()),
                TVector<ui64>{args.CommitId});
        }

        LOG_TRACE(
            ctx,
            TFileStoreComponents::TABLET,
            "%s AddDataUnconfirmed complete: commitId=%lu, nodeId=%lu",
            LogTag.c_str(),
            args.CommitId,
            args.NodeId);
    }

    // Check if ConfirmAddData was received while we were executing.
    auto pendingIt = PendingConfirmation.find(args.CommitId);
    if (pendingIt != PendingConfirmation.end()) {
        if (!HasError(args.Error)) {
            if (deleteOnTxComplete) {
                // Keep deferred reply until DeleteUnconfirmedData completion.
                return;
            }

            // AddBlob Execute will send the deferred ConfirmAddData reply.
            ConfirmData(args.CommitId, ctx);
            return;
        }

        SendPendingConfirmAddDataResponse(ctx, args.CommitId, args.Error);
    }
}

}   // namespace NCloud::NFileStore::NStorage
