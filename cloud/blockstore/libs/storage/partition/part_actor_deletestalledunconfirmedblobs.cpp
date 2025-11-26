#include "part_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/common/alloc.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueDeleteStalledUnconfirmedBlobsIfNeeded(
    const TActorContext& ctx)
{
    if (State->GetStalledUnconfirmedBlobsState().Status !=
        EOperationStatus::Idle)
    {
        // already enqueued
        return;
    }

    if (State->GetStalledUnconfirmedBlobs().empty()) {
        // not ready
        return;
    }

    State->GetStalledUnconfirmedBlobsState().SetStatus(
        EOperationStatus::Enqueued);

    auto request = std::make_unique<
        TEvPartitionPrivate::TEvDeleteStalledUnconfirmedBlobsRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()));

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s DeleteStalledUnconfirmedBlobs request sent: %lu",
        LogTitle.GetWithTime().c_str(),
        request->CallContext->RequestId);

    NCloud::Send(ctx, SelfId(), std::move(request));
}

void TPartitionActor::HandleDeleteStalledUnconfirmedBlobs(
    const TEvPartitionPrivate::TEvDeleteStalledUnconfirmedBlobsRequest::TPtr&
        ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "DeleteStalledUnconfirmedBlobs",
        requestInfo->CallContext->RequestId);

    State->GetStalledUnconfirmedBlobsState().SetStatus(
        EOperationStatus::Started);

    AddTransaction<TEvPartitionPrivate::TDeleteStalledUnconfirmedBlobsMethod>(
        *requestInfo);

    ExecuteTx(ctx, CreateTx<TDeleteStalledUnconfirmedBlobs>(requestInfo));
}

bool TPartitionActor::PrepareDeleteStalledUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteStalledUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteDeleteStalledUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteStalledUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TPartitionDatabase db(tx.DB);

    const auto& stalledBlobs = State->GetStalledUnconfirmedBlobs();

    for (const auto& [commitId, blobs]: stalledBlobs) {
        for (const auto& blob: blobs) {
            auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
            db.DeleteUnconfirmedBlob(blobId);
        }

        // Release barriers for this commitId
        State->GetGarbageQueue().ReleaseBarrier(commitId);
        State->GetCommitQueue().ReleaseBarrier(commitId);
    }

    // Clear the stalled blobs structure
    State->ClearStalledUnconfirmedBlobs();
}

void TPartitionActor::CompleteDeleteStalledUnconfirmedBlobs(
    const TActorContext& ctx,
    TTxPartition::TDeleteStalledUnconfirmedBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);

    State->GetStalledUnconfirmedBlobsState().SetStatus(EOperationStatus::Idle);

    RemoveTransaction(*args.RequestInfo);

    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());
    auto time =
        CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.DeleteStalledUnconfirmedBlobs.AddRequest(
        time);

    // Barriers were released in Execute, so process commit queue
    ProcessCommitQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
