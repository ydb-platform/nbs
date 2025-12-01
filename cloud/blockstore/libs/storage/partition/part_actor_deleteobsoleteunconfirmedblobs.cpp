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

void TPartitionActor::EnqueueDeleteObsoleteUnconfirmedBlobsIfNeeded(
    const TActorContext& ctx)
{
    if (State->GetObsoleteUnconfirmedBlobsState().Status !=
        EOperationStatus::Idle)
    {
        // already enqueued
        return;
    }

    if (State->GetObsoleteUnconfirmedBlobs().empty()) {
        // not ready
        return;
    }

    State->GetObsoleteUnconfirmedBlobsState().SetStatus(
        EOperationStatus::Enqueued);

    auto request = std::make_unique<
        TEvPartitionPrivate::TEvDeleteObsoleteUnconfirmedBlobsRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()));

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s DeleteObsoleteUnconfirmedBlobs request sent: %lu",
        LogTitle.GetWithTime().c_str(),
        request->CallContext->RequestId);

    NCloud::Send(ctx, SelfId(), std::move(request));
}

void TPartitionActor::HandleDeleteObsoleteUnconfirmedBlobs(
    const TEvPartitionPrivate::TEvDeleteObsoleteUnconfirmedBlobsRequest::TPtr&
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
        "DeleteObsoleteUnconfirmedBlobs",
        requestInfo->CallContext->RequestId);

    State->GetObsoleteUnconfirmedBlobsState().SetStatus(
        EOperationStatus::Started);

    AddTransaction<TEvPartitionPrivate::TDeleteObsoleteUnconfirmedBlobsMethod>(
        *requestInfo);

    ExecuteTx(ctx, CreateTx<TDeleteObsoleteUnconfirmedBlobs>(requestInfo));
}

bool TPartitionActor::PrepareDeleteObsoleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteObsoleteUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteDeleteObsoleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteObsoleteUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TPartitionDatabase db(tx.DB);

    const auto& obsoleteBlobs = State->GetObsoleteUnconfirmedBlobs();

    for (const auto& [commitId, blobs]: obsoleteBlobs) {
        for (const auto& blob: blobs) {
            auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
            db.DeleteUnconfirmedBlob(blobId);
        }

        State->GetGarbageQueue().ReleaseBarrier(commitId);
        State->GetCommitQueue().ReleaseBarrier(commitId);
    }

    State->ClearObsoleteUnconfirmedBlobs();
}

void TPartitionActor::CompleteDeleteObsoleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTxPartition::TDeleteObsoleteUnconfirmedBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);

    State->GetObsoleteUnconfirmedBlobsState().SetStatus(EOperationStatus::Idle);

    RemoveTransaction(*args.RequestInfo);

    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());
    auto time =
        CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.DeleteObsoleteUnconfirmedBlobs.AddRequest(
        time);

    EnqueueDeleteObsoleteUnconfirmedBlobsIfNeeded(ctx);
    ProcessCommitQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
