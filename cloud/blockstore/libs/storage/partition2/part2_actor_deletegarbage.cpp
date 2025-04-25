#include "part2_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDeleteGarbage(
    const TEvPartitionPrivate::TEvDeleteGarbageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "DeleteGarbage",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvPartitionPrivate::TDeleteGarbageMethod>(*requestInfo);

    ExecuteTx<TDeleteGarbage>(
        ctx,
        requestInfo,
        msg->CommitId,
        std::move(msg->NewBlobs),
        std::move(msg->GarbageBlobs));
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareDeleteGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteGarbage& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteDeleteGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteGarbage& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    auto& garbageQueue = State->GetGarbageQueue();

    i64 newBlobBytes = 0;
    for (const auto& blobId: args.NewBlobs) {
        newBlobBytes += blobId.BlobSize();

        bool deleted = garbageQueue.RemoveNewBlob(blobId);
        Y_ABORT_UNLESS(deleted);
    }

    i64 garbageBlobBytes = 0;
    for (const auto& blobId: args.GarbageBlobs) {
        garbageBlobBytes += blobId.BlobSize();

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Delete garbage blob: %s",
            TabletID(),
            ToString(MakeBlobId(TabletID(), blobId)).data());

        bool deleted = garbageQueue.RemoveGarbageBlob(blobId);
        Y_ABORT_UNLESS(deleted);

        db.DeleteGarbageBlob(blobId);
    }

    UpdateStorageStats(ctx, newBlobBytes - garbageBlobBytes);

    State->SetLastCollectCommitId(args.CommitId);

    State->WriteStats(db);
}

void TPartitionActor::CompleteDeleteGarbage(
    const TActorContext& ctx,
    TTxPartition::TDeleteGarbage& args)
{
    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete delete garbage @%lu",
        TabletID(),
        args.CommitId);

    auto response =
        std::make_unique<TEvPartitionPrivate::TEvDeleteGarbageResponse>();
    response->ExecCycles = args.RequestInfo->GetExecCycles();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "DeleteGarbage",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    auto time = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.DeleteGarbage.AddRequest(time);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
