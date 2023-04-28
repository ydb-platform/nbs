#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddGarbage(
    const TEvPartitionPrivate::TEvAddGarbageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvPartitionPrivate::TAddGarbageMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    TRequestScope timer(*requestInfo);

    BLOCKSTORE_TRACE_RECEIVED(ctx, &requestInfo->TraceId, this, msg);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "AddGarbage",
        requestInfo->CallContext->RequestId);

    AddTransaction(*requestInfo);

    ExecuteTx<TAddGarbage>(
        ctx,
        requestInfo,
        std::move(msg->BlobIds));
}

bool TPartitionActor::PrepareAddGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddGarbage& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    return db.ReadKnownBlobIds(args.KnownBlobIds)
        && db.ReadGarbageBlobs(args.KnownBlobIds);
}

void TPartitionActor::ExecuteAddGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddGarbage& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    Y_VERIFY(IsSorted(args.BlobIds.begin(), args.BlobIds.end()));
    SortUnique(args.KnownBlobIds);

    TVector<TPartialBlobId> diff;
    std::set_difference(
        args.BlobIds.begin(), args.BlobIds.end(),
        args.KnownBlobIds.begin(), args.KnownBlobIds.end(),
        std::inserter(diff, diff.begin()));

    auto& garbageQueue = State->GetGarbageQueue();
    for (const auto& blobId: diff) {
        if (!IsDeletionMarker(blobId)) {
            LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Add garbage blob: %s",
                TabletID(),
                ToString(MakeBlobId(TabletID(), blobId)).data());

            bool added = garbageQueue.AddGarbageBlob(blobId);
            Y_VERIFY(added);

            db.WriteGarbageBlob(blobId);
        }
    }
}

void TPartitionActor::CompleteAddGarbage(
    const TActorContext& ctx,
    TTxPartition::TAddGarbage& args)
{
    TRequestScope timer(*args.RequestInfo);

    auto response = std::make_unique<TEvPartitionPrivate::TEvAddGarbageResponse>();

    BLOCKSTORE_TRACE_SENT(ctx, &args.RequestInfo->TraceId, this, response);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "AddGarbage",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    EnqueueCollectGarbageIfNeeded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
