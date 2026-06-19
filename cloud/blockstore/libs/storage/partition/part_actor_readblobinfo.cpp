#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleCompactionReadBlobInfo(
    const TEvPartitionPrivate::TEvCompactionReadBlobInfoRequest::TPtr& ev,
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
        "CompactionReadBlobInfo",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvPartitionPrivate::TCompactionReadBlobInfoMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TCompactionReadBlobInfo>(
            requestInfo,
            std::move(msg->BlobsToReadBlockMasks),
            std::move(msg->BlobsToReadBlobMetas)));
}

bool TPartitionActor::PrepareCompactionReadBlobInfo(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompactionReadBlobInfo& args)
{
    Y_UNUSED(ctx);
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    bool ready = true;

    for (size_t i = 0; i < args.BlobsToReadBlockMasks.size(); ++i) {
        TMaybe<TBlockMask> mask;
        if (!db.ReadBlockMask(args.BlobsToReadBlockMasks[i], mask)) {
            ready = false;
            continue;
        }
        STORAGE_VERIFY_C(
            mask.Defined(),
            TWellKnownEntityTypes::TABLET,
            TabletID(),
            TStringBuilder()
                << "Could not read block mask for blob: "
                << MakeBlobId(TabletID(), args.BlobsToReadBlockMasks[i]));
        args.BlockMasks.emplace_back(*mask);
    }

    for (size_t i = 0; i < args.BlobsToReadBlobMetas.size(); ++i) {
        TMaybe<NProto::TBlobMeta> meta;
        if (!db.ReadBlobMeta(args.BlobsToReadBlobMetas[i], meta)) {
            ready = false;
            continue;
        }
        STORAGE_VERIFY_C(
            meta.Defined(),
            TWellKnownEntityTypes::TABLET,
            TabletID(),
            TStringBuilder()
                << "Could not read blob meta for blob: "
                << MakeBlobId(TabletID(), args.BlobsToReadBlobMetas[i]));
        args.BlobMetas.emplace_back(std::move(*meta));
    }

    return ready;
}

void TPartitionActor::ExecuteCompactionReadBlobInfo(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompactionReadBlobInfo& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteCompactionReadBlobInfo(
    const TActorContext& ctx,
    TTxPartition::TCompactionReadBlobInfo& args)
{
    STORAGE_VERIFY_C(
        args.BlockMasks.size() == args.BlobsToReadBlockMasks.size(),
        TWellKnownEntityTypes::TABLET,
        TabletID(),
        TStringBuilder() << "Block masks size mismatch: "
                         << args.BlockMasks.size()
                         << " != " << args.BlobsToReadBlockMasks.size());
    STORAGE_VERIFY_C(
        args.BlobMetas.size() == args.BlobsToReadBlobMetas.size(),
        TWellKnownEntityTypes::TABLET,
        TabletID(),
        TStringBuilder() << "Blob metas size mismatch: "
                         << args.BlobMetas.size()
                         << " != " << args.BlobsToReadBlobMetas.size());

    State->IncrementBlockMaskReadDuringCompaction(
        args.BlobsToReadBlockMasks.size());

    TRequestScope timer(*args.RequestInfo);

    auto response = std::make_unique<
        TEvPartitionPrivate::TEvCompactionReadBlobInfoResponse>();
    response->BlockMasksForBlobs = std::move(args.BlockMasks);
    response->BlobMetasForBlobs = std::move(args.BlobMetas);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "CompactionReadBlobInfo",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
