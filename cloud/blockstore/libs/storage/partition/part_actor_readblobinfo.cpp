#include "part_actor.h"

#include "part_readblobinfo_logic.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr::NTabletFlatExecutor;

using TOutputIndex = TTxPartition::TCompactionReadBlobInfo::TOutputIndex;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleCompactionReadBlobInfo(
    const TEvPartitionPrivate::TEvCompactionReadBlobInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "CompactionReadBlobInfo",
        requestInfo->CallContext->RequestId);

    auto blobsToOutputIndices = DeduplicateBlobInfos(
        msg->BlobsToReadBlockMasks,
        msg->BlobsToReadBlobMetas);

    AddTransaction<TEvPartitionPrivate::TCompactionReadBlobInfoMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TCompactionReadBlobInfo>(
            requestInfo,
            std::move(blobsToOutputIndices),
            msg->BlobsToReadBlockMasks.size(),
            msg->BlobsToReadBlobMetas.size()));
}

bool TPartitionActor::PrepareCompactionReadBlobInfo(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompactionReadBlobInfo& args)
{
    Y_UNUSED(ctx);
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    args.BlockMasks.resize(args.BlockMaskCount);
    args.BlobMetas.resize(args.BlobMetaCount);

    return ReadBlobsInfo(
        db,
        args.BlobsToOutputIndices,
        TabletID(),
        args.BlockMasks,
        args.BlobMetas);
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
        args.BlockMasks.size() == args.BlockMaskCount,
        TWellKnownEntityTypes::TABLET,
        TabletID(),
        TStringBuilder() << "Block masks size mismatch: "
                         << args.BlockMasks.size()
                         << " != " << args.BlockMaskCount);
    STORAGE_VERIFY_C(
        args.BlobMetas.size() == args.BlobMetaCount,
        TWellKnownEntityTypes::TABLET,
        TabletID(),
        TStringBuilder() << "Blob metas size mismatch: "
                         << args.BlobMetas.size()
                         << " != " << args.BlobMetaCount);

    State->IncrementBlockMaskReadDuringCompaction(args.BlockMaskCount);

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
