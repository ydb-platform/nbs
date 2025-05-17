#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleGetUsedBlocks(
    const TEvVolume::TEvGetUsedBlocksRequest::TPtr& ev,
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
        "GetUsedBlocks",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvVolume::TGetUsedBlocksMethod>(
        *requestInfo,
        ETransactionType::GetUsedBlocks);

    ExecuteTx<TGetUsedBlocks>(
        ctx,
        requestInfo);
}

bool TPartitionActor::PrepareGetUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TGetUsedBlocks& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    return db.ReadUsedBlocksRaw([&args](TCompressedBitmap::TSerializedChunk chunk) {
        if (!TCompressedBitmap::IsZeroChunk(chunk)) {
            auto* usedBlock = args.UsedBlocks.Add();
            usedBlock->SetChunkIdx(chunk.ChunkIdx);
            usedBlock->SetData(chunk.Data.data(), chunk.Data.size());
        }
    });
}

void TPartitionActor::ExecuteGetUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TGetUsedBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteGetUsedBlocks(
    const TActorContext& ctx,
    TTxPartition::TGetUsedBlocks& args)
{
    TRequestScope timer(*args.RequestInfo);

    auto response = std::make_unique<TEvVolume::TEvGetUsedBlocksResponse>();
    response->Record.MutableUsedBlocks()->Swap(&args.UsedBlocks);

    RemoveTransaction(*args.RequestInfo);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "GetUsedBlocks",
        args.RequestInfo->CallContext->RequestId);

    const ui64 responseBytes = response->Record.ByteSizeLong();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    UpdateNetworkStat(ctx.Now(), responseBytes);
    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
