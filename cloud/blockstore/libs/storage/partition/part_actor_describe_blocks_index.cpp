#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/tablet/blob_id.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlocksIndexVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
    , public IMixedBlocksIndexVisitor
{
private:
    TTxPartition::TDescribeBlocksIndex& Args;

public:
    TDescribeBlocksIndexVisitor(TTxPartition::TDescribeBlocksIndex& args)
        : Args(args)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        if (block.Content) {
            Args.MarkBlock(
                block.Meta.BlockIndex,
                block.Meta.CommitId,
                block.BlobId,
                0);
        }
        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Args.MarkBlock(blockIndex, commitId, blobId, blobOffset);
        return true;
    }

    bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui8 compactionRangeCount) override
    {
        Y_UNUSED(compactionRangeCount);
        Args.MarkBlock(blockIndex, commitId, blobId, blobOffset);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDescribeBlocksIndex(
    const TEvVolume::TEvDescribeBlocksIndexRequest::TPtr& ev,
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
        "DescribeBlocksIndex",
        requestInfo->CallContext->RequestId);

    auto reply = [&](auto response) {
        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "DescribeBlocksIndex",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    };

    if (msg->Record.GetBlocksCount() == 0) {
        reply(std::make_unique<TEvVolume::TEvDescribeBlocksIndexResponse>(
            MakeError(
                E_ARGUMENT,
                "empty block range is forbidden for DescribeBlocksIndex")));
        return;
    }

    const auto range = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    const auto bounds = TBlockRange64::WithLength(
        0,
        State->GetConfig().GetBlocksCount());

    if (!bounds.Overlaps(range)) {
        reply(std::make_unique<TEvVolume::TEvDescribeBlocksIndexResponse>());
        return;
    }

    const auto describeRange = ConvertRangeSafe(bounds.Intersect(range));

    AddTransaction<TEvVolume::TDescribeBlocksIndexMethod>(*requestInfo);

    ExecuteTx<TDescribeBlocksIndex>(
        ctx,
        requestInfo,
        describeRange);
}

bool TPartitionActor::PrepareDescribeBlocksIndex(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeBlocksIndex& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    bool ready = true;

    TDescribeBlocksIndexVisitor visitor(args);
    State->FindFreshBlocks(visitor, args.BlockRange, Max<ui64>());
    ready &= db.FindMixedBlocks(visitor, args.BlockRange, true);
    ready &= db.FindMergedBlocks(
        visitor,
        args.BlockRange,
        true,
        State->GetMaxBlocksInBlob());

    return ready;
}

void TPartitionActor::ExecuteDescribeBlocksIndex(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeBlocksIndex& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteDescribeBlocksIndex(
    const TActorContext& ctx,
    TTxPartition::TDescribeBlocksIndex& args)
{
    TRequestScope timer(*args.RequestInfo);

    auto response = std::make_unique<TEvVolume::TEvDescribeBlocksIndexResponse>();

    for (const auto& mark : args.BlockMarks) {
        auto* entry = response->Record.AddEntries();
        entry->SetCommitId(mark.CommitId);
        entry->SetBlobOffset(mark.BlobOffset);

        if (mark.BlobId) {
            LogoBlobIDFromLogoBlobID(
                MakeBlobId(TabletID(), mark.BlobId),
                entry->MutableBlobId());
        }
    }

    RemoveTransaction(*args.RequestInfo);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "DescribeBlocksIndex",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
