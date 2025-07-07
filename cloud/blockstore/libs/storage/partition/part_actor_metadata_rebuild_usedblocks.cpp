#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/generic/guid.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMetadataRebuildBlockVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
{
private:
    TTxPartition::TMetadataRebuildUsedBlocks& Args;

public:
    TMetadataRebuildBlockVisitor(TTxPartition::TMetadataRebuildUsedBlocks& args)
        : Args(args)
    {}

public:
    bool Visit(const TFreshBlock& block) override
    {
        OnBlock(block.Meta.BlockIndex, block.Meta.CommitId, !!block.Content);

        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(commitId);
        Y_UNUSED(blobOffset);

        OnBlock(blockIndex, commitId, !IsDeletionMarker(blobId));

        return true;
    }

private:
    void OnBlock(ui32 blockIndex, ui64 commitId, bool filled)
    {
        auto& blockInfo = Args.BlockInfos[blockIndex - Args.BlockRange.Start];
        if (commitId > blockInfo.MaxCommitId) {
            if (blockInfo.Filled != filled) {
                if (filled) {
                    ++Args.FilledBlockCount;
                } else {
                    --Args.FilledBlockCount;
                }
            }
            blockInfo.Filled = filled;
            blockInfo.MaxCommitId = commitId;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMetadataRebuildUsedBlocksActor final
    : public TActorBootstrapped<TMetadataRebuildUsedBlocksActor>
{
private:
    const TActorId Tablet;
    const ui32 BlocksPerBatch;
    const ui32 BlockCount;
    const TDuration RetryTimeout;

    size_t CurrentBlock = 0;

public:
    TMetadataRebuildUsedBlocksActor(
        const TActorId& tablet,
        ui32 blocksPerBatch,
        ui32 blockCount,
        TDuration retryTimeout);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendMetadataRebuildRequest(const TActorContext& ctx);

    void NotifyCompleted(
        const TActorContext& ctx,
        const NProto::TError& error = {});

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleMetadataRebuildUsedBlocksResponse(
        const TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TMetadataRebuildUsedBlocksActor::TMetadataRebuildUsedBlocksActor(
        const TActorId& tablet,
        ui32 blocksPerBatch,
        ui32 blockCount,
        TDuration retryTimeout)
    : Tablet(tablet)
    , BlocksPerBatch(blocksPerBatch)
    , BlockCount(blockCount)
    , RetryTimeout(retryTimeout)
{}

void TMetadataRebuildUsedBlocksActor::Bootstrap(const TActorContext& ctx)
{
    SendMetadataRebuildRequest(ctx);
    Become(&TThis::StateWork);
}

void TMetadataRebuildUsedBlocksActor::SendMetadataRebuildRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksRequest>(
        MakeIntrusive<TCallContext>(),
        CurrentBlock,
        CurrentBlock + BlocksPerBatch);

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TMetadataRebuildUsedBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildCompleted>(error);

    NCloud::Send(ctx, Tablet, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TMetadataRebuildUsedBlocksActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksResponse, HandleMetadataRebuildUsedBlocksResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TMetadataRebuildUsedBlocksActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendMetadataRebuildRequest(ctx);
}

void TMetadataRebuildUsedBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    NotifyCompleted(ctx, error);
}

void TMetadataRebuildUsedBlocksActor::HandleMetadataRebuildUsedBlocksResponse(
    const TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui32 errorCode = msg->GetStatus();
    if (FAILED(errorCode)) {
        NotifyCompleted(ctx, msg->GetError());
        return;
    }

    if (errorCode == S_ALREADY) {
        ctx.Schedule(RetryTimeout, new TEvents::TEvWakeup());
        return;
    }

    CurrentBlock += BlocksPerBatch;
    if (CurrentBlock < BlockCount) {
        SendMetadataRebuildRequest(ctx);
    } else {
        NotifyCompleted(ctx);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleMetadataRebuildUsedBlocks(
    const TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksRequest::TPtr& ev,
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
        "MetadataRebuild",
        requestInfo->CallContext->RequestId);

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response =
            std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksResponse>(
                MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "MetadataRebuild",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (msg->Begin % State->GetUsedBlocks().CHUNK_SIZE != 0
            || msg->End % State->GetUsedBlocks().CHUNK_SIZE != 0)
    {
        replyError(ctx, *requestInfo, E_ARGUMENT, "bad range");
        return;
    }

    auto blockRange = TBlockRange32::MakeClosedIntervalWithLimit(
        msg->Begin,
        State->GetBlocksCount() - 1,
        static_cast<ui64>(msg->End) - 1);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start metadata rebuild for used blocks(range: %s)",
        LogTitle.GetWithTime().c_str(),
        DescribeRange(blockRange).c_str());

    AddTransaction<TEvPartitionPrivate::TMetadataRebuildUsedBlocksMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TMetadataRebuildUsedBlocks>(requestInfo, blockRange));
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareMetadataRebuildUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMetadataRebuildUsedBlocks& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    TMetadataRebuildBlockVisitor visitor(args);
    State->FindFreshBlocks(visitor, args.BlockRange);
    auto ready = db.FindMixedBlocks(
        visitor,
        args.BlockRange,
        true    // precharge
    );
    ready &= db.FindMergedBlocks(
        visitor,
        args.BlockRange,
        true,   // precharge
        State->GetMaxBlocksInBlob()
    );

    return ready;
}

void TPartitionActor::ExecuteMetadataRebuildUsedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMetadataRebuildUsedBlocks& args)
{
    Y_UNUSED(ctx);

    auto& usedBlocks = State->GetUsedBlocks();

    if (args.FilledBlockCount
            || usedBlocks.Count(args.BlockRange.Start, args.BlockRange.End + 1))
    {
        for (ui32 i = 0; i < args.BlockInfos.size(); ++i) {
            const auto& blockInfo = args.BlockInfos[i];
            const auto blockIndex = args.BlockRange.Start + i;
            if (blockInfo.Filled) {
                usedBlocks.Set(blockIndex, blockIndex + 1);
            } else {
                usedBlocks.Unset(blockIndex, blockIndex + 1);
            }
        }

        TPartitionDatabase db(tx.DB);

        auto serializer = usedBlocks.RangeSerializer(
            args.BlockRange.Start, args.BlockRange.End + 1);
        TCompressedBitmap::TSerializedChunk sc;
        while (serializer.Next(&sc)) {
            db.WriteUsedBlocks(sc);
        }
    }

    State->UpdateRebuildMetadataProgress(args.BlockRange.Size());
}

void TPartitionActor::CompleteMetadataRebuildUsedBlocks(
    const TActorContext& ctx,
    TTxPartition::TMetadataRebuildUsedBlocks& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksResponse>());
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr TPartitionActor::CreateMetadataRebuildUsedBlocksActor(
    TActorId tablet,
    ui64 blocksPerBatch,
    ui64 blockCount,
    TDuration retryTimeout)
{
    return std::make_unique<TMetadataRebuildUsedBlocksActor>(
        std::move(tablet),
        blocksPerBatch,
        blockCount,
        retryTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
