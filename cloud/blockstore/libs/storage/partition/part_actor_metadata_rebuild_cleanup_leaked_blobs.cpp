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

constexpr ui64 MaxUniqueId = std::numeric_limits<ui64>::max();

////////////////////////////////////////////////////////////////////////////////

TString StringifyMetadataTx(const TTxPartition::TMetadataRebuildCleanupLeakedBlobs& args)
{
    return TStringBuilder()
        << " StartBlobId: ["
        <<  args.StartBlobId.CommitId() << ':' << args.StartBlobId.UniqueId() << ']'
        << " BlobCountToRead: " << args.BlobCountToRead
        << " FinalBlobId: ["
        << args.FinalBlobId.CommitId() << ':' << args.FinalBlobId.UniqueId() << ']'
        << " ReadCount: " << args.ReadCount
        << " LastReadBlobId: ["
        << args.LastReadBlobId.CommitId() << ':' << args.LastReadBlobId.UniqueId() << ']';
}

////////////////////////////////////////////////////////////////////////////////

TBlockMask CropBlockMask(const TBlockMask& blockMask, ui32 maxBlocksInBlob)
{
    TBlockMask croppedBlockMask;
    for (ui16 i = 0; i < maxBlocksInBlob; ++i) {
        if (blockMask.Get(i)) {
            croppedBlockMask.Set(i);
        }
    }
    return croppedBlockMask;
}

////////////////////////////////////////////////////////////////////////////////

bool VerifyLeakedBlobAbsentFromMixedIndex(
    TPartitionDatabase& db,
    const TPartialBlobId& blobId,
    const NProto::TBlobMeta& blobMeta)
{
    Y_ABORT_UNLESS(blobMeta.HasMixedBlocks());
    Y_ABORT_UNLESS(!blobMeta.HasMergedBlocks());

    const auto& mixedBlocks = blobMeta.GetMixedBlocks();
    if (mixedBlocks.BlocksSize() == 0) {
        return true;
    }

    struct TVisitor final: public IMixedBlocksIndexVisitor
    {
        const TPartialBlobId BlobId;

        explicit TVisitor(const TPartialBlobId& blobId)
            : BlobId(blobId)
        {}

        bool VisitBlock(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset,
            ui8 compactionRangeCount) override
        {
            Y_UNUSED(blockIndex);
            Y_UNUSED(commitId);
            Y_UNUSED(blobOffset);
            Y_UNUSED(compactionRangeCount);

            Y_ABORT_UNLESS(
                blobId != BlobId,
                "leaked blob is still present in mixed blocks index");
            return true;
        }
    };

    TVector<ui32> blocks;
    blocks.reserve(mixedBlocks.BlocksSize());
    for (ui32 blockIndex: mixedBlocks.GetBlocks()) {
        blocks.push_back(blockIndex);
    }

    TVisitor visitor(blobId);
    return db.FindMixedBlocks(visitor, blocks);
}

////////////////////////////////////////////////////////////////////////////////

class TMetadataRebuildCleanupLeakedBlobsVisitor final: public IBlobsIndexVisitor
{
private:
    const TCleanupQueue& CleanupQueue;
    const size_t MaxBlocksInBlob;
    TTxPartition::TMetadataRebuildCleanupLeakedBlobs& Args;

    ui64 BlobCount = 0;

public:
    TMetadataRebuildCleanupLeakedBlobsVisitor(
        const TCleanupQueue& cleanupQueue,
        size_t maxBlocksInBlob,
        TTxPartition::TMetadataRebuildCleanupLeakedBlobs& args)
        : CleanupQueue(cleanupQueue)
        , MaxBlocksInBlob(maxBlocksInBlob)
        , Args(args)
    {}

    bool Visit(
        ui64 commitId,
        ui64 blobId,
        const NProto::TBlobMeta& blobMeta,
        const TStringBuf blockMask) override
    {
        auto partialBlobId = MakePartialBlobId(commitId, blobId);
        Args.LastReadBlobId = partialBlobId;

        OnBlob(partialBlobId, blobMeta, blockMask);
        ++BlobCount;

        return true;
    }

    void UpdateTx()
    {
        if (Args.LastReadBlobId >= Args.FinalBlobId || !BlobCount) {
            Args.LastReadBlobId = Args.FinalBlobId;
        }
        Args.ReadCount = BlobCount;
    }

private:
    void OnBlob(
        const TPartialBlobId& blobId,
        const NProto::TBlobMeta& blobMeta,
        const TStringBuf blockMask)
    {
        auto originalBlockMask = BlockMaskFromString(blockMask);
        auto croppedBlockMask =
            CropBlockMask(originalBlockMask, MaxBlocksInBlob);

        if (IsBlockMaskFull(croppedBlockMask, MaxBlocksInBlob) &&
            !CleanupQueue.HasBlob(blobId))
        {
            Y_ABORT_UNLESS(
                !IsBlockMaskFull(originalBlockMask, MaxBlocksInBlob));
            Y_ABORT_UNLESS(
                blobMeta.HasMixedBlocks() && !blobMeta.HasMergedBlocks());
            Args.LeakedBlobIds.push_back(blobId);
            Args.BlobMetas.push_back(blobMeta);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMetadataRebuildCleanupLeakedBlobsActor final
    : public TActorBootstrapped<TMetadataRebuildCleanupLeakedBlobsActor>
{
private:
    const TActorId Tablet;
    const ui32 BlobsPerBatch;
    const TPartialBlobId FinalBlobId;
    const TDuration RetryTimeout;

    TPartialBlobId BlobIdToRead;

public:
    TMetadataRebuildCleanupLeakedBlobsActor(
        const TActorId& tablet,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        const TDuration retryTimeout);

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

    void HandleMetadataRebuildResponse(
        const TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TMetadataRebuildCleanupLeakedBlobsActor::TMetadataRebuildCleanupLeakedBlobsActor(
        const TActorId& tablet,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        const TDuration retryTimeout)
    : Tablet(tablet)
    , BlobsPerBatch(blobsPerBatch)
    , FinalBlobId(MakePartialBlobId(finalCommitId, Max()))
    , RetryTimeout(retryTimeout)
{}

void TMetadataRebuildCleanupLeakedBlobsActor::Bootstrap(const TActorContext& ctx)
{
    SendMetadataRebuildRequest(ctx);
    Become(&TThis::StateWork);
}

void TMetadataRebuildCleanupLeakedBlobsActor::SendMetadataRebuildRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<
        TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsRequest>(
        MakeIntrusive<TCallContext>(),
        BlobIdToRead,
        BlobsPerBatch,
        FinalBlobId);

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TMetadataRebuildCleanupLeakedBlobsActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<
        TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsCompleted>(
        error);

    NCloud::Send(ctx, Tablet, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TMetadataRebuildCleanupLeakedBlobsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsResponse, HandleMetadataRebuildResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TMetadataRebuildCleanupLeakedBlobsActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendMetadataRebuildRequest(ctx);
}

void TMetadataRebuildCleanupLeakedBlobsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    NotifyCompleted(ctx, error);
}

void TMetadataRebuildCleanupLeakedBlobsActor::HandleMetadataRebuildResponse(
    const TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        if (GetErrorKind(msg->Error) == EErrorKind::ErrorRetriable) {
            ctx.Schedule(RetryTimeout, new TEvents::TEvWakeup());
        } else {
            NotifyCompleted(ctx, msg->Error);
        }
        return;
    }

    if (msg->LastReadBlobId == FinalBlobId) {
        NotifyCompleted(ctx);
    } else {
        BlobIdToRead = NextBlobId(msg->LastReadBlobId, MaxUniqueId);

        SendMetadataRebuildRequest(ctx);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleMetadataRebuildCleanupLeakedBlobs(
    const TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsRequest::
        TPtr& ev,
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
            std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsResponse>(
                MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "MetadataRebuild",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetCommitQueue()->GetMinCommitId() <= msg->BlobId.CommitId()) {
        replyError(ctx, *requestInfo, E_REJECTED, "There are pending write commits");
        return;
    }

    if (State->GetCleanupState().Status == EOperationStatus::Started) {
        replyError(ctx, *requestInfo, E_REJECTED, "Cleanup is running");
        return;
    }

    auto [gen, step] = ParseCommitId(msg->BlobId.CommitId());

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start metadata rebuild for cleanup leaked blobs (starting %u:%u)",
        LogTitle.GetWithTime().c_str(),
        gen,
        step);

    AddTransaction<TEvPartitionPrivate::TMetadataRebuildCleanupLeakedBlobsMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TMetadataRebuildCleanupLeakedBlobs>(
            requestInfo,
            msg->BlobId,
            msg->Count,
            msg->FinalBlobId));
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareMetadataRebuildCleanupLeakedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMetadataRebuildCleanupLeakedBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    TMetadataRebuildCleanupLeakedBlobsVisitor visitor(
        State->GetCleanupQueue(),
        State->GetMaxBlocksInBlob(),
        args);

    auto progress = db.FindBlocksInBlobsIndex(
        visitor,
        args.StartBlobId,
        args.FinalBlobId,
        args.BlobCountToRead);

    auto ready = progress != TPartitionDatabase::EBlobIndexScanProgress::NotReady;

    Y_ABORT_UNLESS(args.LeakedBlobIds.size() == args.BlobMetas.size());
    for (size_t i = 0; i < args.LeakedBlobIds.size(); ++i) {
        ready &= VerifyLeakedBlobAbsentFromMixedIndex(
            db,
            args.LeakedBlobIds[i],
            args.BlobMetas[i]);
    }

    if (ready) {
        visitor.UpdateTx();
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s PrepareMetadataRebuildCleanupLeakedBlobs completed (%u) %s",
        LogTitle.GetWithTime().c_str(),
        static_cast<ui32>(progress),
        StringifyMetadataTx(args).c_str());

    return ready;
}

void TPartitionActor::ExecuteMetadataRebuildCleanupLeakedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMetadataRebuildCleanupLeakedBlobs& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    State->UpdateRebuildMetadataProgress(args.ReadCount);

    for (const auto& blobId: args.LeakedBlobIds) {
        State->GetCleanupQueue().Add({blobId, 0});
        db.WriteCleanupQueue(blobId, 0);
    }
}

void TPartitionActor::CompleteMetadataRebuildCleanupLeakedBlobs(
    const TActorContext& ctx,
    TTxPartition::TMetadataRebuildCleanupLeakedBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<
            TEvPartitionPrivate::TEvMetadataRebuildCleanupLeakedBlobsResponse>(
            args.LastReadBlobId));
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr TPartitionActor::CreateMetadataRebuildCleanupLeakedBlobsActor(
    TActorId tablet,
    ui64 blobsPerBatch,
    ui64 finalCommitId,
    TDuration retryTimeout)
{
    return std::make_unique<TMetadataRebuildCleanupLeakedBlobsActor>(
        tablet,
        blobsPerBatch,
        finalCommitId,
        retryTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
