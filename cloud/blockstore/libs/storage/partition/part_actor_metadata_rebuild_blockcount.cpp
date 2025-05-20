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

TString StringifyMetadataTx(const TTxPartition::TMetadataRebuildBlockCount& args)
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

class TMetadataRebuildBlockCountVisitor final
    : public IBlobsIndexVisitor
{
private:
    TTxPartition::TMetadataRebuildBlockCount& Args;

    ui32 BlobCount = 0;

public:
    TMetadataRebuildBlockCountVisitor(TTxPartition::TMetadataRebuildBlockCount& args)
        : Args(args)
    {}

public:
    bool Visit(
        ui64 commitId,
        ui64 blobId,
        const NProto::TBlobMeta& blobMeta,
        const TStringBuf blockMask) override
    {
        Args.LastReadBlobId = MakePartialBlobId(commitId, blobId);

        OnBlob(commitId, blobId, blobMeta, blockMask);
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
        ui64 commitId,
        ui64 blobId,
        const NProto::TBlobMeta& blobMeta,
        const TStringBuf blockMask)
    {
        Y_UNUSED(blockMask);

        if (IsDeletionMarker(MakePartialBlobId(commitId, blobId))) {
            return;
        }

        if (blobMeta.HasMixedBlocks()) {
            Args.MixedBlockCount += blobMeta.GetMixedBlocks().BlocksSize();
        } else {
            auto delta =
                blobMeta.GetMergedBlocks().GetEnd() - blobMeta.GetMergedBlocks().GetStart() + 1;
            delta -= blobMeta.GetMergedBlocks().GetSkipped();
            Args.MergedBlockCount += delta;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMetadataRebuildBlockCountActor final
    : public TActorBootstrapped<TMetadataRebuildBlockCountActor>
{
private:
    const TActorId Tablet;
    const ui32 BlobsPerBatch;
    const TPartialBlobId FinalBlobId;
    const TDuration RetryTimeout;

    TPartialBlobId BlobIdToRead;

    TBlockCountRebuildState RebuildState;

public:
    TMetadataRebuildBlockCountActor(
        const TActorId& tablet,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        ui64 mixedBlocksCount,
        ui64 mergedBlocksCount,
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
        const TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TMetadataRebuildBlockCountActor::TMetadataRebuildBlockCountActor(
        const TActorId& tablet,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        ui64 mixedBlocksCount,
        ui64 mergedBlocksCount,
        const TDuration retryTimeout)
    : Tablet(tablet)
    , BlobsPerBatch(blobsPerBatch)
    , FinalBlobId(MakePartialBlobId(finalCommitId, Max()))
    , RetryTimeout(retryTimeout)
    , RebuildState{0, 0, mixedBlocksCount, mergedBlocksCount}
{}

void TMetadataRebuildBlockCountActor::Bootstrap(const TActorContext& ctx)
{
    SendMetadataRebuildRequest(ctx);
    Become(&TThis::StateWork);
}

void TMetadataRebuildBlockCountActor::SendMetadataRebuildRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildBlockCountRequest>(
        MakeIntrusive<TCallContext>(),
        BlobIdToRead,
        BlobsPerBatch,
        FinalBlobId,
        RebuildState);

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TMetadataRebuildBlockCountActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildCompleted>(error);

    NCloud::Send(ctx, Tablet, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TMetadataRebuildBlockCountActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse, HandleMetadataRebuildResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TMetadataRebuildBlockCountActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendMetadataRebuildRequest(ctx);
}

void TMetadataRebuildBlockCountActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    NotifyCompleted(ctx, error);
}

void TMetadataRebuildBlockCountActor::HandleMetadataRebuildResponse(
    const TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse::TPtr& ev,
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

        RebuildState = msg->RebuildState;

        SendMetadataRebuildRequest(ctx);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleMetadataRebuildBlockCount(
    const TEvPartitionPrivate::TEvMetadataRebuildBlockCountRequest::TPtr& ev,
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
            std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse>(
                MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "MetadataRebuild",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetCommitQueue().GetMinCommitId() <= msg->BlobId.CommitId()) {
        replyError(ctx, *requestInfo, E_REJECTED, "There are pending write commits");
        return;
    }

    if (State->GetCleanupState().Status == EOperationStatus::Started) {
        replyError(ctx, *requestInfo, E_REJECTED, "Cleanup is running");
        return;
    }

    auto [gen, step] = ParseCommitId(msg->BlobId.CommitId());

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Start metadata rebuild for block count (starting %u:%u)",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        gen,
        step);

    AddTransaction<TEvPartitionPrivate::TMetadataRebuildBlockCountMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TMetadataRebuildBlockCount>(
            requestInfo,
            msg->BlobId,
            msg->Count,
            msg->FinalBlobId,
            msg->RebuildState),
        &TransactionTimeTracker);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareMetadataRebuildBlockCount(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMetadataRebuildBlockCount& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    TMetadataRebuildBlockCountVisitor visitor(args);

    auto progress = db.FindBlocksInBlobsIndex(
        visitor,
        args.StartBlobId,
        args.FinalBlobId,
        args.BlobCountToRead);
    auto ready = progress != TPartitionDatabase::EBlobIndexScanProgress::NotReady;
    if (ready) {
        visitor.UpdateTx();
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] PrepareMetadataRebuildBlockCount completed (%u) %s",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        static_cast<ui32>(progress),
        StringifyMetadataTx(args).c_str());

    return ready;
}

void TPartitionActor::ExecuteMetadataRebuildBlockCount(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMetadataRebuildBlockCount& args)
{
    Y_UNUSED(ctx);

    State->UpdateRebuildMetadataProgress(args.ReadCount);
    args.RebuildState.MixedBlocks += args.MixedBlockCount;
    args.RebuildState.MergedBlocks += args.MergedBlockCount;

    if (args.LastReadBlobId == args.FinalBlobId) {
        TPartitionDatabase db(tx.DB);

        auto mixed = State->GetMixedBlocksCount() -
            args.RebuildState.InitialMixedBlocks +
            args.RebuildState.MixedBlocks;

        auto merged = State->GetMergedBlocksCount() -
            args.RebuildState.InitialMergedBlocks +
            args.RebuildState.MergedBlocks;

        State->UpdateBlocksCountersAfterMetadataRebuild(mixed, merged);

        db.WriteMeta(State->GetMeta());
    }
}

void TPartitionActor::CompleteMetadataRebuildBlockCount(
    const TActorContext& ctx,
    TTxPartition::TMetadataRebuildBlockCount& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse>(
            args.LastReadBlobId,
            args.RebuildState));
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr TPartitionActor::CreateMetadataRebuildBlockCountActor(
    TActorId tablet,
    ui64 blobsPerBatch,
    ui64 finalCommitId,
    ui64 mixedBlocksCount,
    ui64 mergedBlocksCount,
    TDuration retryTimeout)
{
    return std::make_unique<TMetadataRebuildBlockCountActor>(
        std::move(tablet),
        blobsPerBatch,
        finalCommitId,
        mixedBlocksCount,
        mergedBlocksCount,
        retryTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
