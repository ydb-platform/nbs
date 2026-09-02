#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/throttling/leaky_bucket.h>

#include <util/generic/hash_set.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMetadataRebuildCompactionMapVisitor final
    : public IBlocksIndexVisitor
    , public IMixedBlocksIndexVisitor
{
private:
    TTxPartition::TMetadataRebuildCompactionMap& Args;

public:
    explicit TMetadataRebuildCompactionMapVisitor(
        TTxPartition::TMetadataRebuildCompactionMap& args)
        : Args(args)
    {}

    bool Visit(
        ui32 blockIndex,
        ui64 commitId, const TPartialBlobId& blobId, ui16 blobOffset) override
    {
        Y_UNUSED(commitId);

        AddBlock(blockIndex, blobId, blobOffset);
        return true;
    }

    bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset, ui8 compactionRangeCount) override
    {
        Y_UNUSED(commitId);
        Y_UNUSED(compactionRangeCount);

        AddBlock(blockIndex, blobId, blobOffset);
        return true;
    }

private:
    void AddBlock(
        ui32 blockIndex, const TPartialBlobId& blobId, ui16 blobOffset)
    {
        Args.Blobs[blobId].Blocks.push_back({blockIndex, blobOffset});
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMetadataRebuildCompactionMapActor final
    : public TActorBootstrapped<TMetadataRebuildCompactionMapActor>
{
private:
    const TActorId Tablet;
    const ui32 RangesPerBatch;
    const ui32 TotalRangeCount;
    const TDuration AllowedCpuTimePerSecond;
    const TDuration RetryTimeout;

    TLeakyBucket Throttling;

    ui32 RangeIndex = 0;
    ui32 RangesInFlight = 0;

public:
    TMetadataRebuildCompactionMapActor(
        const TActorId& tablet,
        ui32 rangesPerBatch,
        ui32 totalRangeCount,
        TDuration allowedCpuTimePerSecond, TDuration retryTimeout)
        : Tablet(tablet)
        , RangesPerBatch(rangesPerBatch)
        , TotalRangeCount(totalRangeCount)
        , AllowedCpuTimePerSecond(
              allowedCpuTimePerSecond ? allowedCpuTimePerSecond
                                      : TDuration::Seconds(1))
        , RetryTimeout(retryTimeout)
        , Throttling(
              AllowedCpuTimePerSecond.SecondsFloat(),
              AllowedCpuTimePerSecond.SecondsFloat(),
              AllowedCpuTimePerSecond.SecondsFloat())
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        if (TotalRangeCount) {
            Throttling.Register(ctx.Now(), 0);
            SendRequest(ctx);
            Become(&TThis::StateWork);
        } else {
            NotifyCompleted(ctx);
        }
    }

private:
    void SendRequest(const TActorContext& ctx)
    {
        RangesInFlight = Min(RangesPerBatch, TotalRangeCount - RangeIndex);

        auto request = std::make_unique<
            TEvPartitionPrivate::TEvMetadataRebuildCompactionMapRequest>(
            MakeIntrusive<TCallContext>(), RangeIndex, RangesInFlight);

        NCloud::Send(ctx, Tablet, std::move(request));
    }

    void NotifyCompleted(
        const TActorContext& ctx, const NProto::TError& error = {})
    {
        auto response =
            std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildCompleted>(
                error);

        NCloud::Send(ctx, Tablet, std::move(response));
        Die(ctx);
    }

    void ScheduleRetry(const TActorContext& ctx)
    {
        ctx.Schedule(RetryTimeout, new TEvents::TEvWakeup());
    }

    void ScheduleNextRequest(
        const TActorContext& ctx, TDuration cpuTimeSpentDuringLastTx)
    {
        const auto postponeTime = TDuration::MicroSeconds(
            Throttling.CalculatePostponeTime(
                ctx.Now(), cpuTimeSpentDuringLastTx.SecondsFloat()) *
            1e6);

        Throttling.Register(
            ctx.Now() + postponeTime, cpuTimeSpentDuringLastTx.SecondsFloat());

        if (postponeTime) {
            ctx.Schedule(postponeTime, new TEvents::TEvWakeup());
        } else {
            SendRequest(ctx);
        }
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(
                TEvPartitionPrivate::TEvMetadataRebuildCompactionMapResponse,
                HandleResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION_WORKER,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        SendRequest(ctx);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        NotifyCompleted(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
    }

    void HandleResponse(
        const TEvPartitionPrivate::TEvMetadataRebuildCompactionMapResponse::
            TPtr& ev, const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        if (HasError(msg->Error)) {
            if (GetErrorKind(msg->Error) == EErrorKind::ErrorRetriable) {
                ScheduleRetry(ctx);
            } else {
                NotifyCompleted(ctx, msg->Error);
            }
            return;
        }

        Y_ABORT_UNLESS(
            msg->RangeIndex == RangeIndex && msg->RangeCount == RangesInFlight);

        RangeIndex += RangesInFlight;
        if (RangeIndex == TotalRangeCount) {
            NotifyCompleted(ctx);
        } else {
            ScheduleNextRequest(ctx, msg->CpuTime);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleMetadataRebuildCompactionMap(
    const TEvPartitionPrivate::TEvMetadataRebuildCompactionMapRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "MetadataRebuildCompactionMap", requestInfo->CallContext->RequestId);

    auto replyError = [&](ui32 errorCode, TString errorReason)
    {
        auto response = std::make_unique<
            TEvPartitionPrivate::TEvMetadataRebuildCompactionMapResponse>(
            MakeError(errorCode, std::move(errorReason)));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
    };

    if (!State->IsMetadataRebuildStarted() ||
        State->GetMetadataRebuildType() != EMetadataRebuildType::CompactionMap)
    {
        replyError(E_REJECTED, "Compaction map rebuild is not running");
        return;
    }

    const ui64 rangeSize = State->GetCompactionMap().GetRangeSize();
    const ui64 totalRangeCount = CeilDiv(State->GetBlocksCount(), rangeSize);
    if (!msg->RangeCount || msg->RangeIndex >= totalRangeCount ||
        msg->RangeCount > totalRangeCount - msg->RangeIndex)
    {
        replyError(E_ARGUMENT, "Invalid compaction map range batch");
        return;
    }

    AddTransaction<TEvPartitionPrivate::TMetadataRebuildCompactionMapMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TMetadataRebuildCompactionMap>(
            requestInfo, msg->RangeIndex, msg->RangeCount));
}

bool TPartitionActor::PrepareMetadataRebuildCompactionMap(
    const TActorContext& ctx,
    TTransactionContext& tx, TTxPartition::TMetadataRebuildCompactionMap& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    const ui64 rangeSize = State->GetCompactionMap().GetRangeSize();
    const ui64 firstBlock = args.RangeIndex * rangeSize;
    const ui64 blockCount =
        Min(static_cast<ui64>(args.RangeCount) * rangeSize,
            State->GetBlocksCount() - firstBlock);
    args.BlockRange = TBlockRange32::WithLength(
        SafeIntegerCast<ui32>(firstBlock), SafeIntegerCast<ui32>(blockCount));

    TMetadataRebuildCompactionMapVisitor visitor(args);
    bool ready =
        db.FindMixedBlocks(visitor, args.BlockRange, true);   // precharge
    ready &= db.FindMergedBlocks(
        visitor,
        args.BlockRange,
        true,   // precharge
        State->GetMaxBlocksInBlob());

    for (auto& [blobId, blobInfo]: args.Blobs) {
        ready &= db.ReadBlobInfo(blobId, blobInfo.BlockMask, blobInfo.BlobMeta);
    }

    return ready;
}

void TPartitionActor::ExecuteMetadataRebuildCompactionMap(
    const TActorContext& ctx,
    TTransactionContext& tx, TTxPartition::TMetadataRebuildCompactionMap& args)
{
    Y_UNUSED(ctx);

    const ui32 rangeSize = State->GetCompactionMap().GetRangeSize();

    args.Counters.resize(args.RangeCount);
    for (ui32 i = 0; i < args.RangeCount; ++i) {
        args.Counters[i].BlockIndex = (args.RangeIndex + i) * rangeSize;
    }

    for (const auto& [blobId, blobInfo]: args.Blobs) {
        STORAGE_VERIFY_C(
            blobInfo.BlockMask && blobInfo.BlobMeta,
            TWellKnownEntityTypes::TABLET,
            TabletID(),
            TStringBuilder() << "Blob info is missing for " << blobId);

        THashSet<ui32> blobRanges;
        for (const auto& block: blobInfo.Blocks) {
            if (blobInfo.BlockMask->Get(block.BlobOffset)) {
                continue;
            }

            const ui32 rangeIndex = block.BlockIndex / rangeSize;
            STORAGE_VERIFY_C(
                rangeIndex >= args.RangeIndex &&
                    rangeIndex < args.RangeIndex + args.RangeCount,
                TWellKnownEntityTypes::TABLET,
                TabletID(),
                "Index block is outside the requested rebuild ranges");

            auto& counters =
                args.Counters[rangeIndex - args.RangeIndex].Counters;
            if (blobRanges.insert(rangeIndex).second) {
                ++counters.BlobCount;
            }

            if (!IsDeletionMarker(blobId)) {
                ++counters.BlockCount;
                counters.MixedBlockCount += blobInfo.BlobMeta->HasMixedBlocks();
            }
        }
    }

    TPartitionDatabase db(tx.DB);
    auto& compactionMap = State->GetCompactionMap();

    auto saturate = [](ui64 value)
    {
        return static_cast<ui32>(Min<ui64>(Max<ui16>(), value));
    };

    for (auto& item: args.Counters) {
        item.Counters.BlobCount = saturate(item.Counters.BlobCount);
        item.Counters.BlockCount = saturate(item.Counters.BlockCount);
        item.Counters.MixedBlockCount = saturate(item.Counters.MixedBlockCount);

        const ui32 blobCount = item.Counters.BlobCount;
        const ui32 blockCount = item.Counters.BlockCount;
        const ui32 mixedBlockCount = item.Counters.MixedBlockCount;

        if (blobCount || blockCount || mixedBlockCount) {
            db.WriteCompactionMap(
                item.BlockIndex, blobCount, blockCount, mixedBlockCount);
        } else {
            db.DeleteCompactionMap(item.BlockIndex);
        }

        const auto current = compactionMap.Get(item.BlockIndex);
        compactionMap.Update(
            item.BlockIndex,
            blobCount,
            blockCount,
            current.UsedBlockCount,
            current.NewlyZeroedBlocks,
            mixedBlockCount,
            blobCount < 2);   // compacted
    }

    State->UpdateRebuildMetadataProgress(args.RangeCount);
}

void TPartitionActor::CompleteMetadataRebuildCompactionMap(
    const TActorContext& ctx, TTxPartition::TMetadataRebuildCompactionMap& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);
    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<
            TEvPartitionPrivate::TEvMetadataRebuildCompactionMapResponse>(
            args.RangeIndex,
            args.RangeCount,
            CyclesToDurationSafe(args.RequestInfo->ExecCycles),
            std::move(args.Counters)));
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr TPartitionActor::CreateMetadataRebuildCompactionMapActor(
    TActorId tablet,
    ui32 rangesPerBatch,
    ui32 totalRangeCount,
    TDuration allowedCpuTimePerSecond, TDuration retryTimeout)
{
    return std::make_unique<TMetadataRebuildCompactionMapActor>(
        std::move(tablet),
        rangesPerBatch, totalRangeCount, allowedCpuTimePerSecond, retryTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
