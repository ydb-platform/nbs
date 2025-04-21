#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition2/model/rebase_logic.h>

#include <cloud/storage/core/libs/common/alloc.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t Percentage(size_t fraction, size_t total)
{
    return total ? (fraction * 100) / total : 0;
}

////////////////////////////////////////////////////////////////////////////////

class TCompactionActor final
    : public TActorBootstrapped<TCompactionActor>
{
public:
    struct TReadRequest
        : TBlobRefs
    {
        TGuardedBuffer<TBlockBuffer> BlobContent;
        size_t DataBlockCount = 0;

        using TBlobRefs::TBlobRefs;
    };

    struct TBlockRef
    {
        const TReadRequest* Request;
        ui32 Index = 0;
        ui16 BlobOffset = 0;
    };

    struct TWriteRequest
    {
        TPartialBlobId BlobId;
        TVector<TBlockRef> Blocks;
        size_t DataBlockCount = 0;
    };

private:
    const TRequestInfoPtr RequestInfo;

    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ui32 BlockSize;
    const ui64 TabletId;
    const TActorId Tablet;
    const ui64 CommitId;
    const TBlockRange32 BlockRange;
    const TDuration ReadBlobTimeout;
    const ECompactionType CompactionType;
    TGarbageInfo GarbageInfo;
    TAffectedBlobInfos AffectedBlobInfos;
    ui32 BlobsSkipped;
    ui32 BlocksSkipped;
    TVector<TReadRequest> ReadRequests;
    TVector<TWriteRequest> WriteRequests;
    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
    TVector<TBlockRange64> AffectedRanges;
    TVector<IProfileLog::TBlockCommitId> BlockCommitIds;

    size_t DataBlockCount = 0;
    size_t ReadRequestsScheduled = 0;
    size_t ReadRequestsCompleted = 0;
    size_t WriteRequestsScheduled = 0;
    size_t WriteRequestsCompleted = 0;

    ui64 ReadExecCycles = 0;
    ui64 ReadWaitCycles = 0;

    TVector<TCallContextPtr> ForkedReadContexts;
    TVector<TCallContextPtr> ForkedWriteContexts;
    bool SafeToUseOrbit = true;

    ui64 MaxExecCyclesFromRead = 0;
    ui64 MaxExecCyclesFromWrite = 0;

public:
    TCompactionActor(
        TRequestInfoPtr requestInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui32 blockSize,
        ui64 tabletId,
        const TActorId& tablet,
        ui64 commitId,
        const TBlockRange32& blockRange,
        TDuration readBlobTimeout,
        ECompactionType compactionType,
        TGarbageInfo garbageInfo,
        TAffectedBlobInfos affectedBlobInfos,
        ui32 blobsSkipped,
        ui32 blocksSkipped,
        TVector<TReadRequest> readRequests,
        TVector<TWriteRequest> writeRequests);

    void Bootstrap(const TActorContext& ctx);

private:
    void InitBlockDigestsAndRanges();
    TGuardedSgList BuildBlobContent(const TWriteRequest& request) const;
    TVector<TBlock> BuildBlockList(const TWriteRequest& request) const;

    void ReadBlocks(const TActorContext& ctx);
    void WriteBlobs(const TActorContext& ctx);
    void AddBlobs(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvPartitionPrivate::TEvCompactionResponse> response);

private:
    STFUNC(StateWork);

    void HandleReadBlobResponse(
        const TEvPartitionPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteBlobResponse(
        const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCompactionActor::TCompactionActor(
        TRequestInfoPtr requestInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui32 blockSize,
        ui64 tabletId,
        const TActorId& tablet,
        ui64 commitId,
        const TBlockRange32& blockRange,
        TDuration readBlobTimeout,
        ECompactionType compactionType,
        TGarbageInfo garbageInfo,
        TAffectedBlobInfos affectedBlobInfos,
        ui32 blobsSkipped,
        ui32 blocksSkipped,
        TVector<TReadRequest> readRequests,
        TVector<TWriteRequest> writeRequests)
    : RequestInfo(std::move(requestInfo))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , BlockSize(blockSize)
    , TabletId(tabletId)
    , Tablet(tablet)
    , CommitId(commitId)
    , BlockRange(blockRange)
    , ReadBlobTimeout(readBlobTimeout)
    , CompactionType(compactionType)
    , GarbageInfo(std::move(garbageInfo))
    , AffectedBlobInfos(std::move(affectedBlobInfos))
    , BlobsSkipped(blobsSkipped)
    , BlocksSkipped(blocksSkipped)
    , ReadRequests(std::move(readRequests))
    , WriteRequests(std::move(writeRequests))
{}

void TCompactionActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "Compaction",
        RequestInfo->CallContext->RequestId);

    ReadBlocks(ctx);
}

void TCompactionActor::InitBlockDigestsAndRanges()
{
    TBlockRange64Builder rangeBuilder(AffectedRanges);
    for (const auto& req: WriteRequests) {
        for (const auto& ref: req.Blocks) {
            const auto blockIndex = ref.Request->Blocks[ref.Index].BlockIndex;
            rangeBuilder.OnBlock(blockIndex);

            auto content = TBlockDataRef::CreateZeroBlock(BlockSize);
            if (ref.BlobOffset != ZeroBlobOffset) {
                content = ref.Request->BlobContent.Get().GetBlock(ref.BlobOffset);
            }

            const auto digest = BlockDigestGenerator->ComputeDigest(
                blockIndex,
                content
            );

            if (digest.Defined()) {
                AffectedBlockInfos.push_back({blockIndex, *digest});
            }
        }
    }

    for (const auto& req: ReadRequests) {
        for (const auto& block: req.Blocks) {
            rangeBuilder.OnBlock(block.BlockIndex);
        }
    }

    if (!GarbageInfo.BlobCounters) {
        AffectedRanges.push_back(ConvertRangeSafe(BlockRange));
    }
}

TGuardedSgList TCompactionActor::BuildBlobContent(const TWriteRequest& req) const
{
    TSgList joinedSglist(Reserve(req.Blocks.size()));
    THashSet<const TReadRequest*> requests;

    for (const auto& ref: req.Blocks) {
        if (ref.BlobOffset != ZeroBlobOffset) {
            auto& blobContent = ref.Request->BlobContent;
            joinedSglist.push_back(blobContent.Get().GetBlock(ref.BlobOffset));
            requests.insert(ref.Request);
        }
    }

    TVector<TGuardedSgList> guardedObjects(Reserve(requests.size()));
    for (auto* request: requests) {
        guardedObjects.push_back(request->BlobContent.CreateGuardedSgList({}));
    }

    auto result = TGuardedSgList::CreateUnion(std::move(guardedObjects));
    result.SetSgList(std::move(joinedSglist));
    return result;
}

TVector<TBlock> TCompactionActor::BuildBlockList(const TWriteRequest& req) const
{
    TVector<TBlock> result(Reserve(req.Blocks.size()));
    for (const auto& ref: req.Blocks) {
        result.push_back(ref.Request->Blocks[ref.Index]);
    }
    return result;
}

void TCompactionActor::ReadBlocks(const TActorContext& ctx)
{
    bool readBlobSent = false;

    const auto readBlobDeadline = ReadBlobTimeout ?
        ctx.Now() + ReadBlobTimeout :
        TInstant::Max();

    ui32 requestIndex = 0;
    for (auto& req: ReadRequests) {
        if (req.DataBlobOffsets) {
            Y_ABORT_UNLESS(req.Proxy);

            DataBlockCount += req.DataBlockCount;
            ++ReadRequestsScheduled;
            readBlobSent = true;

            auto request = std::make_unique<TEvPartitionPrivate::TEvReadBlobRequest>(
                MakeBlobId(TabletId, req.BlobId),
                req.Proxy,
                std::move(req.DataBlobOffsets),
                req.BlobContent.GetGuardedSgList(),
                req.GroupId,
                true,           // async
                readBlobDeadline // deadline
            );

            if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionPrivate::TEvReadBlobRequest",
                    RequestInfo->CallContext->RequestId);
            }

            ForkedReadContexts.emplace_back(request->CallContext);

            NCloud::Send(
                ctx,
                Tablet,
                std::move(request),
                requestIndex);
        }
        ++requestIndex;
    }

    if (!readBlobSent) {
        WriteBlobs(ctx);
    }
}

void TCompactionActor::WriteBlobs(const TActorContext& ctx)
{
    InitBlockDigestsAndRanges();

    bool writeBlobSent = false;

    for (auto& req: WriteRequests) {
        if (!IsDeletionMarker(req.BlobId)) {
            auto request = std::make_unique<TEvPartitionPrivate::TEvWriteBlobRequest>(
                req.BlobId,
                BuildBlobContent(req));

            if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionPrivate::TEvWriteBlobRequest",
                    RequestInfo->CallContext->RequestId);
            }

            ForkedWriteContexts.emplace_back(request->CallContext);

            writeBlobSent = true;
            ++WriteRequestsScheduled;

            NCloud::Send(
                ctx,
                Tablet,
                std::move(request));
        }
    }

    if (!writeBlobSent) {
        AddBlobs(ctx);
    }
}

void TCompactionActor::AddBlobs(const TActorContext& ctx)
{
    TVector<TAddBlob> blobs(Reserve(WriteRequests.size()));
    for (const auto& req: WriteRequests) {
        blobs.emplace_back(req.BlobId, BuildBlockList(req));
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
        RequestInfo->CallContext,
        ADD_COMPACTION_RESULT,
        std::move(blobs),
        std::move(GarbageInfo),
        std::move(AffectedBlobInfos),
        BlobsSkipped,
        BlocksSkipped);

    SafeToUseOrbit = false;

    NCloud::Send(
        ctx,
        Tablet,
        std::move(request));
}

void TCompactionActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvCompactionCompleted>(error);
    request->CommitId = CommitId;
    request->ExecCycles = RequestInfo->GetExecCycles();
    request->TotalCycles = RequestInfo->GetTotalCycles();

    {
        auto execTime = CyclesToDurationSafe(ReadExecCycles);
        auto waitTime = CyclesToDurationSafe(ReadWaitCycles);

        auto& counters = *request->Stats.MutableSysReadCounters();
        counters.SetRequestsCount(1);
        counters.SetBlocksCount(DataBlockCount);
        counters.SetExecTime(execTime.MicroSeconds());
        counters.SetWaitTime(waitTime.MicroSeconds());
    }

    {
        auto execCycles = RequestInfo->GetExecCycles();
        auto totalCycles = RequestInfo->GetTotalCycles();
        TDuration execTime = CyclesToDurationSafe(execCycles - ReadExecCycles);
        TDuration waitTime;
        if (totalCycles > execCycles + ReadWaitCycles) {
            waitTime = CyclesToDurationSafe(totalCycles - execCycles - ReadWaitCycles);
        }

        auto& counters = *request->Stats.MutableSysWriteCounters();
        counters.SetRequestsCount(1);
        counters.SetBlocksCount(DataBlockCount);
        counters.SetExecTime(execTime.MicroSeconds());
        counters.SetWaitTime(waitTime.MicroSeconds());
    }

    request->AffectedRanges = std::move(AffectedRanges);
    request->AffectedBlockInfos = std::move(AffectedBlockInfos);
    request->BlockCommitIds = std::move(BlockCommitIds);
    request->CompactionType = CompactionType;

    NCloud::Send(ctx, Tablet, std::move(request));
}

bool TCompactionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>(
                error
            )
        );
        return true;
    }
    return false;
}

void TCompactionActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvPartitionPrivate::TEvCompactionResponse> response)
{
    NotifyCompleted(ctx, response->GetError());

    if (SafeToUseOrbit) {
        LWTRACK(
            ResponseSent_Partition,
            RequestInfo->CallContext->LWOrbit,
            "Compaction",
            RequestInfo->CallContext->RequestId);
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCompactionActor::HandleReadBlobResponse(
    const TEvPartitionPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto msg = ev->Release();

    MaxExecCyclesFromRead = Max(MaxExecCyclesFromRead, msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    Y_ABORT_UNLESS(ReadRequestsCompleted < ReadRequestsScheduled);
    if (++ReadRequestsCompleted < ReadRequestsScheduled) {
        return;
    }

    for (auto context: ForkedReadContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    RequestInfo->AddExecCycles(MaxExecCyclesFromRead);

    ReadExecCycles = RequestInfo->GetExecCycles();
    ReadWaitCycles = RequestInfo->GetWaitCycles();

    WriteBlobs(ctx);
}

void TCompactionActor::HandleWriteBlobResponse(
    const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    MaxExecCyclesFromWrite = Max(MaxExecCyclesFromWrite, msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    Y_ABORT_UNLESS(WriteRequestsCompleted < WriteRequestsScheduled);
    if (++WriteRequestsCompleted < WriteRequestsScheduled) {
        return;
    }

    RequestInfo->AddExecCycles(MaxExecCyclesFromWrite);

    for (auto context: ForkedWriteContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    AddBlobs(ctx);
}

void TCompactionActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    SafeToUseOrbit = true;
    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    BlockCommitIds = std::move(msg->BlockCommitIds);

    ReplyAndDie(
        ctx,
        std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>()
    );
}

void TCompactionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto response = std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>(
        MakeError(E_REJECTED, "tablet is shutting down"));

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TCompactionActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvPartitionPrivate::TEvReadBlobResponse, HandleReadBlobResponse);
        HFunc(TEvPartitionPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCompactionVisitor final
    : public IMergedBlockVisitor
{
private:
    TTxPartition::TCompaction& Args;
    TVector<TBlockAndLocation> Blocks;
    bool RegroupBlocks;

public:
    TCompactionVisitor(TTxPartition::TCompaction& args, bool regroupBlocks)
        : Args(args)
        , RegroupBlocks(regroupBlocks)
    {}

    void Visit(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_ABORT_UNLESS(blobOffset != InvalidBlobOffset);

        // filter out garbage blocks
        if (block.MinCommitId != block.MaxCommitId) {
            if (RegroupBlocks) {
                Blocks.push_back({block, {blobId, blobOffset}});
            } else {
                Args.Blobs.AddBlock(block, blobId, blobOffset);
            }
        }
    }

    void Finish()
    {
        if (RegroupBlocks) {
            Sort(
                Blocks.begin(),
                Blocks.end(),
                [] (const TBlockAndLocation& l, const TBlockAndLocation& r) {
                    if (l.Location.BlobId != r.Location.BlobId) {
                        return l.Location.BlobId < r.Location.BlobId;
                    }

                    return l.Block < r.Block;
                }
            );

            for (const auto& b: Blocks) {
                Args.Blobs.AddBlock(
                    b.Block,
                    b.Location.BlobId,
                    b.Location.BlobOffset
                );
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueCompactionIfNeeded(const TActorContext& ctx)
{
    if (State->GetCompactionStatus(ECompactionType::Tablet) !=
        EOperationStatus::Idle)
    {
        // compaction already enqueued
        return;
    }

    if (State->GetCleanupStatus() != EOperationStatus::Idle) {
        // cleanup already enqueued
        return;
    }

    if (!State->IsCompactionAllowed()) {
        return;
    }

    auto top = State->GetCompactionMap().GetTop();

    ui32 garbageScoreInt = Percentage(
        State->GetGarbageBlockCount(),
        State->GetMergedBlockCount());

    double cleanupScore = State->GetPendingUpdates()
        / double(Config->GetUpdateBlobsThreshold());
    double rangeScore = top.Stat.Compacted ? 0 : top.Stat.CompactionScore.Score;
    double garbageScore = garbageScoreInt
        / double(Config->GetCompactionGarbageThreshold());

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Enqueue compaction, scores: "
        "cleanupScore=%f"
        " rangeScore=%f"
        " garbageScore=%f",
        TabletID(),
        cleanupScore,
        rangeScore,
        garbageScore
    );

    if (cleanupScore >= 1) {
         EnqueueCleanup(
            ctx,
            TEvPartitionPrivate::ECleanupMode::DirtyBlobCleanup);

         return;
    }

    if (garbageScore < 1 && rangeScore <= 0) {
        Y_ABORT_UNLESS(cleanupScore < 1);

        if (State->HasCheckpointsToDelete()) {
            EnqueueCleanup(
                ctx,
                TEvPartitionPrivate::ECleanupMode::CheckpointBlobCleanup);
        }

        return;
    }

    TEvPartitionPrivate::ECompactionMode compactionMode;
    if (rangeScore > 0) {
        compactionMode = TEvPartitionPrivate::RangeCompaction;
    } else if (garbageScore >= 1) {
        compactionMode = TEvPartitionPrivate::GarbageCompaction;
    } else {
        Y_DEBUG_ABORT_UNLESS(false);
        return;
    }

    State->SetCompactionStatus(
        ECompactionType::Tablet,
        EOperationStatus::Enqueued);

    auto request = std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()),
        compactionMode);

    NCloud::Send(
        ctx,
        SelfId(),
        std::move(request));
}

void TPartitionActor::HandleCompaction(
    const TEvPartitionPrivate::TEvCompactionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        BackgroundTaskStarted_Partition,
        requestInfo->CallContext->LWOrbit,
        "Compaction",
        static_cast<ui32>(PartitionConfig.GetStorageMediaKind()),
        requestInfo->CallContext->RequestId,
        PartitionConfig.GetDiskId());

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "Compaction",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    const auto compactionType =
        msg->CompactionOptions.test(ToBit(ECompactionOption::Forced)) ?
        ECompactionType::Forced:
        ECompactionType::Tablet;

    if (State->GetCompactionStatus(compactionType) == EOperationStatus::Started) {
        replyError(ctx, *requestInfo, E_TRY_AGAIN, "compaction already started");
        return;
    }

    if (!State->IsCompactionAllowed()) {
        State->SetCompactionStatus(compactionType, EOperationStatus::Idle);

        replyError(ctx, *requestInfo, E_BS_OUT_OF_SPACE, "all channels readonly");
        return;
    }

    ui32 startIndex = 0;
    TRangeStat rangeStat;

    TGarbageInfo garbageInfo;

    const auto& cm = State->GetCompactionMap();

    if (msg->Mode == TEvPartitionPrivate::RangeCompaction) {
        if (msg->BlockIndex) {
            startIndex = cm.GetRangeStart(*msg->BlockIndex);
            rangeStat = cm.Get(startIndex);
            State->OnNewCompactionRange();
        } else {
            auto top = State->GetCompactionMap().GetTop();
            startIndex = top.BlockIndex;
            rangeStat = top.Stat;
        }
    } else {
        if (msg->GarbageInfo.BlobCounters) {
            garbageInfo = std::move(msg->GarbageInfo);
        } else {
            garbageInfo = State->GetBlobs().GetTopGarbage(
                Config->GetCompactionGarbageBlobLimit(),
                Config->GetCompactionGarbageBlockLimit()
            );
        }
    }

    if (!rangeStat.BlobCount && !garbageInfo.BlobCounters) {
        State->SetCompactionStatus(compactionType, EOperationStatus::Idle);

        replyError(ctx, *requestInfo, S_ALREADY, "nothing to compact");
        return;
    }

    std::unique_ptr<ITransactionBase> tx;
    if (msg->Mode == TEvPartitionPrivate::RangeCompaction) {
        auto endIndex = Min(
            State->GetBlockCount() - 1,
            static_cast<ui64>(startIndex) + cm.GetRangeSize() - 1
        );

        auto blockRange =
            TBlockRange32::MakeClosedInterval(startIndex, endIndex);

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Start compaction "
            "(range: %s, blobs: %u, blocks: %u"
            ", reads: %u, blobsread: %u, blocksread: %u, score: %f"
            ", forceFullCompaction: %d)",
            TabletID(),
            DescribeRange(blockRange).data(),
            rangeStat.BlobCount,
            rangeStat.BlockCount,
            rangeStat.ReadRequestCount,
            rangeStat.ReadRequestBlobCount,
            rangeStat.ReadRequestBlockCount,
            rangeStat.CompactionScore.Score,
            msg->CompactionOptions.test(ToBit(ECompactionOption::Forced))
        );

        tx = CreateTx<TCompaction>(
            requestInfo,
            blockRange,
            msg->CompactionOptions);
    } else {
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Start compaction (blobs: %s)",
            TabletID(),
            DumpBlobIds(TabletID(), SelectFirst(garbageInfo.BlobCounters)).data());

        tx = CreateTx<TCompaction>(requestInfo, std::move(garbageInfo));
    }


    State->SetCompactionStatus(compactionType, EOperationStatus::Started);

    AddTransaction<TEvPartitionPrivate::TCompactionMethod>(*requestInfo);

    auto& queue = State->GetCCCRequestQueue();
    // shouldn't wait for inflight fresh blocks to complete (commitID == 0)
    queue.push_back({ 0, std::move(tx) });

    ProcessCCCRequestQueue(ctx);
}

void TPartitionActor::HandleCompactionCompleted(
    const TEvPartitionPrivate::TEvCompactionCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    State->StopProcessingCCCRequest();

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete compaction @%lu",
        TabletID(),
        msg->CommitId);

    UpdateStats(msg->Stats);

    UpdateCPUUsageStats(ctx, CyclesToDurationSafe(msg->ExecCycles));

    const auto d = CyclesToDurationSafe(msg->TotalCycles);
    const auto ts = ctx.Now() - d;
    PartCounters->RequestCounters.Compaction.AddRequest(d.MicroSeconds());

    State->ReleaseCollectBarrier(msg->CommitId);

    State->SetCompactionStatus(msg->CompactionType, EOperationStatus::Idle);

    Actors.erase(ev->Sender);

    EnqueueCompactionIfNeeded(ctx);

    ProcessCCCRequestQueue(ctx);

    {
        IProfileLog::TSysReadWriteRequest request;
        request.RequestType = ESysRequestType::Compaction;
        request.Duration = d;
        request.Ranges = std::move(msg->AffectedRanges);

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ts;
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }

    LogBlockInfos(
        ctx,
        ESysRequestType::Compaction,
        std::move(msg->AffectedBlockInfos),
        msg->CommitId);

    LogBlockCommitIds(
        ctx,
        ESysRequestType::Compaction,
        std::move(msg->BlockCommitIds),
        msg->CommitId);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompaction& args)
{
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    if (args.GarbageInfo.BlobCounters) {
        TCompactionVisitor visitor(args, false);
        if (!State->FindMergedBlocks(db, args.GarbageInfo, visitor)) {
            return false;
        }

        ui32 firstBlock = Max<ui32>();
        ui32 lastBlock = Max<ui32>();
        for (const auto& blobRefs: args.Blobs) {
            if (firstBlock == Max<ui32>()) {
                firstBlock = blobRefs.Blocks.front().BlockIndex;
                lastBlock = blobRefs.Blocks.front().BlockIndex;
            } else {
                firstBlock = Min(firstBlock, blobRefs.Blocks.front().BlockIndex);
                lastBlock = Max(lastBlock, blobRefs.Blocks.back().BlockIndex);
            }
        }

        return State->InitIndex(
            db,
            TBlockRange32::MakeClosedInterval(firstBlock, lastBlock));
    } else {
        if (!State->InitIndex(db, args.BlockRange)) {
            return false;
        }

        // NBS-2451
        // TODO: make this update appear in the profile log for better debugging
        if (Config->GetEnableConversionIntoMixedIndexV2()) {
            if (!State->UpdateIndexStructures(db, ctx.Now(), args.BlockRange)) {
                return false;
            }
        }
    }

    TCompactionVisitor visitor(args, State->ContainsMixedZones(args.BlockRange));
    if (!State->FindMergedBlocks(db, args.BlockRange, visitor)) {
        return false;
    }

    visitor.Finish();

    if (!args.CompactionOptions.test(ToBit(ECompactionOption::Full))) {
        Sort(
            args.Blobs.begin(),
            args.Blobs.end(),
            [] (const TBlobRefs& l, const TBlobRefs& r) {
                return l.Blocks.size() < r.Blocks.size();
            }
        );

        auto it = args.Blobs.begin();
        args.BlobsSkipped = args.Blobs.size();
        ui32 blocks = 0;

        while (it != args.Blobs.end()) {
            const auto bytes = blocks * State->GetBlockSize();
            const auto blobCountOk =
                args.BlobsSkipped <= Config->GetMaxSkippedBlobsDuringCompaction();
            const auto byteCountOk =
                bytes >= Config->GetTargetCompactionBytesPerOp();

            if (blobCountOk && byteCountOk) {
                break;
            }

            blocks += it->Blocks.size();
            --args.BlobsSkipped;
            ++it;
        }

        while (it != args.Blobs.end()) {
            args.BlocksSkipped += it->Blocks.size();
            ++it;
        }

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Dropping last %u blobs, %u blocks"
            ", remaining blobs: %u, blocks: %u",
            TabletID(),
            args.BlobsSkipped,
            args.BlocksSkipped,
            args.Blobs.size() - args.BlobsSkipped,
            blocks
        );

        args.Blobs.resize(args.Blobs.size() - args.BlobsSkipped);
    }

    bool ready = true;
    for (const auto& blobRefs: args.Blobs) {
        TMaybe<TBlockList> blockList;
        const auto found = State->FindBlockList(
            db,
            args.BlockRange.Start / State->GetZoneBlockCount(),
            blobRefs.BlobId,
            blockList
        );

        if (!found) {
            ready = false;
        }

        if (ready) {
            Y_ABORT_UNLESS(blockList.Defined());

            auto blocks = blockList->GetBlocks();
            const auto blobRange = TBlockRange32::MakeClosedInterval(
                blocks.front().BlockIndex,
                blocks.back().BlockIndex);
            ready &= State->InitIndex(db, blobRange);

            if (ready) {
                // TODO: collect only deleted blocks
                args.AffectedBlobInfos.emplace_back(
                    blobRefs.BlobId,
                    std::move(blocks)
                );

                MarkOverwrittenBlocks(
                    blobRefs.Blocks,
                    args.AffectedBlobInfos.back().Blocks
                );
            }
        }
    }

    return ready;
}

void TPartitionActor::ExecuteCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompaction& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    args.CommitId = State->GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        // commit id overflow, cannot proceed
        return;
    }
    State->AcquireCollectBarrier(args.CommitId);
}

void TPartitionActor::CompleteCompaction(
    const TActorContext& ctx,
    TTxPartition::TCompaction& args)
{
    if (args.CommitId == InvalidCommitId) {
        RebootPartitionOnCommitIdOverflow(ctx, "Compaction");
        return;
    }

    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    // prepare read requests
    TVector<TCompactionActor::TReadRequest> readRequests(
        Reserve(args.Blobs.size()));

    size_t blockCount = 0;
    for (auto& blob: args.Blobs) {
        Y_ABORT_UNLESS(IsSorted(blob.Blocks.begin(), blob.Blocks.end()));
        blockCount += blob.Blocks.size();

        TActorId proxy;
        if (!IsDeletionMarker(blob.BlobId)) {
            proxy = Info()->BSProxyIDForChannel(
                blob.BlobId.Channel(),
                blob.BlobId.Generation());
        }

        const auto dataBlockCount = blob.DataBlobOffsets.size();
        readRequests.emplace_back(
            blob.BlobId,
            proxy,
            std::move(blob.Blocks),
            std::move(blob.DataBlobOffsets),
            Info()->GroupFor(blob.BlobId.Channel(), blob.BlobId.Generation()));
        readRequests.back().DataBlockCount = dataBlockCount;

        TBlockBuffer blockBuffer(TProfilingAllocator::Instance());
        for (size_t i = 0; i < dataBlockCount; ++i) {
            blockBuffer.AddBlock(State->GetBlockSize(), 0);
        }
        readRequests.back().BlobContent = TGuardedBuffer(std::move(blockBuffer));
    }

    // arrange blocks
    TVector<TCompactionActor::TBlockRef> blockRefs(Reserve(blockCount));
    for (const auto& req: readRequests) {
        ui16 blobOffset = 0;
        for (ui32 i = 0; i < req.Blocks.size(); ++i) {
            if (req.Blocks[i].Zeroed) {
                blockRefs.push_back({ &req, i, ZeroBlobOffset });
            } else {
                blockRefs.push_back({ &req, i, blobOffset });
                ++blobOffset;
            }
        }
    }

    Sort(blockRefs, [] (const auto& l, const auto& r) {
        return l.Request->Blocks[l.Index] < r.Request->Blocks[r.Index];
    });

    // prepare write requests
    TVector<TCompactionActor::TWriteRequest> writeRequests(
        Reserve(args.Blobs.size()));

    TCompactionActor::TWriteRequest* req = nullptr;
    ui32 blobIndex = 0;

    const EChannelPermissions compactionPermissions = EChannelPermission::SystemWritesAllowed;

    for (const auto& ref: blockRefs) {
        const auto& currentBlock = ref.Request->Blocks[ref.Index];

        if (req) {
            const auto& firstBlock = req->Blocks.front().Request->Blocks[
                req->Blocks.front().Index
            ];
            const auto rangeSize =
                currentBlock.BlockIndex - firstBlock.BlockIndex + 1;
            if (req->Blocks.size() == State->GetMaxBlocksInBlob()
                    || rangeSize > State->GetMaxBlocksInBlob())
            {
                req->BlobId = State->GenerateBlobId(
                    EChannelDataKind::Merged,
                    compactionPermissions,
                    args.CommitId,
                    req->DataBlockCount * State->GetBlockSize(),
                    blobIndex++);
                req = nullptr;
            }
        }

        if (!req) {
            writeRequests.resize(writeRequests.size() + 1);
            req = &writeRequests.back();
        }

        req->Blocks.push_back(ref);
        if (!currentBlock.Zeroed) {
            ++req->DataBlockCount;
        }
    }

    if (req) {
        req->BlobId = State->GenerateBlobId(
            EChannelDataKind::Merged,
            compactionPermissions,
            args.CommitId,
            req->DataBlockCount * State->GetBlockSize(),
            blobIndex++);
    }

    auto readBlobTimeout =
        PartitionConfig.GetStorageMediaKind() == NProto::STORAGE_MEDIA_SSD ?
        Config->GetBlobStorageAsyncGetTimeoutSSD() :
        Config->GetBlobStorageAsyncGetTimeoutHDD();


    const auto compactionType =
        args.CompactionOptions.test(ToBit(ECompactionOption::Forced)) ?
            ECompactionType::Forced:
            ECompactionType::Tablet;

    auto actor = NCloud::Register<TCompactionActor>(
        ctx,
        args.RequestInfo,
        BlockDigestGenerator,
        State->GetBlockSize(),
        TabletID(),
        SelfId(),
        args.CommitId,
        args.BlockRange,
        readBlobTimeout,
        compactionType,
        std::move(args.GarbageInfo),
        std::move(args.AffectedBlobInfos),
        args.BlobsSkipped,
        args.BlocksSkipped,
        std::move(readRequests),
        std::move(writeRequests));

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
