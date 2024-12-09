#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/alloc.h>
#include <cloud/storage/core/libs/common/block_buffer.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRangeCompactionInfo
{
    const TBlockRange32 BlockRange;
    const TPartialBlobId OriginalBlobId;
    const TPartialBlobId DataBlobId;
    const TBlockMask DataBlobSkipMask;
    const TPartialBlobId ZeroBlobId;
    const TBlockMask ZeroBlobSkipMask;
    const ui32 BlobsSkippedByCompaction;
    const ui32 BlocksSkippedByCompaction;
    const TVector<ui32> BlockChecksums;

    TGuardedBuffer<TBlockBuffer> BlobContent;
    TVector<ui32> ZeroBlocks;
    TAffectedBlobs AffectedBlobs;
    TAffectedBlocks AffectedBlocks;
    TVector<ui16> UnchangedBlobOffsets;
    TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> Diffs;
    ui32 DiffCount = 0;

    TRangeCompactionInfo(
            TBlockRange32 blockRange,
            TPartialBlobId originalBlobId,
            TPartialBlobId dataBlobId,
            TBlockMask dataBlobSkipMask,
            TPartialBlobId zeroBlobId,
            TBlockMask zeroBlobSkipMask,
            ui32 blobsSkippedByCompaction,
            ui32 blocksSkippedByCompaction,
            TVector<ui32> blockChecksums,
            TBlockBuffer blobContent,
            TVector<ui32> zeroBlocks,
            TAffectedBlobs affectedBlobs,
            TAffectedBlocks affectedBlocks)
        : BlockRange(blockRange)
        , OriginalBlobId(originalBlobId)
        , DataBlobId(dataBlobId)
        , DataBlobSkipMask(dataBlobSkipMask)
        , ZeroBlobId(zeroBlobId)
        , ZeroBlobSkipMask(zeroBlobSkipMask)
        , BlobsSkippedByCompaction(blobsSkippedByCompaction)
        , BlocksSkippedByCompaction(blocksSkippedByCompaction)
        , BlockChecksums(std::move(blockChecksums))
        , BlobContent(std::move(blobContent))
        , ZeroBlocks(std::move(zeroBlocks))
        , AffectedBlobs(std::move(affectedBlobs))
        , AffectedBlocks(std::move(affectedBlocks))
    {}
};

class TCompactionActor final
    : public TActorBootstrapped<TCompactionActor>
{
public:
    struct TRequest
    {
        TPartialBlobId BlobId;
        TActorId Proxy;
        ui16 BlobOffset;
        ui32 BlockIndex;
        size_t IndexInBlobContent;
        ui32 GroupId;
        ui32 RangeCompactionIndex;

        TRequest(const TPartialBlobId& blobId,
                 const TActorId& proxy,
                 ui16 blobOffset,
                 ui32 blockIndex,
                 size_t indexInBlobContent,
                 ui32 groupId,
                 ui32 rangeCompactionIndex)
            : BlobId(blobId)
            , Proxy(proxy)
            , BlobOffset(blobOffset)
            , BlockIndex(blockIndex)
            , IndexInBlobContent(indexInBlobContent)
            , GroupId(groupId)
            , RangeCompactionIndex(rangeCompactionIndex)
        {}
    };

    struct TBatchRequest
    {
        TPartialBlobId BlobId;
        TActorId Proxy;
        TVector<ui16> BlobOffsets;
        TVector<ui16> UnchangedBlobOffsets;
        TVector<TRequest*> Requests;
        TRangeCompactionInfo* RangeCompactionInfo = nullptr;
        ui32 GroupId = 0;

        TBatchRequest() = default;

        TBatchRequest(const TPartialBlobId& blobId,
                      const TActorId& proxy,
                      TVector<ui16> blobOffsets,
                      TVector<ui16> unchangedBlobOffsets,
                      TVector<TRequest*> requests,
                      TRangeCompactionInfo* rangeCompactionInfo,
                      ui32 groupId)
            : BlobId(blobId)
            , Proxy(proxy)
            , BlobOffsets(std::move(blobOffsets))
            , UnchangedBlobOffsets(std::move(unchangedBlobOffsets))
            , Requests(std::move(requests))
            , RangeCompactionInfo(rangeCompactionInfo)
            , GroupId(groupId)
        {}
    };

private:
    const TRequestInfoPtr RequestInfo;

    const ui64 TabletId;
    const TActorId Tablet;
    const ui32 BlockSize;
    const ui32 MaxBlocksInBlob;
    const ui32 MaxAffectedBlocksPerCompaction;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const TDuration ReadBlobTimeout;
    const ECompactionType CompactionType;

    const ui64 CommitId;

    TVector<TRangeCompactionInfo> RangeCompactionInfos;
    TVector<TRequest> Requests;

    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

    TVector<TBatchRequest> BatchRequests;
    size_t ReadRequestsCompleted = 0;
    size_t RealReadRequestsCompleted = 0;
    size_t WriteAndPatchBlobRequestsCompleted = 0;

    ui64 ReadExecCycles = 0;
    ui64 ReadWaitCycles = 0;
    ui64 MaxExecCyclesFromRead = 0;
    ui64 MaxExecCyclesFromWrite = 0;

    TVector<TCallContextPtr> ForkedReadCallContexts;
    TVector<TCallContextPtr> ForkedWriteAndPatchCallContexts;
    bool SafeToUseOrbit = true;

public:
    TCompactionActor(
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        const TActorId& tablet,
        ui32 blockSize,
        ui32 maxBlocksInBlob,
        ui32 maxAffectedBlocksPerCompaction,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TDuration readBlobTimeout,
        ECompactionType compactionType,
        ui64 commitId,
        TVector<TRangeCompactionInfo> rangeCompactionInfos,
        TVector<TRequest> requests);

    void Bootstrap(const TActorContext& ctx);

private:
    void InitBlockDigests();

    void ReadBlocks(const TActorContext& ctx);
    void WriteBlobs(const TActorContext& ctx);
    void AddBlobs(const TActorContext& ctx);
    void MakeDiffs(TRangeCompactionInfo& rc);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvPartitionPrivate::TEvCompactionResponse> response);

private:
    STFUNC(StateWork);

    template <typename TEvent>
    void HandleWriteOrPatchBlobResponse(
        TEvent& ev,
        const TActorContext& ctx);

    void HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteBlobResponse(
        const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePatchBlobResponse(
        const TEvPartitionPrivate::TEvPatchBlobResponse::TPtr& ev,
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
        ui64 tabletId,
        const TActorId& tablet,
        ui32 blockSize,
        ui32 maxBlocksInBlob,
        ui32 maxAffectedBlocksPerCompaction,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TDuration readBlobTimeout,
        ECompactionType compactionType,
        ui64 commitId,
        TVector<TRangeCompactionInfo> rangeCompactionInfos,
        TVector<TRequest> requests)
    : RequestInfo(std::move(requestInfo))
    , TabletId(tabletId)
    , Tablet(tablet)
    , BlockSize(blockSize)
    , MaxBlocksInBlob(maxBlocksInBlob)
    , MaxAffectedBlocksPerCompaction(maxAffectedBlocksPerCompaction)
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , ReadBlobTimeout(readBlobTimeout)
    , CompactionType(compactionType)
    , CommitId(commitId)
    , RangeCompactionInfos(std::move(rangeCompactionInfos))
    , Requests(std::move(requests))
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

    if (Requests) {
        ReadBlocks(ctx);

        if (ReadRequestsCompleted == Requests.size()) {
            // all blocks are in Fresh index there is nothing to read
            WriteBlobs(ctx);
        }
    } else {
        InitBlockDigests();

        // for zeroed range we only add deletion marker to the index
        AddBlobs(ctx);
    }
}

void TCompactionActor::InitBlockDigests()
{
    for (auto& rc: RangeCompactionInfos) {
        const auto& sgList = rc.BlobContent.Get().GetBlocks();

        if (rc.DataBlobId) {
            Y_ABORT_UNLESS(sgList.size() == rc.BlockRange.Size() - rc.DataBlobSkipMask.Count());

            ui32 skipped = 0;
            for (const auto blockIndex: xrange(rc.BlockRange)) {
                if (rc.DataBlobSkipMask.Get(blockIndex - rc.BlockRange.Start)) {
                    ++skipped;
                    continue;
                }

                const auto digest = BlockDigestGenerator->ComputeDigest(
                    blockIndex,
                    sgList[blockIndex - rc.BlockRange.Start - skipped]
                );

                if (digest.Defined()) {
                    AffectedBlockInfos.push_back({blockIndex, *digest});
                }
            }
        }

        if (rc.ZeroBlobId) {
            for (const auto blockIndex: xrange(rc.BlockRange)) {
                if (rc.ZeroBlobSkipMask.Get(blockIndex - rc.BlockRange.Start)) {
                    continue;
                }

                const auto digest = BlockDigestGenerator->ComputeDigest(
                    blockIndex,
                    TBlockDataRef::CreateZeroBlock(BlockSize)
                );

                if (digest.Defined()) {
                    AffectedBlockInfos.push_back({blockIndex, *digest});
                }
            }
        }
    }
}

void TCompactionActor::ReadBlocks(const TActorContext& ctx)
{
    TVector<TRequest*> requests(Reserve(Requests.size()));
    for (auto& r: Requests) {
        requests.push_back(&r);
    }

    Sort(requests, [] (const TRequest* l, const TRequest* r) {
        return l->BlobId < r->BlobId
            || l->BlobId == r->BlobId && l->BlobOffset < r->BlobOffset;
    });

    TBatchRequest current;
    for (auto* r: requests) {
        if (IsDeletionMarker(r->BlobId)) {
            ++ReadRequestsCompleted;
            continue;
        }

        if (current.BlobId != r->BlobId) {
            if (current.BlobOffsets || current.UnchangedBlobOffsets) {
                BatchRequests.emplace_back(
                    current.BlobId,
                    current.Proxy,
                    std::move(current.BlobOffsets),
                    std::move(current.UnchangedBlobOffsets),
                    std::move(current.Requests),
                    current.RangeCompactionInfo,
                    current.GroupId);
            }
            current.GroupId = r->GroupId;
            current.BlobId = r->BlobId;
            current.Proxy = r->Proxy;
            current.RangeCompactionInfo =
                &RangeCompactionInfos[r->RangeCompactionIndex];
        }

        if (current.BlobId == current.RangeCompactionInfo->OriginalBlobId) {
            if (r->IndexInBlobContent == r->BlobOffset) {
                current.UnchangedBlobOffsets.push_back(r->BlobOffset);
            } else {
                current.BlobOffsets.push_back(r->BlobOffset);
                current.Requests.push_back(r);
            }
        } else {
            current.BlobOffsets.push_back(r->BlobOffset);
            current.Requests.push_back(r);
        }
    }

    if (current.BlobOffsets || current.UnchangedBlobOffsets) {
        BatchRequests.emplace_back(
            current.BlobId,
            current.Proxy,
            std::move(current.BlobOffsets),
            std::move(current.UnchangedBlobOffsets),
            std::move(current.Requests),
            current.RangeCompactionInfo,
            current.GroupId);
    }

    const auto readBlobDeadline = ReadBlobTimeout ?
        ctx.Now() + ReadBlobTimeout :
        TInstant::Max();

    for (ui32 batchIndex = 0; batchIndex < BatchRequests.size(); ++batchIndex) {
        auto& batch = BatchRequests[batchIndex];
        if (batch.UnchangedBlobOffsets) {
            batch.RangeCompactionInfo->UnchangedBlobOffsets =
                std::move(batch.UnchangedBlobOffsets);
            ReadRequestsCompleted +=
                batch.RangeCompactionInfo->UnchangedBlobOffsets.size();

            if (batch.BlobOffsets.empty()) {
                continue;
            }
        }

        auto& blobContent = batch.RangeCompactionInfo->BlobContent;
        const auto& srcSglist = blobContent.Get().GetBlocks();

        TSgList subset(Reserve(batch.Requests.size()));
        for (const auto* r: batch.Requests) {
            subset.push_back(srcSglist[r->IndexInBlobContent]);
        }

        // TODO: initialize checksums at UnchangedBlobOffsets - right now we
        // leave zeroes at those offsets => checksum verification can produce
        // false negatives in case BlobPatchingEnabled == true

        auto subSgList = blobContent.CreateGuardedSgList(std::move(subset));

        const bool shouldCalculateChecksums =
            !batch.RangeCompactionInfo->BlockChecksums.empty();
        auto request = std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
            MakeBlobId(TabletId, batch.BlobId),
            batch.Proxy,
            std::move(batch.BlobOffsets),
            std::move(subSgList),
            batch.GroupId,
            true,            // async
            readBlobDeadline, // deadline
            shouldCalculateChecksums
        );


        if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                RequestInfo->CallContext->LWOrbit,
                "TEvPartitionCommonPrivate::TEvReadBlobRequest",
                RequestInfo->CallContext->RequestId);
        }

        ForkedReadCallContexts.emplace_back(request->CallContext);

        NCloud::Send(
            ctx,
            Tablet,
            std::move(request),
            batchIndex);
    }
}

void TCompactionActor::MakeDiffs(TRangeCompactionInfo& rc)
{
    const auto& sgList = rc.BlobContent.Get().GetBlocks();

    rc.DiffCount = sgList.size() - rc.UnchangedBlobOffsets.size();
    rc.Diffs.Reset(new TEvBlobStorage::TEvPatch::TDiff[rc.DiffCount]);
    ui32 i = 0;
    ui32 j = 0;
    while (true) {
        while (j < rc.UnchangedBlobOffsets.size()
                && rc.UnchangedBlobOffsets[j] == i)
        {
            ++i;
            ++j;
        }

        if (i == sgList.size()) {
            break;
        }

        auto& dataBlock = sgList[i];
        TString buffer(dataBlock.Data(), dataBlock.Size());
        rc.Diffs[i - j].Set(buffer, i * BlockSize);
        ++i;
    }
}

void TCompactionActor::WriteBlobs(const TActorContext& ctx)
{
    InitBlockDigests();

    for (auto& rc: RangeCompactionInfos) {
        if (!rc.DataBlobId) {
            ++WriteAndPatchBlobRequestsCompleted;
            continue;
        }

        if (rc.OriginalBlobId) {
            MakeDiffs(rc);

            auto request = std::make_unique<TEvPartitionPrivate::TEvPatchBlobRequest>(
                rc.OriginalBlobId,
                rc.DataBlobId,
                std::move(rc.Diffs),
                rc.DiffCount,
                true,               // async
                TInstant::Max());   // deadline

            if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionPrivate::TEvPatchBlobRequest",
                    RequestInfo->CallContext->RequestId);
            }

            ForkedWriteAndPatchCallContexts.emplace_back(request->CallContext);

            NCloud::Send(
                ctx,
                Tablet,
                std::move(request));
        } else {
            auto request = std::make_unique<TEvPartitionPrivate::TEvWriteBlobRequest>(
                rc.DataBlobId,
                rc.BlobContent.GetGuardedSgList(),
                0,      // blockSizeForChecksums
                true);  // async

            if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionPrivate::TEvWriteBlobRequest",
                    RequestInfo->CallContext->RequestId);
            }

            ForkedWriteAndPatchCallContexts.emplace_back(request->CallContext);

            NCloud::Send(
                ctx,
                Tablet,
                std::move(request));
        }
    }

    SafeToUseOrbit = false;
}

void TCompactionActor::AddBlobs(const TActorContext& ctx)
{
    TVector<TAddMergedBlob> mergedBlobs;
    TVector<TMergedBlobCompactionInfo> blobCompactionInfos;
    TAffectedBlobs affectedBlobs;
    TAffectedBlocks affectedBlocks;

    auto addBlob = [&] (
        const TPartialBlobId& blobId,
        TBlockRange32 range,
        TBlockMask skipMask,
        const TVector<ui32>& blockChecksums,
        ui32 blobsSkipped,
        ui32 blocksSkipped)
    {
        while (skipMask.Get(range.End - range.Start)) {
            Y_ABORT_UNLESS(range.End > range.Start);
            // modifying skipMask is crucial since otherwise there would be
            // 2 blobs with the same key in merged index (the key is
            // commitId + blockRange.End)
            skipMask.Reset(range.End - range.Start);
            --range.End;
        }

        mergedBlobs.emplace_back(blobId, range, skipMask, blockChecksums);

        blobCompactionInfos.push_back({blobsSkipped, blocksSkipped});
    };

    for (auto& rc: RangeCompactionInfos) {
        if (rc.DataBlobId) {
            addBlob(
                rc.DataBlobId,
                rc.BlockRange,
                rc.DataBlobSkipMask,
                rc.BlockChecksums,
                rc.BlobsSkippedByCompaction,
                rc.BlocksSkippedByCompaction);
        }

        if (rc.ZeroBlobId) {
            ui32 blobsSkipped = 0;
            ui32 blocksSkipped = 0;

            if (!rc.DataBlobId) {
                blobsSkipped = rc.BlobsSkippedByCompaction;
                blocksSkipped = rc.BlocksSkippedByCompaction;
            }

            addBlob(
                rc.ZeroBlobId,
                rc.BlockRange,
                rc.ZeroBlobSkipMask,
                rc.BlockChecksums,
                blobsSkipped,
                blocksSkipped);
        }

        if (rc.DataBlobId && rc.ZeroBlobId) {
            // if both blobs are present, none of them should contain all range
            // blocks
            Y_ABORT_UNLESS(rc.DataBlobSkipMask.Count());
            Y_ABORT_UNLESS(rc.ZeroBlobSkipMask.Count());
        }

        for (auto it = rc.AffectedBlobs.begin(); it != rc.AffectedBlobs.end(); ) {
            auto& blockMask = it->second.BlockMask.GetRef();

            // could already be full
            if (IsBlockMaskFull(blockMask, MaxBlocksInBlob)) {
                ++it;

                continue;
            }

            // mask overwritten blocks
            for (ui16 blobOffset: it->second.Offsets) {
                blockMask.Set(blobOffset);
            }

            auto& affectedBlob = affectedBlobs[it->first];
            Y_ABORT_UNLESS(affectedBlob.Offsets.empty());
            Y_ABORT_UNLESS(affectedBlob.BlockMask.Empty());
            Y_ABORT_UNLESS(affectedBlob.AffectedBlockIndices.empty());
            Y_ABORT_UNLESS(affectedBlob.BlobMeta.Empty());
            affectedBlob = std::move(it->second);

            ++it;
        }

        Sort(rc.AffectedBlocks, [] (const auto& l, const auto& r) {
            // sort by (BlockIndex ASC, CommitId DESC)
            return (l.BlockIndex < r.BlockIndex)
                || (l.BlockIndex == r.BlockIndex && l.CommitId > r.CommitId);
        });

        if (rc.AffectedBlocks.size() > MaxAffectedBlocksPerCompaction) {
            // KIKIMR-6286: preventing heavy transactions
            LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Cropping AffectedBlocks: %lu -> %lu, range: %s",
                TabletId,
                rc.AffectedBlocks.size(),
                MaxAffectedBlocksPerCompaction,
                DescribeRange(rc.BlockRange).c_str());

            rc.AffectedBlocks.crop(MaxAffectedBlocksPerCompaction);
        }

        affectedBlocks.insert(
            affectedBlocks.end(),
            rc.AffectedBlocks.begin(),
            rc.AffectedBlocks.end());
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
        RequestInfo->CallContext,
        CommitId,
        TVector<TAddMixedBlob>(),
        std::move(mergedBlobs),
        TVector<TAddFreshBlob>(),
        ADD_COMPACTION_RESULT,
        std::move(affectedBlobs),
        std::move(affectedBlocks),
        std::move(blobCompactionInfos));

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

    request->ExecCycles = RequestInfo->GetExecCycles();
    request->TotalCycles = RequestInfo->GetTotalCycles();

    request->CommitId = CommitId;

    {
        auto execTime = CyclesToDurationSafe(ReadExecCycles);
        auto waitTime = CyclesToDurationSafe(ReadWaitCycles);

        SetCounters(
            *request->Stats.MutableSysReadCounters(),
            execTime,
            waitTime,
            ReadRequestsCompleted);
        SetCounters(
            *request->Stats.MutableRealSysReadCounters(),
            execTime,
            waitTime,
            RealReadRequestsCompleted);
    }

    {
        auto execCycles = RequestInfo->GetExecCycles();
        auto totalCycles = RequestInfo->GetTotalCycles();
        TDuration execTime = CyclesToDurationSafe(execCycles - ReadExecCycles);
        TDuration waitTime;
        if (totalCycles > execCycles + ReadWaitCycles) {
            waitTime = CyclesToDurationSafe(totalCycles - execCycles - ReadWaitCycles);
        }

        ui64 blocksCount = 0;
        ui64 realBlocksCount = 0;
        for (auto& rc: RangeCompactionInfos) {
            const auto curBlocksCount = rc.DataBlobId.BlobSize() / BlockSize;
            blocksCount += curBlocksCount;
            realBlocksCount += rc.OriginalBlobId ? rc.DiffCount : curBlocksCount;
        }

        SetCounters(
            *request->Stats.MutableSysWriteCounters(),
            execTime,
            waitTime,
            blocksCount);
        SetCounters(
            *request->Stats.MutableRealSysWriteCounters(),
            execTime,
            waitTime,
            realBlocksCount);
    }

    for (const auto& rc: RangeCompactionInfos) {
        request->AffectedRanges.push_back(ConvertRangeSafe(rc.BlockRange));
    }
    request->AffectedBlockInfos = std::move(AffectedBlockInfos);
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
    const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    MaxExecCyclesFromRead = Max(MaxExecCyclesFromRead, msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    ui32 batchIndex = ev->Cookie;

    Y_ABORT_UNLESS(batchIndex < BatchRequests.size());
    auto& batch = BatchRequests[batchIndex];
    const auto& rc = *batch.RangeCompactionInfo;

    const auto n = Min(batch.Requests.size(), msg->BlockChecksums.size());
    for (ui32 i = 0; i < n; ++i) {
        const auto* r = batch.Requests[i];
        const auto expectedChecksum =
            rc.BlockChecksums[r->IndexInBlobContent];

        auto error = VerifyBlockChecksum(
            msg->BlockChecksums[i],
            MakeBlobId(TabletId, r->BlobId),
            r->BlockIndex,
            r->BlobOffset,
            expectedChecksum);

        if (HasError(error)) {
            HandleError(ctx, error);
            return;
        }
    }

    RealReadRequestsCompleted += batch.Requests.size();
    ReadRequestsCompleted += batch.Requests.size();
    Y_ABORT_UNLESS(ReadRequestsCompleted <= Requests.size());
    if (ReadRequestsCompleted < Requests.size()) {
        return;
    }

    RequestInfo->AddExecCycles(MaxExecCyclesFromRead);

    ReadExecCycles = RequestInfo->GetExecCycles();
    ReadWaitCycles = RequestInfo->GetWaitCycles();

    for (auto context: ForkedReadCallContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    WriteBlobs(ctx);
}

template <typename TEvent>
void TCompactionActor::HandleWriteOrPatchBlobResponse(
    TEvent& ev,
    const TActorContext& ctx)
{
    auto* msg = ev.Get();

    MaxExecCyclesFromWrite = Max(MaxExecCyclesFromWrite, msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    ++WriteAndPatchBlobRequestsCompleted;
    Y_ABORT_UNLESS(WriteAndPatchBlobRequestsCompleted <= RangeCompactionInfos.size());
    if (WriteAndPatchBlobRequestsCompleted < RangeCompactionInfos.size()) {
        return;
    }

    RequestInfo->AddExecCycles(MaxExecCyclesFromWrite);

    SafeToUseOrbit = true;

    for (auto context: ForkedWriteAndPatchCallContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    AddBlobs(ctx);
}

void TCompactionActor::HandleWriteBlobResponse(
    const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleWriteOrPatchBlobResponse(*ev, ctx);
}

void TCompactionActor::HandlePatchBlobResponse(
    const TEvPartitionPrivate::TEvPatchBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleWriteOrPatchBlobResponse(*ev, ctx);
}

void TCompactionActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    SafeToUseOrbit = true;

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

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
        HFunc(TEvPartitionCommonPrivate::TEvReadBlobResponse, HandleReadBlobResponse);
        HFunc(TEvPartitionPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvPartitionPrivate::TEvPatchBlobResponse, HandlePatchBlobResponse);
        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCompactionBlockVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
{
private:
    TTxPartition::TRangeCompaction& Args;
    const ui64 MaxCommitId;

public:
    TCompactionBlockVisitor(
            TTxPartition::TRangeCompaction& args,
            ui64 maxCommitId)
        : Args(args)
        , MaxCommitId(maxCommitId)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        Args.MarkBlock(
            block.Meta.BlockIndex,
            block.Meta.CommitId,
            block.Content);
        return true;
    }

    bool KeepTrackOfAffectedBlocks = false;

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        if (commitId > MaxCommitId) {
            return true;
        }

        Args.MarkBlock(
            blockIndex,
            commitId,
            blobId,
            blobOffset,
            KeepTrackOfAffectedBlocks);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

ui32 GetPercentage(ui64 total, ui64 real)
{
    const double p = (real - total) * 100. / Max(total, 1UL);
    const double MAX_P = 1'000;
    return Min(p, MAX_P);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////


void TPartitionActor::ChangeRangeCountPerRunIfNeeded(
    ui64 rangeRealCount,
    ui64 rangeThreshold,
    ui64 diskRealCount,
    ui64 diskThreshold,
    const TActorContext& ctx)
{
    const auto countPerRunIncreasingThreshold =
        Config->GetCompactionCountPerRunIncreasingThreshold();
    const auto countPerRunDecreasingThreshold =
        Config->GetCompactionCountPerRunDecreasingThreshold();

    ui32 thresholdPercentage = 0;

    if (rangeThreshold && rangeRealCount > rangeThreshold) {
        thresholdPercentage =
            GetPercentage(rangeThreshold, rangeRealCount);
    }

    if (diskThreshold && diskRealCount > diskThreshold) {
        thresholdPercentage =
            Max(thresholdPercentage, GetPercentage(diskThreshold, diskRealCount));
    }

    const auto compactionRangeCountPerRun =
        State->GetCompactionRangeCountPerRun();

    if (thresholdPercentage > countPerRunIncreasingThreshold
        && compactionRangeCountPerRun <
        Config->GetMaxCompactionRangeCountPerRun())
    {
        State->IncrementCompactionRangeCountPerRun();
        State->SetLastCompactionRangeCountPerRunTime(ctx.Now());
    } else if (thresholdPercentage < countPerRunDecreasingThreshold &&
        compactionRangeCountPerRun > 1)
    {
        State->DecrementCompactionRangeCountPerRun();
        State->SetLastCompactionRangeCountPerRunTime(ctx.Now());
    }
}

void TPartitionActor::EnqueueCompactionIfNeeded(const TActorContext& ctx)
{
    if (State->GetCompactionState(ECompactionType::Tablet).Status !=
        EOperationStatus::Idle)
    {
        // already enqueued
        return;
    }

    if (!State->IsCompactionAllowed()) {
        // not allowed for now
        return;
    }

    // TODO: move this logic to TPartitionState to simplify unit testing
    const auto& cm = State->GetCompactionMap();
    auto topRange = cm.GetTop();
    auto topByGarbageBlockCount = cm.GetTopByGarbageBlockCount();
    TEvPartitionPrivate::ECompactionMode mode =
        TEvPartitionPrivate::RangeCompaction;
    bool throttlingAllowed = true;

    auto& scoreHistory = State->GetCompactionScoreHistory();
    const auto now = ctx.Now();
    if (scoreHistory.LastTs() + Config->GetMaxCompactionDelay() <= now) {
        scoreHistory.Register({
            now,
            {
                topRange.Stat.CompactionScore.Score,
                topByGarbageBlockCount.Stat.GarbageBlockCount(),
            },
        });
    }

    const auto blockCount = State->GetMixedBlocksCount()
        + State->GetMergedBlocksCount() - State->GetCleanupQueue().GetQueueBlocks();
    const auto diskGarbage =
        GetPercentage(State->GetUsedBlocksCount(), blockCount);

    const bool diskGarbageBelowThreshold =
        diskGarbage < Config->GetCompactionGarbageThreshold();

    const auto blobCount = State->GetMixedBlobsCount() +
        State->GetMergedBlobsCount();

    const bool diskBlobCountOverThreshold = State->GetMaxBlobsPerDisk() &&
        blobCount > State->GetMaxBlobsPerDisk() + State->GetCleanupQueue().GetCount();

    ui32 rangeGarbage = 0;

    if (topRange.Stat.CompactionScore.Score <= 0  && !diskBlobCountOverThreshold) {
        if (!Config->GetV1GarbageCompactionEnabled()) {
            // nothing to compact
            return;
        }

        if (!State->GetCheckpoints().IsEmpty()) {
            // should not compact, see NBS-1042
            return;
        }

        // ranges containing 0 used blocks could have a nonzero BlockCount value
        // in the corresponding compaction range before r7082716
        const auto isZeroedRange = topByGarbageBlockCount.Stat.BlockCount
            && !topByGarbageBlockCount.Stat.UsedBlockCount;

        if (topByGarbageBlockCount.Stat.Compacted
                || topByGarbageBlockCount.Stat.BlobCount < 2
                && !isZeroedRange)
        {
            // nothing to compact
            return;
        }

        rangeGarbage = GetPercentage(
            topByGarbageBlockCount.Stat.UsedBlockCount,
            topByGarbageBlockCount.Stat.BlockCount
        );

        if (rangeGarbage < Config->GetCompactionRangeGarbageThreshold()) {
            // not enough garbage in this range
            if (diskGarbageBelowThreshold) {
                // and not enough garbage on the whole disk, no need to compact
                return;
            }

            if (rangeGarbage < Config->GetCompactionGarbageThreshold()) {
                // really not enough garbage in this range, see NBS-1045
                return;
            }
            PartCounters->Cumulative.CompactionByGarbageBlocksPerDisk.Increment(1);
        } else {
            PartCounters->Cumulative.CompactionByGarbageBlocksPerRange.Increment(1);
        }

        mode = TEvPartitionPrivate::GarbageCompaction;
    } else if (topRange.Stat.CompactionScore.Score >= Config->GetCompactionScoreLimitForThrottling()) {
        throttlingAllowed = false;
    }

    State->GetCompactionState(ECompactionType::Tablet).SetStatus(
        EOperationStatus::Enqueued);

    if (Config->GetCompactionCountPerRunIncreasingThreshold()
        && Config->GetCompactionCountPerRunDecreasingThreshold()
        && now - State->GetLastCompactionRangeCountPerRunTime() >
        Config->GetCompactionCountPerRunChangingPeriod())
    {
        switch (mode) {
            case TEvPartitionPrivate::GarbageCompaction: {
                ChangeRangeCountPerRunIfNeeded(
                    rangeGarbage,
                    Config->GetCompactionRangeGarbageThreshold(),
                    diskGarbage,
                    Config->GetCompactionGarbageThreshold(),
                    ctx);
                break;
            }
            case TEvPartitionPrivate::RangeCompaction: {
                ChangeRangeCountPerRunIfNeeded(
                    topRange.Stat.BlobCount,
                    State->GetMaxBlobsPerRange(),
                    blobCount,
                    State->GetMaxBlobsPerDisk(),
                    ctx);
                break;
            }
        }
    }

    if (topRange.Stat.CompactionScore.Score <= 0 && diskBlobCountOverThreshold) {
        PartCounters->Cumulative.CompactionByBlobCountPerDisk.Increment(1);
    } else if (mode != TEvPartitionPrivate::GarbageCompaction) {
        switch (topRange.Stat.CompactionScore.Type) {
            case TCompactionScore::EType::BlobCount: {
                PartCounters->Cumulative.CompactionByBlobCountPerRange.Increment(1);
                break;
            }
            case TCompactionScore::EType::Read: {
                PartCounters->Cumulative.CompactionByReadStats.Increment(1);
                break;
            }
        }
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()),
        mode);

    if (mode == TEvPartitionPrivate::GarbageCompaction
            || !diskGarbageBelowThreshold)
    {
        request->CompactionOptions.set(ToBit(ECompactionOption::Full));
    }

    if (throttlingAllowed && Config->GetMaxCompactionDelay()) {
        auto execTime = State->GetCompactionExecTimeForLastSecond(ctx.Now());
        auto delay = Config->GetMinCompactionDelay();
        if (Config->GetMaxCompactionExecTimePerSecond()) {
            auto throttlingFactor = double(execTime.GetValue())
                / Config->GetMaxCompactionExecTimePerSecond().GetValue();
            const auto throttleDelay = (TDuration::Seconds(1) - execTime) * throttlingFactor;

            delay = Max(delay, throttleDelay);
        }

        delay = Min(delay, Config->GetMaxCompactionDelay());
        State->SetCompactionDelay(delay);
    } else {
        State->SetCompactionDelay({});
    }

    if (State->GetCompactionDelay()) {
        ctx.Schedule(State->GetCompactionDelay(), request.release());
    } else {
        NCloud::Send(
            ctx,
            SelfId(),
            std::move(request));
    }
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

    if (State->GetCompactionState(compactionType).Status ==
        EOperationStatus::Started)
    {
        replyError(ctx, *requestInfo, E_TRY_AGAIN, "compaction already started");
        return;
    }

    if (!State->IsCompactionAllowed()) {
        State->GetCompactionState(compactionType).SetStatus(
            EOperationStatus::Idle);

        replyError(ctx, *requestInfo, E_BS_OUT_OF_SPACE, "all channels readonly");
        return;
    }

    TVector<TCompactionCounter> tops;

    const auto& cm = State->GetCompactionMap();

    if (msg->BlockIndex.Defined()) {
        const auto startIndex = cm.GetRangeStart(*msg->BlockIndex);
        tops.push_back({startIndex, cm.Get(startIndex)});
        State->OnNewCompactionRange();
    } else if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
        const auto& top = State->GetCompactionMap().GetTopByGarbageBlockCount();
        tops.push_back({top.BlockIndex, top.Stat});
    } else {
        ui32 rangeCount = 1;

        const bool batchCompactionEnabledForCloud =
            Config->IsBatchCompactionFeatureEnabled(
                PartitionConfig.GetCloudId(),
                PartitionConfig.GetFolderId(),
                PartitionConfig.GetDiskId());
        const bool batchCompactionEnabled =
            Config->GetBatchCompactionEnabled() || batchCompactionEnabledForCloud;

        if (batchCompactionEnabled) {
            rangeCount = State->GetCompactionRangeCountPerRun();
        }

        tops = State->GetCompactionMap().GetTopsFromGroups(rangeCount);
    }

    if (tops.empty() || !tops.front().Stat.BlobCount) {
        State->GetCompactionState(compactionType).SetStatus(
            EOperationStatus::Idle);

        replyError(ctx, *requestInfo, S_ALREADY, "nothing to compact");
        return;
    }

    ui64 commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        requestInfo->CancelRequest(ctx);
        RebootPartitionOnCommitIdOverflow(ctx, "Compaction");
        return;
    }

    TVector<std::pair<ui32, TBlockRange32>> ranges(Reserve(tops.size()));
    for (const auto& x: tops) {
        const ui32 rangeIdx = cm.GetRangeIndex(x.BlockIndex);

        const auto blockRange = TBlockRange32::MakeClosedIntervalWithLimit(
            x.BlockIndex,
            static_cast<ui64>(x.BlockIndex) + cm.GetRangeSize() - 1,
            State->GetBlocksCount() - 1);

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Start %s compaction @%lu (range: %s, blobs: %u, blocks: %u"
            ", reads: %u, blobsread: %u, blocksread: %u, score: %f)",
            TabletID(),
            compactionType == ECompactionType::Forced ? "forced" : "tablet",
            commitId,
            DescribeRange(blockRange).c_str(),
            x.Stat.BlobCount,
            x.Stat.BlockCount,
            x.Stat.ReadRequestCount,
            x.Stat.ReadRequestBlobCount,
            x.Stat.ReadRequestBlockCount,
            x.Stat.CompactionScore.Score);

        ranges.emplace_back(rangeIdx, blockRange);
    }

    State->GetCompactionState(compactionType).SetStatus(EOperationStatus::Started);

    State->GetCommitQueue().AcquireBarrier(commitId);
    State->GetCleanupQueue().AcquireBarrier(commitId);
    State->GetGarbageQueue().AcquireBarrier(commitId);

    AddTransaction<TEvPartitionPrivate::TCompactionMethod>(*requestInfo);

    auto tx = CreateTx<TCompaction>(
        requestInfo,
        commitId,
        msg->CompactionOptions,
        std::move(ranges));

    ui64 minCommitId = State->GetCommitQueue().GetMinCommitId();
    Y_ABORT_UNLESS(minCommitId <= commitId);

    if (minCommitId == commitId) {
        // start execution
        ExecuteTx(ctx, std::move(tx));
    } else {
        // delay execution until all previous commits completed
        State->GetCommitQueue().Enqueue(std::move(tx), commitId);
    }
}

void TPartitionActor::ProcessCommitQueue(const TActorContext& ctx)
{
    ui64 minCommitId = State->GetCommitQueue().GetMinCommitId();

    while (!State->GetCommitQueue().Empty()) {
        ui64 commitId = State->GetCommitQueue().Peek();
        Y_ABORT_UNLESS(minCommitId <= commitId);

        if (minCommitId == commitId) {
            // start execution
            ExecuteTx(ctx, State->GetCommitQueue().Dequeue());
        } else {
            // delay execution until all previous commits completed
            break;
        }
    }

    // TODO: too many different queues exist
    // Since create checkpoint operation waits for the last commit to complete
    // here we force checkpoints queue to try to proceed to the next
    // create checkpoint request
    ProcessCheckpointQueue(ctx);
}

void TPartitionActor::HandleCompactionCompleted(
    const TEvPartitionPrivate::TEvCompactionCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ui64 commitId = msg->CommitId;
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete compaction @%lu",
        TabletID(),
        commitId);

    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Compaction @%lu failed: %s",
            TabletID(),
            commitId,
            FormatError(msg->GetError()).c_str());
    }

    UpdateStats(msg->Stats);

    State->GetCommitQueue().ReleaseBarrier(commitId);
    State->GetCleanupQueue().ReleaseBarrier(commitId);
    State->GetGarbageQueue().ReleaseBarrier(commitId);

    State->GetCompactionState(msg->CompactionType).SetStatus(
        EOperationStatus::Idle);

    Actors.Erase(ev->Sender);

    const auto d = CyclesToDurationSafe(msg->TotalCycles);
    ui32 blocks = msg->Stats.GetSysReadCounters().GetBlocksCount()
        + msg->Stats.GetSysWriteCounters().GetBlocksCount();
    PartCounters->RequestCounters.Compaction.AddRequest(
        d.MicroSeconds(),
        blocks * State->GetBlockSize());
    State->SetLastCompactionExecTime(d, ctx.Now());

    const auto ts = ctx.Now() - d;

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

    if (msg->AffectedBlockInfos) {
        IProfileLog::TSysReadWriteRequestBlockInfos request;
        request.RequestType = ESysRequestType::Compaction;
        request.BlockInfos = std::move(msg->AffectedBlockInfos);
        request.CommitId = commitId;

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ts;
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }

    EnqueueCompactionIfNeeded(ctx);
    EnqueueCleanupIfNeeded(ctx);
    ProcessCommitQueue(ctx);
}

namespace {

////////////////////////////////////////////////////////////////////////////////

void PrepareRangeCompaction(
    const TStorageConfig& config,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const ui64 commitId,
    const bool fullCompaction,
    const TActorContext& ctx,
    const ui64 tabletId,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& affectedBlobIds,
    bool& ready,
    TPartitionDatabase& db,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args)
{
    const bool incrementalCompactionEnabledForCloud =
        config.IsIncrementalCompactionFeatureEnabled(cloudId, folderId, diskId);
    const bool incrementalCompactionEnabled =
        config.GetIncrementalCompactionEnabled()
        || incrementalCompactionEnabledForCloud;

    TCompactionBlockVisitor visitor(args, commitId);
    state.FindFreshBlocks(visitor, args.BlockRange, commitId);
    visitor.KeepTrackOfAffectedBlocks = true;
    ready &= state.FindMixedBlocksForCompaction(
        db,
        visitor,
        args.RangeIdx);
    visitor.KeepTrackOfAffectedBlocks = false;
    ready &= db.FindMergedBlocks(
        visitor,
        args.BlockRange,
        true,   // precharge
        state.GetMaxBlocksInBlob(),
        commitId);

    if (ready) {
        for (const auto& x: args.AffectedBlobs) {
            if (affectedBlobIds.contains(x.first)) {
                args.Discarded = true;
                return;
            }
        }

        for (const auto& x: args.AffectedBlobs) {
            affectedBlobIds.insert(x.first);
        }
    }

    if (ready
            && incrementalCompactionEnabled
            && !fullCompaction)
    {
        THashMap<TPartialBlobId, ui32, TPartialBlobIdHash> liveBlocks;
        for (const auto& m: args.BlockMarks) {
            if (m.CommitId && m.BlobId) {
                ++liveBlocks[m.BlobId];
            }
        }

        TVector<TPartialBlobId> blobIds;
        blobIds.reserve(liveBlocks.size());
        for (const auto& x: liveBlocks) {
            blobIds.push_back(x.first);
        }

        Sort(
            blobIds.begin(),
            blobIds.end(),
            [&] (const TPartialBlobId& l, const TPartialBlobId& r) {
                return liveBlocks[l] < liveBlocks[r];
            }
        );

        auto it = blobIds.begin();
        args.BlobsSkipped = blobIds.size();
        ui32 blocks = 0;

        while (it != blobIds.end()) {
            const auto bytes = blocks * state.GetBlockSize();
            const auto blobCountOk = args.BlobsSkipped
                <= config.GetMaxSkippedBlobsDuringCompaction();
            const auto byteCountOk =
                bytes >= config.GetTargetCompactionBytesPerOp();

            if (blobCountOk && byteCountOk) {
                break;
            }

            blocks += liveBlocks[*it];
            --args.BlobsSkipped;
            ++it;
        }

        // liveBlocks will contain only skipped blobs after this
        for (auto it2 = blobIds.begin(); it2 != it; ++it2) {
            liveBlocks.erase(*it2);
        }

        while (it != blobIds.end()) {
            args.BlocksSkipped += liveBlocks[*it];
            ++it;
        }

        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Dropping last %u blobs, %u blocks"
            ", remaining blobs: %u, blocks: %u",
            tabletId,
            args.BlobsSkipped,
            args.BlocksSkipped,
            liveBlocks.size(),
            blocks);

        THashSet<ui32> skippedBlockIndices;

        for (const auto& x: liveBlocks) {
            auto ab = args.AffectedBlobs.find(x.first);
            Y_ABORT_UNLESS(ab != args.AffectedBlobs.end());
            for (const auto blockIndex: ab->second.AffectedBlockIndices) {
                // we can actually add extra indices to skippedBlockIndices,
                // but it does not cause data corruption - the important thing
                // is to ensure that all skipped indices are added, not that
                // all non-skipped are preserved
                skippedBlockIndices.insert(blockIndex);
            }
            args.AffectedBlobs.erase(ab);
        }

        if (liveBlocks.size()) {
            TAffectedBlocks affectedBlocks;
            for (const auto& b: args.AffectedBlocks) {
                if (!skippedBlockIndices.contains(b.BlockIndex)) {
                    affectedBlocks.push_back(b);
                }
            }
            args.AffectedBlocks = std::move(affectedBlocks);

            for (auto& m: args.BlockMarks) {
                if (liveBlocks.contains(m.BlobId)) {
                    m = {};
                }
            }
        }
    }

    const ui32 checksumBoundary =
        config.GetDiskPrefixLengthWithBlockChecksumsInBlobs()
        / state.GetBlockSize();
    args.ChecksumsEnabled = args.BlockRange.Start < checksumBoundary;

    for (auto& kv: args.AffectedBlobs) {
        if (db.ReadBlockMask(kv.first, kv.second.BlockMask)) {
            Y_ABORT_UNLESS(kv.second.BlockMask.Defined(),
                "Could not read block mask for blob: %s",
                ToString(MakeBlobId(tabletId, kv.first)).data());
        } else {
            ready = false;
        }

        if (args.ChecksumsEnabled) {
            if (db.ReadBlobMeta(kv.first, kv.second.BlobMeta)) {
                Y_ABORT_UNLESS(kv.second.BlobMeta.Defined(),
                    "Could not read blob meta for blob: %s",
                    ToString(MakeBlobId(tabletId, kv.first)).data());
            } else {
                ready = false;
            }
        }
    }
}

void CompleteRangeCompaction(
    const bool blobPatchingEnabled,
    const ui64 commitId,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    TVector<TCompactionActor::TRequest>& requests,
    TVector<TRangeCompactionInfo>& result,
    ui32 maxDiffPercentageForBlobPatching)
{
    const EChannelPermissions compactionPermissions =
        EChannelPermission::SystemWritesAllowed;
    const auto initialRequestsSize = requests.size();

    // at first we count number of data blocks
    size_t dataBlocksCount = 0, zeroBlocksCount = 0;
    for (const auto& mark: args.BlockMarks) {
        if (mark.CommitId) {
            // there could be fresh block OR merged/mixed block
            Y_ABORT_UNLESS(!(mark.BlockContent && !IsDeletionMarker(mark.BlobId)));
            if (mark.BlockContent || !IsDeletionMarker(mark.BlobId)) {
                ++dataBlocksCount;
            } else {
                ++zeroBlocksCount;
            }
        }
    }

    // determine the results kind
    TPartialBlobId dataBlobId, zeroBlobId;
    TBlockMask dataBlobSkipMask, zeroBlobSkipMask;

    if (dataBlocksCount) {
        ui32 skipped = 0;
        for (const auto& mark: args.BlockMarks) {
            if (!mark.BlockContent && IsDeletionMarker(mark.BlobId)) {
                ++skipped;
            }
        }

        dataBlobId = state.GenerateBlobId(
            EChannelDataKind::Merged,
            compactionPermissions,
            commitId,
            (args.BlockRange.Size() - skipped) * state.GetBlockSize(),
            result.size());
    }

    if (zeroBlocksCount) {
        // for zeroed region we will write blob without any data
        // XXX same commitId used for 2 blobs: data blob and zero blob
        // we differentiate between them by storing the last block index in
        // MergedBlocksIndex::RangeEnd not for the last block of the processed
        // compaction range but for the last actual block that's referenced by
        // the corresponding blob
        zeroBlobId = state.GenerateBlobId(
            EChannelDataKind::Merged,
            compactionPermissions,
            commitId,
            0,
            result.size());
    }

    // now build the blob content for all blocks to be written
    TBlockBuffer blobContent(TProfilingAllocator::Instance());
    TVector<ui32> blockChecksums;
    TVector<ui32> zeroBlocks;

    ui32 blockIndex = args.BlockRange.Start;
    TPartialBlobId patchingCandidate;
    ui32 patchingCandidateChangedBlockCount = 0;
    for (auto& mark: args.BlockMarks) {
        if (mark.CommitId) {
            if (mark.BlockContent) {
                Y_ABORT_UNLESS(IsDeletionMarker(mark.BlobId));
                requests.emplace_back(
                    mark.BlobId,
                    TActorId(),
                    mark.BlobOffset,
                    blockIndex,
                    blobContent.GetBlocksCount(),
                    0,
                    result.size());

                // fresh block will be written
                blobContent.AddBlock({
                    mark.BlockContent.data(),
                    mark.BlockContent.size()
                });

                if (args.ChecksumsEnabled) {
                    blockChecksums.push_back(
                        ComputeDefaultDigest(blobContent.GetBlocks().back()));
                }

                if (zeroBlobId) {
                    zeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                }
            } else if (!IsDeletionMarker(mark.BlobId)) {
                const auto proxy = tabletStorageInfo.BSProxyIDForChannel(
                    mark.BlobId.Channel(),
                    mark.BlobId.Generation());

                requests.emplace_back(
                    mark.BlobId,
                    proxy,
                    mark.BlobOffset,
                    blockIndex,
                    blobContent.GetBlocksCount(),
                    tabletStorageInfo.GroupFor(
                        mark.BlobId.Channel(),
                        mark.BlobId.Generation()),
                    result.size());

                // we will read this block later
                blobContent.AddBlock(state.GetBlockSize(), char(0));

                // block checksum is simply moved from the affected blob's meta
                if (args.ChecksumsEnabled) {
                    ui32 blockChecksum = 0;

                    auto* affectedBlob = args.AffectedBlobs.FindPtr(mark.BlobId);
                    Y_DEBUG_ABORT_UNLESS(affectedBlob);
                    if (affectedBlob) {
                        if (auto* meta = affectedBlob->BlobMeta.Get()) {
                            if (mark.BlobOffset < meta->BlockChecksumsSize()) {
                                blockChecksum =
                                    meta->GetBlockChecksums(mark.BlobOffset);
                            }
                        }
                    }

                    blockChecksums.push_back(blockChecksum);
                }

                if (zeroBlobId) {
                    zeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                }

                if (!patchingCandidate
                        && blobPatchingEnabled
                        && mark.BlobId.BlobSize() == dataBlobId.BlobSize())
                {
                    patchingCandidate = mark.BlobId;
                    ++patchingCandidateChangedBlockCount;
                } else if (patchingCandidate == mark.BlobId) {
                    ++patchingCandidateChangedBlockCount;
                }
            } else {
                dataBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                zeroBlocks.push_back(blockIndex);
            }
        } else {
            if (dataBlobId) {
                dataBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
            }
            if (zeroBlobId) {
                zeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
            }
        }

        ++blockIndex;
    }

    if (patchingCandidate) {
        TPartialBlobId targetBlobId(
            dataBlobId.Generation(),
            dataBlobId.Step(),
            patchingCandidate.Channel(),
            dataBlobId.BlobSize(),
            dataBlobId.Cookie(),
            0);

        TLogoBlobID realTargetBlobId = MakeBlobId(
            tabletStorageInfo.TabletID,
            targetBlobId);

        ui32 originalChannel = patchingCandidate.Channel();
        ui32 originalGroup = tabletStorageInfo.GroupFor(
            originalChannel,
            patchingCandidate.Generation());
        Y_ABORT_UNLESS(originalGroup != Max<ui32>());

        ui32 patchedChannel = realTargetBlobId.Channel();
        ui32 patchedGroup = tabletStorageInfo.GroupFor(
            patchedChannel,
            realTargetBlobId.Generation());
        Y_ABORT_UNLESS(patchedGroup != Max<ui32>());

        bool found = TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(
            MakeBlobId(tabletStorageInfo.TabletID, patchingCandidate),
            &realTargetBlobId,
            0xfe0000,
            originalGroup,
            patchedGroup);

        ui32 blockCount = patchingCandidate.BlobSize() / state.GetBlockSize();
        ui32 patchingBlockCount =
            dataBlocksCount - patchingCandidateChangedBlockCount;
        ui32 changedPercentage = 100 * patchingBlockCount / blockCount;

        if (found &&
            (!maxDiffPercentageForBlobPatching ||
            changedPercentage <= maxDiffPercentageForBlobPatching))
        {
            dataBlobId = TPartialBlobId(
                dataBlobId.Generation(),
                dataBlobId.Step(),
                patchingCandidate.Channel(),
                dataBlobId.BlobSize(),
                realTargetBlobId.Cookie(),
                0);
        } else {
            patchingCandidate = {};
        }
    }

    result.emplace_back(
        args.BlockRange,
        patchingCandidate,
        dataBlobId,
        dataBlobSkipMask,
        zeroBlobId,
        zeroBlobSkipMask,
        args.BlobsSkipped,
        args.BlocksSkipped,
        std::move(blockChecksums),
        std::move(blobContent),
        std::move(zeroBlocks),
        std::move(args.AffectedBlobs),
        std::move(args.AffectedBlocks));

    if (!dataBlobId && !zeroBlobId) {
        const auto rangeDescr = DescribeRange(args.BlockRange);
        Y_ABORT("No blocks in compacted range: %s", rangeDescr.c_str());
    }
    Y_ABORT_UNLESS(requests.size() - initialRequestsSize == dataBlocksCount);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompaction& args)
{
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    bool ready = true;

    THashSet<TPartialBlobId, TPartialBlobIdHash> affectedBlobIds;

    for (auto& rangeCompaction: args.RangeCompactions) {
        PrepareRangeCompaction(
            *Config,
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId(),
            args.CommitId,
            args.CompactionOptions.test(ToBit(ECompactionOption::Full)),
            ctx,
            TabletID(),
            affectedBlobIds,
            ready,
            db,
            *State,
            rangeCompaction);
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
    Y_UNUSED(args);
}

void TPartitionActor::CompleteCompaction(
    const TActorContext& ctx,
    TTxPartition::TCompaction& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    for (auto& rangeCompaction: args.RangeCompactions) {
        if (!rangeCompaction.Discarded) {
            State->RaiseRangeTemperature(rangeCompaction.RangeIdx);
        }
    }

    const bool blobPatchingEnabledForCloud =
        Config->IsBlobPatchingFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());
    const bool blobPatchingEnabled =
        Config->GetBlobPatchingEnabled() || blobPatchingEnabledForCloud;

    TVector<TRangeCompactionInfo> rangeCompactionInfos;
    TVector<TCompactionActor::TRequest> requests;

    for (auto& rangeCompaction: args.RangeCompactions) {
        if (rangeCompaction.Discarded) {
            continue;
        }

        CompleteRangeCompaction(
            blobPatchingEnabled,
            args.CommitId,
            *Info(),
            *State,
            rangeCompaction,
            requests,
            rangeCompactionInfos,
            Config->GetMaxDiffPercentageForBlobPatching());

        if (rangeCompactionInfos.back().OriginalBlobId) {
            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Selected patching candidate: %s, data blob: %s",
                TabletID(),
                ToString(rangeCompactionInfos.back().OriginalBlobId).c_str(),
                ToString(rangeCompactionInfos.back().DataBlobId).c_str());
        }
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
        TabletID(),
        SelfId(),
        State->GetBlockSize(),
        State->GetMaxBlocksInBlob(),
        Config->GetMaxAffectedBlocksPerCompaction(),
        BlockDigestGenerator,
        readBlobTimeout,
        compactionType,
        args.CommitId,
        std::move(rangeCompactionInfos),
        std::move(requests));

    Actors.Insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
