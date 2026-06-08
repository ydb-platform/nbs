#include "part_actor.h"

#include "part_compaction_logic.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

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

class TCompactionActor final
    : public TActorBootstrapped<TCompactionActor>
{
public:
    using TRequest = TBlobCompactionRequest;

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
    const TString DiskId;
    const TActorId Tablet;
    const ui32 BlockSize;
    const ui32 MaxBlocksInBlob;
    const ui32 MaxAffectedBlocksPerCompaction;
    const bool ForceChecksumsCalculation;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const TDuration BlobStorageAsyncRequestTimeout;
    const ECompactionType CompactionType;
    TChildLogTitle LogTitle;

    const ui64 CommitId;

    TVector<TRangeCompactionInfo> RangeCompactionInfos;
    TVector<TRequest> Requests;

    const bool SplitTxEnabled;
    TVector<TPartialBlobId> BlobsToReadBlockMasks;
    TVector<TPartialBlobId> BlobsToReadBlobMetas;

    THashMap<TPartialBlobId, size_t, TPartialBlobIdHash>
        BlockMaskResponseIndex;
    THashMap<TPartialBlobId, size_t, TPartialBlobIdHash>
        BlobMetaResponseIndex;

    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

    TVector<TBatchRequest> BatchRequests;
    size_t ReadRequestsCompleted = 0;
    size_t RealReadRequestsCompleted = 0;
    size_t WriteAndPatchBlobRequestsCompleted = 0;

    ui64 ReadExecCycles = 0;
    ui64 ReadWaitCycles = 0;
    ui64 MaxExecCyclesFromRead = 0;
    ui64 MaxExecCyclesFromWrite = 0;

    TInstant ReadBlobsStarted;
    TDuration ReadBlobsTime;

    TInstant WriteBlobsStarted;
    TDuration WriteBlobsTime;

    TInstant AddBlobsStarted;
    TDuration AddBlobsTime;

    TDuration CompactionTxTime;

    TVector<TCallContextPtr> ForkedReadCallContexts;
    TVector<TCallContextPtr> ForkedWriteAndPatchCallContexts;
    bool SafeToUseOrbit = true;

public:
    TCompactionActor(
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        TString diskId,
        const TActorId& tablet,
        ui32 blockSize,
        ui32 maxBlocksInBlob,
        ui32 maxAffectedBlocksPerCompaction,
        bool forceChecksumsCalculation,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TDuration blobStorageAsyncRequestTimeout,
        ECompactionType compactionType,
        ui64 commitId,
        TVector<TRangeCompactionInfo> rangeCompactionInfos,
        TVector<TRequest> requests,
        TDuration compactionTxTime,
        TChildLogTitle logTitle,
        bool splitTxEnabled,
        TVector<TPartialBlobId> blobsToReadBlockMasks,
        TVector<TPartialBlobId> blobsToReadBlobMetas);

    void Bootstrap(const TActorContext& ctx);

private:
    void InitBlockDigests();

    void ReadBlocks(const TActorContext& ctx);
    void WriteBlobs(const TActorContext& ctx);
    void AddBlobs(const TActorContext& ctx);
    void MakeDiffs(TRangeCompactionInfo& rc);

    void ContinueAfterBlobInfo(const TActorContext& ctx);

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
        const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePatchBlobResponse(
        const TEvPartitionCommonPrivate::TEvPatchBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCompactionReadBlobInfoResponse(
        const TEvPartitionPrivate::TEvCompactionReadBlobInfoResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCompactionActor::TCompactionActor(
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        TString diskId,
        const TActorId& tablet,
        ui32 blockSize,
        ui32 maxBlocksInBlob,
        ui32 maxAffectedBlocksPerCompaction,
        bool forceChecksumsCalculation,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TDuration blobStorageAsyncRequestTimeout,
        ECompactionType compactionType,
        ui64 commitId,
        TVector<TRangeCompactionInfo> rangeCompactionInfos,
        TVector<TRequest> requests,
        TDuration compactionTxTime,
        TChildLogTitle logTitle,
        bool splitTxEnabled,
        TVector<TPartialBlobId> blobsToReadBlockMasks,
        TVector<TPartialBlobId> blobsToReadBlobMetas)
    : RequestInfo(std::move(requestInfo))
    , TabletId(tabletId)
    , DiskId(std::move(diskId))
    , Tablet(tablet)
    , BlockSize(blockSize)
    , MaxBlocksInBlob(maxBlocksInBlob)
    , MaxAffectedBlocksPerCompaction(maxAffectedBlocksPerCompaction)
    , ForceChecksumsCalculation(forceChecksumsCalculation)
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , BlobStorageAsyncRequestTimeout(blobStorageAsyncRequestTimeout)
    , CompactionType(compactionType)
    , LogTitle(std::move(logTitle))
    , CommitId(commitId)
    , RangeCompactionInfos(std::move(rangeCompactionInfos))
    , Requests(std::move(requests))
    , SplitTxEnabled(splitTxEnabled)
    , BlobsToReadBlockMasks(std::move(blobsToReadBlockMasks))
    , BlobsToReadBlobMetas(std::move(blobsToReadBlobMetas))
    , CompactionTxTime(compactionTxTime)
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

    if (SplitTxEnabled &&
        (!BlobsToReadBlockMasks.empty() || !BlobsToReadBlobMetas.empty()))
    {
        size_t blockMaskResponseIndex = 0;
        for (const auto& blobId: BlobsToReadBlockMasks) {
            BlockMaskResponseIndex[blobId] = blockMaskResponseIndex;
            ++blockMaskResponseIndex;
        }

        size_t blobMetaResponseIndex = 0;
        for (const auto& blobId: BlobsToReadBlobMetas) {
            BlobMetaResponseIndex[blobId] = blobMetaResponseIndex;
            ++blobMetaResponseIndex;
        }

        auto request = std::make_unique<
            TEvPartitionPrivate::TEvCompactionReadBlobInfoRequest>(
            std::move(BlobsToReadBlockMasks),
            std::move(BlobsToReadBlobMetas));

        NCloud::Send(ctx, Tablet, std::move(request));
        return;
    }

    ContinueAfterBlobInfo(ctx);
}

void TCompactionActor::ContinueAfterBlobInfo(const TActorContext& ctx)
{
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

                const auto digest =
                    ForceChecksumsCalculation
                        ? BlockDigestGenerator->ComputeDigestForce(
                              sgList
                                  [blockIndex - rc.BlockRange.Start - skipped])
                        : BlockDigestGenerator->ComputeDigest(
                              blockIndex,
                              sgList
                                  [blockIndex - rc.BlockRange.Start - skipped]);

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

                const auto digest =
                    ForceChecksumsCalculation
                        ? BlockDigestGenerator->ComputeDigestForce(
                              TBlockDataRef::CreateZeroBlock(BlockSize))
                        : BlockDigestGenerator->ComputeDigest(
                              blockIndex,
                              TBlockDataRef::CreateZeroBlock(BlockSize));

                if (digest.Defined()) {
                    AffectedBlockInfos.push_back({blockIndex, *digest});
                }
            }
        }
    }
}

void TCompactionActor::ReadBlocks(const TActorContext& ctx)
{
    ReadBlobsStarted = ctx.Now();

    TVector<TRequest*> requests(Reserve(Requests.size()));
    for (auto& r: Requests) {
        requests.push_back(&r);
    }

    const auto makeTie = [](const TRequest* request)
    {
        return std::tie(
            request->BlobId,
            request->RangeCompactionIndex,
            request->BlobOffset);
    };

    Sort(
        requests,
        [makeTie](const TRequest* l, const TRequest* r)
        { return makeTie(l) < makeTie(r); });

    TBatchRequest current;
    ui32 currentRangeCompactionIndex = 0;
    for (auto* r: requests) {
        if (IsDeletionMarker(r->BlobId)) {
            ++ReadRequestsCompleted;
            continue;
        }

        if (std::tie(current.BlobId, currentRangeCompactionIndex) !=
            std::tie(r->BlobId, r->RangeCompactionIndex))
        {
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
            currentRangeCompactionIndex = r->RangeCompactionIndex;
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

    const auto readBlobDeadline =
        BlobStorageAsyncRequestTimeout
            ? ctx.Now() + BlobStorageAsyncRequestTimeout
            : TInstant::Max();

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
        request->CallContext->RequestId = RequestInfo->CallContext->RequestId;

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
    WriteBlobsStarted = ctx.Now();

    InitBlockDigests();

    const auto deadline = BlobStorageAsyncRequestTimeout
                              ? ctx.Now() + BlobStorageAsyncRequestTimeout
                              : TInstant::Max();

    for (auto& rc: RangeCompactionInfos) {
        if (!rc.DataBlobId) {
            ++WriteAndPatchBlobRequestsCompleted;
            continue;
        }

        if (rc.OriginalBlobId) {
            MakeDiffs(rc);

            auto request =
                std::make_unique<TEvPartitionCommonPrivate::TEvPatchBlobRequest>(
                    rc.OriginalBlobId,
                    rc.DataBlobId,
                    std::move(rc.Diffs),
                    rc.DiffCount,
                    true,        // async
                    deadline);   // deadline

            if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionCommonPrivate::TEvPatchBlobRequest",
                    RequestInfo->CallContext->RequestId);
            }

            ForkedWriteAndPatchCallContexts.emplace_back(request->CallContext);

            NCloud::Send(
                ctx,
                Tablet,
                std::move(request));
        } else {
            auto request =
                std::make_unique<TEvPartitionCommonPrivate::TEvWriteBlobRequest>(
                    rc.DataBlobId,
                    rc.BlobContent.GetGuardedSgList(),
                    0,           // blockSizeForChecksums
                    true,        // async
                    deadline);   // deadline

            if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionCommonPrivate::TEvWriteBlobRequest",
                    RequestInfo->CallContext->RequestId);
            }
            request->CallContext->RequestId =
                RequestInfo->CallContext->RequestId;

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
    AddBlobsStarted = ctx.Now();

    TVector<TAddMixedBlob> mixedBlobs;
    TVector<TAddMergedBlob> mergedBlobs;
    TVector<TBlobCompactionInfo> mixedBlobCompactionInfos;
    TVector<TBlobCompactionInfo> mergedBlobCompactionInfos;
    TAffectedBlobs affectedBlobs;
    TAffectedBlocks affectedBlocks;

    auto addBlob = [&] (
        const TPartialBlobId& blobId,
        TBlockRange32 range,
        TBlockMask skipMask,
        const TVector<ui32>& blockChecksums,
        ui32 blobsSkipped,
        ui32 blocksSkipped,
        EChannelDataKind channelDataKind)
    {
        while (skipMask.Get(range.End - range.Start)) {
            Y_ABORT_UNLESS(range.End > range.Start);
            // modifying skipMask is crucial since otherwise there would be
            // 2 blobs with the same key in merged index (the key is
            // commitId + blockRange.End)
            skipMask.Reset(range.End - range.Start);
            --range.End;
        }

        if (channelDataKind == EChannelDataKind::Merged) {
            mergedBlobs.emplace_back(blobId, range, skipMask, blockChecksums);
            mergedBlobCompactionInfos.push_back({blobsSkipped, blocksSkipped});
        } else if (channelDataKind == EChannelDataKind::Mixed) {
            TVector<ui32> blockIndices(Reserve(range.Size()));
            for (auto blockIndex = range.Start; blockIndex <= range.End;
                 ++blockIndex)
            {
                if (!skipMask.Get(blockIndex - range.Start)) {
                    blockIndices.emplace_back(blockIndex);
                }
            }
            mixedBlobs.emplace_back(
                blobId,
                std::move(blockIndices),
                blockChecksums,
                0);   // unknown blob alignment
            mixedBlobCompactionInfos.push_back({blobsSkipped, blocksSkipped});
        } else {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s unexpected channel data kind %u",
                LogTitle.GetWithTime().c_str(),
                static_cast<int>(channelDataKind));
        }
    };

    for (auto& rc: RangeCompactionInfos) {
        if (rc.DataBlobId) {
            addBlob(
                rc.DataBlobId,
                rc.BlockRange,
                rc.DataBlobSkipMask,
                rc.BlockChecksums,
                rc.BlobsSkippedByCompaction,
                rc.BlocksSkippedByCompaction,
                rc.ChannelDataKind);
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
                blocksSkipped,
                rc.ChannelDataKind);
        }

        if (rc.DataBlobId && rc.ZeroBlobId) {
            // if both blobs are present, none of them should contain all range
            // blocks
            Y_ABORT_UNLESS(rc.DataBlobSkipMask.Count());
            Y_ABORT_UNLESS(rc.ZeroBlobSkipMask.Count());
        }

        for (auto& [blobId, blob]: rc.AffectedBlobs) {
            if (auto* blockMask = blob.BlockMask.Get()) {
                // could already be full
                if (IsBlockMaskFull(*blockMask, MaxBlocksInBlob)) {
                    continue;
                }

                // mask overwritten blocks
                for (ui16 blobOffset: blob.Offsets) {
                    blockMask->Set(blobOffset);
                }
            } else {
                blob.BlockMask = GetFullBlockMask();
            }

            auto& blockMask = blob.BlockMask.GetRef();

            auto [blobIt, inserted] =
                affectedBlobs.try_emplace(blobId, std::move(blob));
            if (!inserted) {
                auto& affectedBlob = blobIt->second;
                affectedBlob.Offsets.insert(
                    affectedBlob.Offsets.end(),
                    blob.Offsets.begin(),
                    blob.Offsets.end());
                affectedBlob.AffectedBlocks.insert(
                    affectedBlob.AffectedBlocks.end(),
                    blob.AffectedBlocks.begin(),
                    blob.AffectedBlocks.end());
                if (affectedBlob.BlockMask) {
                    affectedBlob.BlockMask.GetRef() |= blockMask;
                }
                if (affectedBlob.BlobMeta && blob.BlobMeta) {
                    affectedBlob.BlobMeta->MergeFrom(blob.BlobMeta.GetRef());
                }
            }
        }

        Sort(rc.AffectedBlocks, [] (const auto& l, const auto& r) {
            // sort by (BlockIndex ASC, CommitId DESC)
            return (l.BlockIndex < r.BlockIndex)
                || (l.BlockIndex == r.BlockIndex && l.CommitId > r.CommitId);
        });

        if (rc.AffectedBlocks.size() > MaxAffectedBlocksPerCompaction) {
            // KIKIMR-6286: preventing heavy transactions
            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Cropping AffectedBlocks: %lu -> %lu, range: %s",
                LogTitle.GetWithTime().c_str(),
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
        std::move(mixedBlobs),
        std::move(mergedBlobs),
        TVector<TAddFreshBlob>(),
        ADD_COMPACTION_RESULT,
        std::move(affectedBlobs),
        std::move(affectedBlocks),
        std::move(mixedBlobCompactionInfos),
        std::move(mergedBlobCompactionInfos));

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

    request->ReadBlobsTime = ReadBlobsTime;
    request->WriteBlobsTime = WriteBlobsTime;
    request->AddBlobsTime = AddBlobsTime;
    request->CompactionTxTime = CompactionTxTime;

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
    auto& rc = *batch.RangeCompactionInfo;
    ApplyChecksumFixups(rc);

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
            expectedChecksum,
            DiskId);

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

    if (ReadBlobsStarted) {
        ReadBlobsTime = ctx.Now() - ReadBlobsStarted;
    }

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

    if (WriteBlobsStarted) {
        WriteBlobsTime = ctx.Now() - WriteBlobsStarted;
    }

    SafeToUseOrbit = true;

    for (auto context: ForkedWriteAndPatchCallContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    AddBlobs(ctx);
}

void TCompactionActor::HandleWriteBlobResponse(
    const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleWriteOrPatchBlobResponse(*ev, ctx);
}

void TCompactionActor::HandlePatchBlobResponse(
    const TEvPartitionCommonPrivate::TEvPatchBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleWriteOrPatchBlobResponse(*ev, ctx);
}

void TCompactionActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (AddBlobsStarted) {
        AddBlobsTime = ctx.Now() - AddBlobsStarted;
    }

    SafeToUseOrbit = true;

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>()
    );
}

void TCompactionActor::HandleCompactionReadBlobInfoResponse(
    const TEvPartitionPrivate::TEvCompactionReadBlobInfoResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    Y_ABORT_UNLESS(
        msg->BlockMasksForBlobs.size() == BlockMaskResponseIndex.size());
    Y_ABORT_UNLESS(
        msg->BlobMetasForBlobs.size() == BlobMetaResponseIndex.size());

    for (auto& rc: RangeCompactionInfos) {
        for (auto& [blobId, ab]: rc.AffectedBlobs) {
            if (auto it = BlockMaskResponseIndex.find(blobId);
                it != BlockMaskResponseIndex.end())
            {
                ab.BlockMask = msg->BlockMasksForBlobs[it->second];
            }

            if (auto it = BlobMetaResponseIndex.find(blobId);
                it != BlobMetaResponseIndex.end())
            {
                ab.BlobMeta = msg->BlobMetasForBlobs[it->second];
            }
        }
    }

    ContinueAfterBlobInfo(ctx);
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
        HFunc(
            TEvPartitionCommonPrivate::TEvWriteBlobResponse,
            HandleWriteBlobResponse);
        HFunc(TEvPartitionCommonPrivate::TEvPatchBlobResponse, HandlePatchBlobResponse);
        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);
        HFunc(
            TEvPartitionPrivate::TEvCompactionReadBlobInfoResponse,
            HandleCompactionReadBlobInfoResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

ui32 GetPercentage(ui64 total, ui64 real)
{
    const double p = (real - total) * 100. / Max(total, 1UL);
    const double MAX_P = 1'000;
    return Min(p, MAX_P);
}

////////////////////////////////////////////////////////////////////////////////

class TCompactionTriggerer
{
private:
    const TStorageConfigPtr Config;
    TPartitionState& State;

    TRangeStat TopRangeStat;
    TRangeStat TopGarbageRangeStat;

public:
    enum class ECompactionTriggerKind
    {
        ByBlobCountPerDisk,
        ByBlobCountPerRange,
        ByReadStats,
        ByGarbageBlocksPerDisk,
        ByGarbageBlocksPerRange
    };

    struct TTriggerInfo
    {
        ui64 PerRangeCount; // Blobs count or garbage blocks count.
        ui64 PerRangeThreshold;
        ui64 PerDiskCount; // Blobs count or garbage blocks count.
        ui64 PerDiskThreshold;
        TEvPartitionPrivate::ECompactionMode Mode;
        ECompactionTriggerKind TriggerKind;
        bool ThrottlingAllowed;
        bool FullCompaction;

        TTriggerInfo(
            ui64 perRangeCount,
            ui64 perRangeThreshold,
            ui64 perDiskCount,
            ui64 perDiskThreshold,
            TEvPartitionPrivate::ECompactionMode mode,
            ECompactionTriggerKind triggerKind,
            bool throttlingAllowed,
            bool fullCompaction)
            : PerRangeCount(perRangeCount)
            , PerRangeThreshold(perRangeThreshold)
            , PerDiskCount(perDiskCount)
            , PerDiskThreshold(perDiskThreshold)
            , Mode(mode)
            , TriggerKind(triggerKind)
            , ThrottlingAllowed(throttlingAllowed)
            , FullCompaction(fullCompaction)
        {}
    };

public:
    TCompactionTriggerer(
        const TStorageConfigPtr config,
        TPartitionState& state,
        TInstant now)
        : Config(config)
        , State(state)
    {
        const auto& cm = State.GetCompactionMap();
        TopRangeStat = cm.GetTop().Stat;
        TopGarbageRangeStat = cm.GetTopByGarbageBlockCount().Stat;

        auto& scoreHistory = State.GetCompactionScoreHistory();
        if (scoreHistory.LastTs() + Config->GetMaxCompactionDelay() <= now) {
            scoreHistory.Register({
                now,
                {
                    TopRangeStat.CompactionScore.Score,
                    TopGarbageRangeStat.GarbageBlockCount(),
                },
            });
        }
    }

    [[nodiscard]] std::optional<TTriggerInfo> TriggerCompactionIfNeeded() const
    {
        std::optional<TTriggerInfo> info;

        info = TriggerRangeCompactionIfNeeded();
        if (!info) {
            info = TriggerGarbageCompactionIfNeeded();
        }

        return info;
    }

private:
    [[nodiscard]] ui64 GetBlockCount() const
    {
        return State.GetMixedBlocksCount() + State.GetMergedBlocksCount() -
               State.GetCleanupQueue().GetQueueBlocks();
    }

    [[nodiscard]] ui64 GetGarbagePercentage() const
    {
        return GetPercentage(State.GetUsedBlocksCount(), GetBlockCount());
    }

    [[nodiscard]] std::optional<TTriggerInfo>
    TriggerRangeCompactionIfNeeded() const
    {
        const auto blobCount =
            State.GetMixedBlobsCount() + State.GetMergedBlobsCount();
        const bool diskBlobCountOverThreshold =
            State.GetMaxBlobsPerDisk() &&
            blobCount >
                State.GetMaxBlobsPerDisk() + State.GetCleanupQueue().GetCount();

        if (TopRangeStat.CompactionScore.Score <= 0 &&
            !diskBlobCountOverThreshold)
        {
            return std::nullopt;
        }

        ECompactionTriggerKind triggerKind =
            ECompactionTriggerKind::ByBlobCountPerDisk;

        if (TopRangeStat.CompactionScore.Score > 0) {
            switch (TopRangeStat.CompactionScore.Type) {
                case TCompactionScore::EType::BlobCount: {
                    triggerKind = ECompactionTriggerKind::ByBlobCountPerRange;
                    break;
                }
                case TCompactionScore::EType::Read: {
                    triggerKind = ECompactionTriggerKind::ByReadStats;
                    break;
                }
            }
        }

        bool throttlingAllowed = TopRangeStat.CompactionScore.Score <
                                 Config->GetCompactionScoreLimitForThrottling();

        // Compaction by blob count takes priority over compaction by garbage.
        // As a result, it may happen that we constantly compact by blob count
        // while garbage accumulates on disk. To avoid that, we fall back to
        // full compaction when the garbage level is also high.
        bool fullCompaction =
            GetGarbagePercentage() >= Config->GetCompactionGarbageThreshold();

        return TTriggerInfo(
            TopRangeStat.BlobCount,
            State.GetMaxBlobsPerRange(),
            blobCount,
            State.GetMaxBlobsPerDisk(),
            TEvPartitionPrivate::RangeCompaction,
            triggerKind,
            throttlingAllowed,
            fullCompaction);
    }

    [[nodiscard]] std::optional<TTriggerInfo>
    TriggerGarbageCompactionIfNeeded() const
    {
        if (!Config->GetV1GarbageCompactionEnabled()) {
            return std::nullopt;
        }

        if (!State.GetCheckpoints().IsEmpty()) {
            // Should not compact. Compaction produces more garbage, but garbage
            // is not collected while a checkpoint exists, and we don't want the
            // disk to accumulate too much garbage.
            return std::nullopt;
        }

        {
            // Nothing to compact if there are no blobs in the range.
            // Nothing to compact if there is only one blob in the range and it
            // is not zeroed.
            const auto isZeroedRange = TopGarbageRangeStat.BlockCount &&
                                       !TopGarbageRangeStat.UsedBlockCount;

            if (TopGarbageRangeStat.Compacted ||
                TopGarbageRangeStat.BlobCount < 2 && !isZeroedRange)
            {
                return std::nullopt;
            }
        }

        ui64 diskGarbage = GetGarbagePercentage();
        ui64 rangeGarbage = GetPercentage(
            TopGarbageRangeStat.UsedBlockCount,
            TopGarbageRangeStat.BlockCount);

        const bool diskGarbageBelowThreshold =
            diskGarbage < Config->GetCompactionGarbageThreshold();

        ECompactionTriggerKind triggerKind =
            ECompactionTriggerKind::ByGarbageBlocksPerRange;

        if (rangeGarbage < Config->GetCompactionRangeGarbageThreshold()) {
            // Not enough garbage in this range.
            if (diskGarbageBelowThreshold) {
                // And not enough garbage on the whole disk, no need to compact.
                return std::nullopt;
            }

            if (rangeGarbage < Config->GetCompactionGarbageThreshold()) {
                // Really not enough garbage in this range.
                return std::nullopt;
            }

            triggerKind = ECompactionTriggerKind::ByGarbageBlocksPerDisk;
        }

        return TTriggerInfo(
            rangeGarbage,
            Config->GetCompactionRangeGarbageThreshold(),
            diskGarbage,
            Config->GetCompactionGarbageThreshold(),
            TEvPartitionPrivate::GarbageCompaction,
            triggerKind,
            true /* throttlingAllowed */,
            true /* fullCompaction */);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void IncrementCompactionCounterByTriggerKind(
    TPartitionDiskCountersPtr& partCounters,
    TCompactionTriggerer::ECompactionTriggerKind triggerKind)
{
    switch (triggerKind) {
        case TCompactionTriggerer::ECompactionTriggerKind::ByBlobCountPerDisk:
            partCounters->Cumulative.CompactionByBlobCountPerDisk.Increment(1);
            break;
        case TCompactionTriggerer::ECompactionTriggerKind::ByBlobCountPerRange:
            partCounters->Cumulative.CompactionByBlobCountPerRange.Increment(1);
            break;
        case TCompactionTriggerer::ECompactionTriggerKind::ByReadStats:
            partCounters->Cumulative.CompactionByReadStats.Increment(1);
            break;
        case TCompactionTriggerer::ECompactionTriggerKind::
            ByGarbageBlocksPerDisk:
            partCounters->Cumulative.CompactionByGarbageBlocksPerDisk.Increment(
                1);
            break;
        case TCompactionTriggerer::ECompactionTriggerKind::
            ByGarbageBlocksPerRange:
            partCounters->Cumulative.CompactionByGarbageBlocksPerRange
                .Increment(1);
            break;
    }
}

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

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueCompactionIfNeeded(const TActorContext& ctx)
{
    if (CompactionMapLoadState) {
        return;
    }

    if (State->GetCompactionState(ECompactionType::Tablet).Status !=
        EOperationStatus::Idle)
    {
        // already enqueued
        return;
    }

    if (!State->IsCompactionAllowed()) {
        return;
    }

    auto now = ctx.Now();
    TCompactionTriggerer triggerer(Config, *State, now);

    auto info = triggerer.TriggerCompactionIfNeeded();
    if (!info) {
        // No need to compact.
        return;
    }

    IncrementCompactionCounterByTriggerKind(PartCounters, info->TriggerKind);

    State->GetCompactionState(ECompactionType::Tablet)
        .SetStatus(EOperationStatus::Enqueued, ctx.Now());

    if (Config->GetCompactionCountPerRunIncreasingThreshold() &&
        Config->GetCompactionCountPerRunDecreasingThreshold() &&
        now - State->GetLastCompactionRangeCountPerRunTime() >
            Config->GetCompactionCountPerRunChangingPeriod())
    {
        ChangeRangeCountPerRunIfNeeded(
            info->PerRangeCount,
            info->PerRangeThreshold,
            info->PerDiskCount,
            info->PerDiskThreshold,
            ctx);
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()),
        info->Mode);

    if (info->FullCompaction) {
        request->CompactionOptions.set(ToBit(ECompactionOption::Full));
    }

    if (info->ThrottlingAllowed && Config->GetMaxCompactionDelay()) {
        auto execTime = State->GetCompactionExecTimeForLastSecond(ctx.Now());
        auto delay = Config->GetMinCompactionDelay();
        if (Config->GetMaxCompactionExecTimePerSecond()) {
            auto throttlingFactor =
                double(execTime.GetValue()) /
                Config->GetMaxCompactionExecTimePerSecond().GetValue();
            const auto throttleDelay =
                (TDuration::Seconds(1) - execTime) * throttlingFactor;

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
        NCloud::Send(ctx, SelfId(), std::move(request));
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
            EOperationStatus::Idle, ctx.Now());

        replyError(ctx, *requestInfo, E_BS_OUT_OF_SPACE, "all channels readonly");
        return;
    }

    TVector<TCompactionCounter> tops;

    const bool batchCompactionEnabledForCloud =
        Config->IsBatchCompactionFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());
    const bool batchCompactionEnabled =
        Config->GetBatchCompactionEnabled() || batchCompactionEnabledForCloud;

    const auto& cm = State->GetCompactionMap();

    if (!msg->RangeBlockIndices.empty()) {
        for (const auto blockIndex: msg->RangeBlockIndices) {
            const auto startIndex = cm.GetRangeStart(blockIndex);
            auto range = cm.Get(startIndex);
            if (range.BlobCount > 0) {
                tops.emplace_back(startIndex, std::move(range));
            }
        }
        State->OnNewCompactionRange(msg->RangeBlockIndices.size());
    } else if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
        if (batchCompactionEnabled &&
            Config->GetGarbageCompactionRangeCountPerRun() > 1)
        {
            tops = cm.GetTopByGarbageBlockCount(
                Config->GetGarbageCompactionRangeCountPerRun());
        } else {
            const auto& top = cm.GetTopByGarbageBlockCount();
            tops.push_back({top.BlockIndex, top.Stat});
        }
    } else {
        tops = cm.GetTopsFromGroups(
            batchCompactionEnabled ? State->GetCompactionRangeCountPerRun()
                                   : 1);
    }

    if (tops.empty() || !tops.front().Stat.BlobCount) {
        State->GetCompactionState(compactionType)
            .SetStatus(EOperationStatus::Idle, ctx.Now());

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

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Start %s compaction @%lu (range: %s, blobs: %u, blocks: %u"
            ", reads: %u, blobsread: %u, blocksread: %u, score: %f)",
            LogTitle.GetWithTime().c_str(),
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

    State->GetCompactionState(compactionType)
        .SetStatus(EOperationStatus::Started, ctx.Now());

    State->AccessCommitQueue()->AcquireBarrier(commitId);
    State->GetCleanupQueue().AcquireBarrier(commitId);
    State->GetGarbageQueue().AcquireBarrier(commitId);

    AddTransaction<TEvPartitionPrivate::TCompactionMethod>(*requestInfo);

    auto tx = CreateTx<TCompaction>(
        requestInfo,
        commitId,
        msg->CompactionOptions,
        std::move(ranges),
        ctx.Now());

    SharedState->WaitCommitForCompaction(ctx, std::move(tx), commitId);
}

void TPartitionActor::ProcessCommitQueue(const TActorContext& ctx)
{
    SharedState->ProcessCommitQueue(ctx);
}

void TPartitionActor::HandleCompactionCompleted(
    const TEvPartitionPrivate::TEvCompactionCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ui64 commitId = msg->CommitId;
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete compaction @%lu",
        LogTitle.GetWithTime().c_str(),
        commitId);

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Compaction @%lu failed: %s",
            LogTitle.GetWithTime().c_str(),
            commitId,
            FormatError(msg->GetError()).c_str());
    }

    UpdateStats(msg->Stats);

    State->AccessCommitQueue()->ReleaseBarrier(commitId);
    State->GetCleanupQueue().ReleaseBarrier(commitId);
    State->GetGarbageQueue().ReleaseBarrier(commitId);

    const auto compactionStartedTs =
        State->GetCompactionState(msg->CompactionType).Timestamp;
    State->GetCompactionState(msg->CompactionType)
        .SetStatus(EOperationStatus::Idle, ctx.Now());

    Actors.Erase(ev->Sender);

    const auto d = CyclesToDurationSafe(msg->TotalCycles);
    ui32 blocks = msg->Stats.GetSysReadCounters().GetBlocksCount()
        + msg->Stats.GetSysWriteCounters().GetBlocksCount();
    PartCounters->RequestCounters.Compaction.AddRequest(
        d.MicroSeconds(),
        blocks * State->GetBlockSize());

    PartCounters->Cumulative.CompactionReadBlobsTime.Increment(
        msg->ReadBlobsTime.MicroSeconds());
    PartCounters->Cumulative.CompactionWriteBlobsTime.Increment(
        msg->WriteBlobsTime.MicroSeconds());
    PartCounters->Cumulative.CompactionAddBlobsTime.Increment(
        msg->AddBlobsTime.MicroSeconds());
    PartCounters->Cumulative.CompactionTxTime.Increment(
        msg->CompactionTxTime.MicroSeconds());

    PartCounters->Cumulative.CompactionExecutionTime.Increment(
        CyclesToDurationSafe(msg->ExecCycles).MicroSeconds());

    if (compactionStartedTs.MicroSeconds()) {
        const auto totalTime = ctx.Now() - compactionStartedTs;
        PartCounters->Cumulative.CompactionTotalTime.Increment(
            totalTime.MicroSeconds());
    }
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

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCompaction& args)
{
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    const bool incrementalCompactionEnabled =
        Config->GetIncrementalCompactionEnabled() ||
        Config->IsIncrementalCompactionFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());
    const bool fullCompaction =
        args.CompactionOptions.test(ToBit(ECompactionOption::Full));
    const ui32 maxSkippedBlobs =
        (!incrementalCompactionEnabled || fullCompaction) ? 0
        : PartitionConfig.GetStorageMediaKind() ==
                NCloud::NProto::STORAGE_MEDIA_SSD
            ? Config->GetMaxSkippedBlobsDuringCompaction()
            : Config->GetMaxSkippedBlobsDuringCompactionHDD();

    bool ready = true;

    for (auto& rangeCompaction: args.RangeCompactions) {
        PrepareRangeCompaction(
            *Config,
            maxSkippedBlobs,
            args.CommitId,
            TabletID(),
            IsReadBlockMaskOnCompactionOptimizationEnabled(),
            ready,
            db,
            *State,
            rangeCompaction,
            args.BlobsToReadBlockMasks,
            args.BlobsToReadBlobMetas);

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Dropping last %u blobs, %u blocks, remaining blobs: %u",
            LogTitle.GetWithTime().c_str(),
            rangeCompaction.BlobsSkipped,
            rangeCompaction.BlocksSkipped,
            rangeCompaction.AffectedBlobs.size());
    }

    if (SplitCompactionTxEnabled) {
        // BlockMask / BlobMeta are read in a separate TX issued by
        // TCompactionActor.
        return ready;
    }

    for (const auto& blobId: args.BlobsToReadBlockMasks) {
        TMaybe<TBlockMask> blockMask;
        if (db.ReadBlockMask(blobId, blockMask)) {
            STORAGE_VERIFY_C(
                blockMask.Defined(),
                TWellKnownEntityTypes::TABLET,
                TabletID(),
                TStringBuilder() << "Could not read block mask for blob: "
                                 << MakeBlobId(TabletID(), blobId));
        } else {
            ready = false;
            continue;
        }

        for (auto& rangeCompaction: args.RangeCompactions) {
            if (auto* ab = rangeCompaction.AffectedBlobs.FindPtr(blobId)) {
                ab->BlockMask = *blockMask;
            }
        }
    }

    for (const auto& blobId: args.BlobsToReadBlobMetas) {
        TMaybe<NProto::TBlobMeta> blobMeta;
        if (db.ReadBlobMeta(blobId, blobMeta)) {
            STORAGE_VERIFY_C(
                blobMeta.Defined(),
                TWellKnownEntityTypes::TABLET,
                TabletID(),
                TStringBuilder() << "Could not read blob meta for blob: "
                                 << MakeBlobId(TabletID(), blobId));
        } else {
            ready = false;
            continue;
        }

        for (auto& rangeCompaction: args.RangeCompactions) {
            if (auto* ab = rangeCompaction.AffectedBlobs.FindPtr(blobId)) {
                ab->BlobMeta = *blobMeta;
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
    Y_UNUSED(args);
}

void TPartitionActor::CompleteCompaction(
    const TActorContext& ctx,
    TTxPartition::TCompaction& args)
{
    TRequestScope timer(*args.RequestInfo);

    const auto compactionTxTime = ctx.Now() - args.TxStarted;

    RemoveTransaction(*args.RequestInfo);

    for (auto& rangeCompaction: args.RangeCompactions) {
        State->RaiseRangeTemperature(rangeCompaction.RangeIdx);
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

    const auto mergedBlobThreshold =
        PartitionConfig.GetStorageMediaKind() ==
                NCloud::NProto::STORAGE_MEDIA_SSD
            ? 0
            : Config->GetCompactionMergedBlobThresholdHDD();
    for (auto& rangeCompaction: args.RangeCompactions) {
        CompleteRangeCompaction(
            blobPatchingEnabled,
            mergedBlobThreshold,
            args.CommitId,
            *Info(),
            *State,
            rangeCompaction,
            requests,
            rangeCompactionInfos,
            Config->GetMaxDiffPercentageForBlobPatching());

        if (rangeCompactionInfos.back().OriginalBlobId) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Selected patching candidate: %s, data blob: %s",
                LogTitle.GetWithTime().c_str(),
                ToString(rangeCompactionInfos.back().OriginalBlobId).c_str(),
                ToString(rangeCompactionInfos.back().DataBlobId).c_str());
        }
    }

    const auto compactionType =
        args.CompactionOptions.test(ToBit(ECompactionOption::Forced))
            ? ECompactionType::Forced
            : ECompactionType::Tablet;

    auto actor = NCloud::Register<TCompactionActor>(
        ctx,
        args.RequestInfo,
        TabletID(),
        PartitionConfig.GetDiskId(),
        SelfId(),
        State->GetBlockSize(),
        State->GetMaxBlocksInBlob(),
        Config->GetMaxAffectedBlocksPerCompaction(),
        Config->GetComputeDigestForEveryBlockOnCompaction(),
        BlockDigestGenerator,
        GetBlobStorageAsyncRequestTimeout(),
        compactionType,
        args.CommitId,
        std::move(rangeCompactionInfos),
        std::move(requests),
        compactionTxTime,
        LogTitle.GetChild(GetCycleCount()),
        SplitCompactionTxEnabled,
        TVector<TPartialBlobId>(
            args.BlobsToReadBlockMasks.begin(),
            args.BlobsToReadBlockMasks.end()),
        TVector<TPartialBlobId>(
            args.BlobsToReadBlobMetas.begin(),
            args.BlobsToReadBlobMetas.end()));
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Partition registered TCompactionActor with id [%lu]",
        LogTitle.GetWithTime().c_str(),
        actor.ToString().c_str());

    Actors.Insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
