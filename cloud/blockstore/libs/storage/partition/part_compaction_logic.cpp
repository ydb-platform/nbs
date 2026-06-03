#include "part_compaction_logic.h"

#include "part_database.h"
#include "part_state.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/model/channel_permissions.h>
#include <cloud/blockstore/libs/storage/partition/model/block.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>

#include <cloud/storage/core/libs/common/alloc.h>
#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TBlockMask GetFullBlockMask()
{
    TBlockMask mask;
    mask.Set(0, MaxBlocksCount);
    return mask;
}

TRangeCompactionInfo::TRangeCompactionInfo(
        TBlockRange32 blockRange,
        TPartialBlobId originalBlobId,
        TPartialBlobId dataBlobId,
        TBlockMask dataBlobSkipMask,
        TPartialBlobId zeroBlobId,
        TBlockMask zeroBlobSkipMask,
        ui32 blobsSkippedByCompaction,
        ui32 blocksSkippedByCompaction,
        TVector<ui32> blockChecksums,
        EChannelDataKind channelDataKind,
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
    , ChannelDataKind(channelDataKind)
    , BlobContent(std::move(blobContent))
    , ZeroBlocks(std::move(zeroBlocks))
    , AffectedBlobs(std::move(affectedBlobs))
    , AffectedBlocks(std::move(affectedBlocks))
{}

TBlobCompactionRequest::TBlobCompactionRequest(
        const TPartialBlobId& blobId,
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

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompactionBlockVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
    , public IMixedBlocksIndexVisitor
    , public IBlobsVisitor
{
private:
    TTxPartition::TRangeCompaction& Args;
    const ui64 MaxCommitId;
    const TCompactionMap& CompactionMap;
    const ui64 TabletId;

public:
    TCompactionBlockVisitor(
            TTxPartition::TRangeCompaction& args,
            ui64 maxCommitId,
            const TCompactionMap& compactionMap,
            ui64 tabletId)
        : Args(args)
        , MaxCommitId(maxCommitId)
        , CompactionMap(compactionMap)
        , TabletId(tabletId)
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

    bool VisitBlock(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset,
        ui8 compactionRangeCount) override
    {
        auto& ab = Args.AffectedBlobs[blobId];

        ab.MaxCommitIdInCompactionRange =
            std::max(commitId, ab.MaxCommitIdInCompactionRange);
        ab.MinCommitIdInCompactionRange =
            std::min(commitId, ab.MinCommitIdInCompactionRange);

        if (commitId > MaxCommitId) {
            return true;
        }

        STORAGE_VERIFY_C(
            ab.CompactionRangeCount == 0 ||
                ab.CompactionRangeCount == compactionRangeCount,
            TWellKnownEntityTypes::TABLET,
            TabletId,
            TStringBuilder()
                << "Compaction range count mismatch, BlobId: " << blobId
                << ", expected: " << ab.CompactionRangeCount
                << ", actual: " << compactionRangeCount);

        ab.CompactionRangeCount = compactionRangeCount;

        Args.MarkBlock(
            blockIndex,
            commitId,
            blobId,
            blobOffset,
            KeepTrackOfAffectedBlocks);
        return true;
    }

    bool Visit(TBlockRange32 blockRange, const TPartialBlobId& blobId) override
    {
        auto& ab = Args.AffectedBlobs[blobId];
        ab.MaxCommitIdInCompactionRange = blobId.CommitId();
        ab.MinCommitIdInCompactionRange = blobId.CommitId();
        ab.CompactionRangeCount =
            CompactionMap.GetRangeIndex(blockRange.End) -
            CompactionMap.GetRangeIndex(blockRange.Start) + 1;
        return true;
    }

    void Finish()
    {
        TVector<TPartialBlobId> blobIdsToDelete;

        for (const auto& [blobId, ab]: Args.AffectedBlobs) {
            if (ab.MinCommitIdInCompactionRange > MaxCommitId) {
                blobIdsToDelete.push_back(blobId);
            }
        }

        for (const auto& blobId: blobIdsToDelete) {
            Args.AffectedBlobs.erase(blobId);
        }
    }
};

struct TCompactionBlockCounts {
    size_t DataBlocksCount = 0;
    size_t ZeroBlocksCount = 0;
};

TCompactionBlockCounts CountDataAndZeroBlocks(
    const TVector<TTxPartition::TRangeCompaction::TBlockMark>& blockMarks)
{
    TCompactionBlockCounts counts;

    for (const auto& mark: blockMarks) {
        if (mark.CommitId) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            // there could be fresh block OR merged/mixed block
            Y_ABORT_UNLESS(!(isFresh && isMixedOrMerged));
            if (isFresh || isMixedOrMerged) {
                ++counts.DataBlocksCount;
            } else {
                ++counts.ZeroBlocksCount;
            }
        }
    }

    return counts;
}

struct TCompactionResultBlobIds {
    TPartialBlobId DataBlobId;
    TPartialBlobId ZeroBlobId;
    EChannelDataKind ChannelDataKind = EChannelDataKind::Merged;
};

TCompactionResultBlobIds DetermineCompactionResultBlobIds(
    size_t dataBlocksCount,
    size_t zeroBlocksCount,
    ui32 mergedBlobThreshold,
    ui64 commitId,
    EChannelPermissions compactionPermissions,
    TPartitionState& state,
    const TTxPartition::TRangeCompaction& args,
    size_t rangeCompactionInfosSize)
{
    TCompactionResultBlobIds result;

    if (dataBlocksCount) {
        ui32 skipped = 0;
        for (const auto& mark: args.BlockMarks) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            if (!isFresh && !isMixedOrMerged) {
                ++skipped;
            }
        }

        const auto blobSize =
            (args.BlockRange.Size() - skipped) * state.GetBlockSize();
        if (blobSize < mergedBlobThreshold) {
            result.ChannelDataKind = EChannelDataKind::Mixed;
        }
        result.DataBlobId = state.GenerateBlobId(
            result.ChannelDataKind,
            compactionPermissions,
            commitId,
            blobSize,
            rangeCompactionInfosSize);
    }

    if (zeroBlocksCount) {
        // for zeroed region we will write blob without any data
        // XXX same commitId used for 2 blobs: data blob and zero blob
        // we differentiate between them by storing the last block index in
        // MergedBlocksIndex::RangeEnd not for the last block of the processed
        // compaction range but for the last actual block that's referenced by
        // the corresponding blob
        result.ZeroBlobId = state.GenerateBlobId(
            result.ChannelDataKind,
            compactionPermissions,
            commitId,
            0,
            rangeCompactionInfosSize);
    }

    return result;
}

void ApplyIncrementalCompactionSkipping(
    const TStorageConfig& config,
    ui32 maxSkippedBlobs,
    const TActorContext& ctx,
    const TString& logTitle,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args)
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
        blobIds,
        [&](const TPartialBlobId& l, const TPartialBlobId& r)
        { return liveBlocks[l] < liveBlocks[r]; });

    auto it = blobIds.begin();
    args.BlobsSkipped = blobIds.size();
    ui32 blocks = 0;

    while (it != blobIds.end()) {
        const auto bytes = blocks * state.GetBlockSize();
        const auto blobCountOk = args.BlobsSkipped <= maxSkippedBlobs;
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

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Dropping last %u blobs, %u blocks, remaining blobs: %u, "
        "blocks: %u",
        logTitle.c_str(),
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

void ReadAffectedBlobsForCompaction(
    ui64 commitId,
    ui64 tabletId,
    bool readBlockMaskOnCompactionOptimizationEnabled,
    bool& ready,
    TPartitionDatabase& db,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args)
{
    for (auto& kv: args.AffectedBlobs) {
        state.IncrementBlobsProcessedDuringCompaction();

        const bool blobOnlyInOneCompactRange =
            kv.second.CompactionRangeCount == 1;

        const bool blobFullyAvailableForRangeCompaction =
            kv.second.MaxCommitIdInCompactionRange <= commitId &&
            blobOnlyInOneCompactRange;

        if (!blobFullyAvailableForRangeCompaction ||
            !readBlockMaskOnCompactionOptimizationEnabled)
        {
            state.IncrementBlockMaskReadDuringCompaction();

            if (db.ReadBlockMask(kv.first, kv.second.BlockMask)) {
                STORAGE_VERIFY_C(
                    kv.second.BlockMask.Defined(),
                    TWellKnownEntityTypes::TABLET,
                    tabletId,
                    TStringBuilder() << "Could not read block mask for blob: "
                                     << MakeBlobId(tabletId, kv.first));
            } else {
                ready = false;
            }
        } else if (state.GetCleanupQueue().HasBlob(kv.first)) {
            // If the blob is in the cleanup queue, we should not try to add
            // this blob to cleanup queue again.
            kv.second.BlockMask = GetFullBlockMask();
        }

        if (args.ChecksumsEnabled) {
            if (db.ReadBlobMeta(kv.first, kv.second.BlobMeta)) {
                STORAGE_VERIFY_C(
                    kv.second.BlobMeta.Defined(),
                    TWellKnownEntityTypes::TABLET,
                    tabletId,
                    TStringBuilder() << "Could not read blob meta for blob: "
                                     << MakeBlobId(tabletId, kv.first));
            } else {
                ready = false;
            }
        }
    }
}

struct TProcessBlockMarksResult {
    TBlockBuffer BlobContent;
    TVector<ui32> BlockChecksums;
    TVector<ui32> ZeroBlocks;
    TBlockMask DataBlobSkipMask;
    TBlockMask ZeroBlobSkipMask;
    TPartialBlobId PatchingCandidate;
    ui32 PatchingCandidateChangedBlockCount = 0;
};

TProcessBlockMarksResult ProcessBlockMarksForCompaction(
    bool blobPatchingEnabled,
    const TPartialBlobId& dataBlobId,
    const TPartialBlobId& zeroBlobId,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    TVector<TBlobCompactionRequest>& requests,
    size_t rangeCompactionInfosSize)
{
    TProcessBlockMarksResult result;
    result.BlobContent = TBlockBuffer(TProfilingAllocator::Instance());

    ui32 blockIndex = args.BlockRange.Start;
    for (auto& mark: args.BlockMarks) {
        if (mark.CommitId) {
            if (mark.BlockContent) {
                Y_ABORT_UNLESS(IsDeletionMarker(mark.BlobId));
                requests.emplace_back(
                    mark.BlobId,
                    TActorId(),
                    mark.BlobOffset,
                    blockIndex,
                    result.BlobContent.GetBlocksCount(),
                    0,
                    rangeCompactionInfosSize);

                // fresh block will be written
                result.BlobContent.AddBlock({
                    mark.BlockContent.data(),
                    mark.BlockContent.size()
                });

                if (args.ChecksumsEnabled) {
                    result.BlockChecksums.push_back(
                        ComputeDefaultDigest(
                            result.BlobContent.GetBlocks().back()));
                }

                if (zeroBlobId) {
                    result.ZeroBlobSkipMask.Set(
                        blockIndex - args.BlockRange.Start);
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
                    result.BlobContent.GetBlocksCount(),
                    tabletStorageInfo.GroupFor(
                        mark.BlobId.Channel(),
                        mark.BlobId.Generation()),
                    rangeCompactionInfosSize);

                // we will read this block later
                result.BlobContent.AddBlock(state.GetBlockSize(), char(0));

                // block checksum is simply moved from the affected blob's meta
                if (args.ChecksumsEnabled) {
                    ui32 blockChecksum = 0;

                    auto* affectedBlob =
                        args.AffectedBlobs.FindPtr(mark.BlobId);
                    Y_DEBUG_ABORT_UNLESS(affectedBlob);
                    if (affectedBlob) {
                        if (auto* meta = affectedBlob->BlobMeta.Get()) {
                            if (mark.BlobOffset < meta->BlockChecksumsSize()) {
                                blockChecksum =
                                    meta->GetBlockChecksums(mark.BlobOffset);
                            }
                        }
                    }

                    result.BlockChecksums.push_back(blockChecksum);
                }

                if (zeroBlobId) {
                    result.ZeroBlobSkipMask.Set(
                        blockIndex - args.BlockRange.Start);
                }

                if (blobPatchingEnabled) {
                    if (!result.PatchingCandidate &&
                        mark.BlobId.BlobSize() == dataBlobId.BlobSize())
                    {
                        result.PatchingCandidate = mark.BlobId;
                        ++result.PatchingCandidateChangedBlockCount;
                    } else if (result.PatchingCandidate == mark.BlobId) {
                        ++result.PatchingCandidateChangedBlockCount;
                    }
                }
            } else {
                result.DataBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                result.ZeroBlocks.push_back(blockIndex);
            }
        } else {
            if (dataBlobId) {
                result.DataBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
            }
            if (zeroBlobId) {
                result.ZeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
            }
        }

        ++blockIndex;
    }

    return result;
}

struct TBlobPatchingResult {
    TPartialBlobId DataBlobId;
    TPartialBlobId PatchingCandidate;
};

TBlobPatchingResult ResolveBlobPatchingCandidate(
    TPartialBlobId dataBlobId,
    TPartialBlobId patchingCandidate,
    ui32 patchingCandidateChangedBlockCount,
    size_t dataBlocksCount,
    ui32 maxDiffPercentageForBlobPatching,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state)
{
    TBlobPatchingResult result{
        std::move(dataBlobId),
        std::move(patchingCandidate)};

    if (!result.PatchingCandidate) {
        return result;
    }

    TPartialBlobId targetBlobId(
        result.DataBlobId.Generation(),
        result.DataBlobId.Step(),
        result.PatchingCandidate.Channel(),
        result.DataBlobId.BlobSize(),
        result.DataBlobId.Cookie(),
        0);

    TLogoBlobID realTargetBlobId = MakeBlobId(
        tabletStorageInfo.TabletID,
        targetBlobId);

    ui32 originalChannel = result.PatchingCandidate.Channel();
    ui32 originalGroup = tabletStorageInfo.GroupFor(
        originalChannel,
        result.PatchingCandidate.Generation());
    Y_ABORT_UNLESS(originalGroup != Max<ui32>());

    ui32 patchedChannel = realTargetBlobId.Channel();
    ui32 patchedGroup = tabletStorageInfo.GroupFor(
        patchedChannel,
        realTargetBlobId.Generation());
    Y_ABORT_UNLESS(patchedGroup != Max<ui32>());

    bool found = TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(
        MakeBlobId(tabletStorageInfo.TabletID, result.PatchingCandidate),
        &realTargetBlobId,
        0xfe0000,
        originalGroup,
        patchedGroup);

    ui32 blockCount =
        result.PatchingCandidate.BlobSize() / state.GetBlockSize();
    ui32 patchingBlockCount =
        dataBlocksCount - patchingCandidateChangedBlockCount;
    ui32 changedPercentage = 100 * patchingBlockCount / blockCount;

    if (found &&
        (!maxDiffPercentageForBlobPatching ||
        changedPercentage <= maxDiffPercentageForBlobPatching))
    {
        result.DataBlobId = TPartialBlobId(
            result.DataBlobId.Generation(),
            result.DataBlobId.Step(),
            result.PatchingCandidate.Channel(),
            result.DataBlobId.BlobSize(),
            realTargetBlobId.Cookie(),
            0);
    } else {
        result.PatchingCandidate = {};
    }

    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void PrepareRangeCompaction(
    const TStorageConfig& config,
    const ui32 maxSkippedBlobs,
    const ui64 commitId,
    const TActorContext& ctx,
    const ui64 tabletId,
    const bool readBlockMaskOnCompactionOptimizationEnabled,
    bool& ready,
    TPartitionDatabase& db,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    const TString& logTitle)
{
    TCompactionBlockVisitor visitor(
        args,
        commitId,
        state.GetCompactionMap(),
        tabletId);
    state.FindFreshBlocks(visitor, args.BlockRange, commitId);
    visitor.KeepTrackOfAffectedBlocks = true;
    ready &= state.FindMixedBlocksForCompaction(db, visitor, args.RangeIdx);
    visitor.KeepTrackOfAffectedBlocks = false;
    ready &= db.FindMergedBlocks(
        visitor,
        visitor,
        args.BlockRange,
        true,   // precharge
        state.GetMaxBlocksInBlob(),
        commitId);

    visitor.Finish();

    if (ready && maxSkippedBlobs > 0) {
        ApplyIncrementalCompactionSkipping(
            config,
            maxSkippedBlobs,
            ctx,
            logTitle,
            state,
            args);
    }

    const ui32 checksumBoundary =
        config.GetDiskPrefixLengthWithBlockChecksumsInBlobs()
        / state.GetBlockSize();
    args.ChecksumsEnabled = args.BlockRange.Start < checksumBoundary;

    ReadAffectedBlobsForCompaction(
        commitId,
        tabletId,
        readBlockMaskOnCompactionOptimizationEnabled,
        ready,
        db,
        state,
        args);
}

void CompleteRangeCompaction(
    const bool blobPatchingEnabled,
    const ui32 mergedBlobThreshold,
    const ui64 commitId,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    TVector<TBlobCompactionRequest>& requests,
    TVector<TRangeCompactionInfo>& rangeCompactionInfos,
    ui32 maxDiffPercentageForBlobPatching)
{
    const EChannelPermissions compactionPermissions =
        EChannelPermission::SystemWritesAllowed;
    const auto initialRequestsSize = requests.size();

    const auto blockCounts = CountDataAndZeroBlocks(args.BlockMarks);

    const auto resultBlobIds = DetermineCompactionResultBlobIds(
        blockCounts.DataBlocksCount,
        blockCounts.ZeroBlocksCount,
        mergedBlobThreshold,
        commitId,
        compactionPermissions,
        state,
        args,
        rangeCompactionInfos.size());

    auto processResult = ProcessBlockMarksForCompaction(
        blobPatchingEnabled,
        resultBlobIds.DataBlobId,
        resultBlobIds.ZeroBlobId,
        tabletStorageInfo,
        state,
        args,
        requests,
        rangeCompactionInfos.size());

    const auto patchingResult = ResolveBlobPatchingCandidate(
        resultBlobIds.DataBlobId,
        processResult.PatchingCandidate,
        processResult.PatchingCandidateChangedBlockCount,
        blockCounts.DataBlocksCount,
        maxDiffPercentageForBlobPatching,
        tabletStorageInfo,
        state);

    rangeCompactionInfos.emplace_back(
        args.BlockRange,
        patchingResult.PatchingCandidate,
        patchingResult.DataBlobId,
        processResult.DataBlobSkipMask,
        resultBlobIds.ZeroBlobId,
        processResult.ZeroBlobSkipMask,
        args.BlobsSkipped,
        args.BlocksSkipped,
        std::move(processResult.BlockChecksums),
        resultBlobIds.ChannelDataKind,
        std::move(processResult.BlobContent),
        std::move(processResult.ZeroBlocks),
        std::move(args.AffectedBlobs),
        std::move(args.AffectedBlocks));

    if (!patchingResult.DataBlobId && !resultBlobIds.ZeroBlobId) {
        const auto rangeDescr = DescribeRange(args.BlockRange);
        Y_ABORT("No blocks in compacted range: %s", rangeDescr.c_str());
    }
    Y_ABORT_UNLESS(
        requests.size() - initialRequestsSize == blockCounts.DataBlocksCount);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
