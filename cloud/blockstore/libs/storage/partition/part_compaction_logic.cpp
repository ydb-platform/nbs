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
#include <cloud/storage/core/libs/diagnostics/logging.h>
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

TRangeCompactionInfo::TRangeCompactionInfo(
    TBlockRange32 blockRange,
    TPartialBlobId originalBlobId,
    TPartialBlobId dataBlobId,
    TBlockMask dataBlobSkipMask,
    TPartialBlobId zeroBlobId,
    TBlockMask zeroBlobSkipMask,
    ui32 blobsSkippedByCompaction,
    ui32 blocksSkippedByCompaction,
    TVector<std::optional<ui32>> blockChecksums,
    EChannelDataKind channelDataKind,
    TBlockBuffer blobContent,
    TVector<ui32> zeroBlocks,
    TAffectedBlobs affectedBlobs,
    TAffectedBlocks affectedBlocks,
    TVector<TChecksumFixup> checksumFixups)
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
    , ChecksumFixups(std::move(checksumFixups))
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

void ApplyChecksumFixups(TRangeCompactionInfo& rc)
{
    for (const auto& fixup: rc.ChecksumFixups) {
        const auto* ab = rc.AffectedBlobs.FindPtr(fixup.BlobId);
        Y_ABORT_UNLESS(ab);
        auto* meta = ab->BlobMeta.Get();
        Y_ABORT_UNLESS(meta);

        ui32 checksum = fixup.BlobOffset < meta->BlockChecksumsSize()
                            ? meta->GetBlockChecksums(fixup.BlobOffset)
                            : 0;
        rc.BlockChecksums[fixup.ChecksumIndex] = checksum;
    }
}

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

    bool Visit(
        TBlockRange32 blockRange,
        const TPartialBlobId& blobId,
        ui32 skippedBlocksCount) override
    {
        auto& ab = Args.AffectedBlobs[blobId];
        ab.MaxCommitIdInCompactionRange = blobId.CommitId();
        ab.MinCommitIdInCompactionRange = blobId.CommitId();
        ab.CompactionRangeCount =
            CompactionMap.GetRangeIndex(blockRange.End) -
            CompactionMap.GetRangeIndex(blockRange.Start) + 1;

        ab.MergedBlobsSpecificInfo.ConstructInPlace();
        ab.MergedBlobsSpecificInfo->BlockRange = blockRange;
        ab.MergedBlobsSpecificInfo->SkippedBlocksCount = skippedBlocksCount;
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

bool IsBlobFullyAvailableForRangeCompaction(
    const TAffectedBlob& ab,
    ui64 commitId)
{
    const bool blobOnlyInOneCompactRange = ab.CompactionRangeCount == 1;

    return ab.MaxCommitIdInCompactionRange <= commitId &&
           blobOnlyInOneCompactRange;
}

struct TCompactionBlockCounts
{
    size_t DataBlockCount = 0;
    size_t ZeroBlockCount = 0;
};

TCompactionBlockCounts CountDataAndZeroBlocks(
    const TVector<TTxPartition::TRangeCompaction::TBlockMark>& blockMarks,
    ui64 tabletId)
{
    TCompactionBlockCounts counts;

    for (const auto& mark: blockMarks) {
        if (mark.CommitId) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            // there could be fresh block OR merged/mixed block
            STORAGE_VERIFY(
                !(isFresh && isMixedOrMerged),
                TWellKnownEntityTypes::TABLET,
                tabletId);
            if (isFresh || isMixedOrMerged) {
                ++counts.DataBlockCount;
            } else {
                ++counts.ZeroBlockCount;
            }
        }
    }

    return counts;
}

struct TCompactionResultBlobIds
{
    TPartialBlobId DataBlobId;
    TPartialBlobId ZeroBlobId;
    EChannelDataKind ChannelDataKind = EChannelDataKind::Merged;
};

TCompactionResultBlobIds DetermineCompactionResultBlobIds(
    size_t dataBlockCount,
    size_t zeroBlockCount,
    ui32 mergedBlobThreshold,
    ui64 commitId,
    EChannelPermissions compactionPermissions,
    TPartitionState& state,
    const TTxPartition::TRangeCompaction& args,
    size_t rangeCompactionInfosSize)
{
    TCompactionResultBlobIds result;

    if (dataBlockCount) {
        ui32 skipped = 0;
        for (const auto& mark: args.BlockMarks) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            if (!isFresh && !isMixedOrMerged) {
                ++skipped;
            }
        }

        const ui32 blobSize =
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

    if (zeroBlockCount) {
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
        const ui32 bytes = blocks * state.GetBlockSize();
        const bool blobCountOk = args.BlobsSkipped <= maxSkippedBlobs;
        const bool byteCountOk =
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

    THashSet<ui32> skippedBlockIndices;

    for (const auto& x: liveBlocks) {
        auto ab = args.AffectedBlobs.find(x.first);
        Y_ABORT_UNLESS(ab != args.AffectedBlobs.end());
        for (const auto& [blockIndex, _]: ab->second.AffectedBlocks) {
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

void FillBlobsInfoToRead(
    ui64 commitId,
    bool readBlockMaskOnCompactionOptimizationEnabled,
    TTxPartition::TRangeCompaction& args,
    TPartitionState& state,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlockMasks,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlobMetas)
{
    for (auto& kv: args.AffectedBlobs) {
        if (state.GetCleanupQueue().HasBlob(kv.first)) {
            kv.second.BlockMask = GetFullBlockMask(state.GetMaxBlocksInBlob());
            continue;
        }

        if (!IsBlobFullyAvailableForRangeCompaction(kv.second, commitId) ||
            !readBlockMaskOnCompactionOptimizationEnabled)
        {
            blobsToReadBlockMasks.emplace(kv.first);
        }
    }

    // blob metas are needed for checksums validation, so we will read
    // them only for blobs with live data
    if (args.ChecksumsEnabled) {
        for (const auto& mark: args.BlockMarks) {
            if (mark.CommitId && !mark.BlockContent &&
                !IsDeletionMarker(mark.BlobId))
            {
                blobsToReadBlobMetas.emplace(mark.BlobId);
            }
        }
    }
}

struct TBuildBlobContentAndMasksResult
{
    TBlockBuffer BlobContent;
    TVector<std::optional<ui32>> BlockChecksums;
    TVector<ui32> ZeroBlocks;
    TBlockMask DataBlobSkipMask;
    TBlockMask ZeroBlobSkipMask;
    TVector<TChecksumFixup> ChecksumFixups;
};

TBuildBlobContentAndMasksResult BuildBlobContentAndMasks(
    const TPartialBlobId& dataBlobId,
    const TPartialBlobId& zeroBlobId,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    TVector<TBlobCompactionRequest>& requests,
    size_t rangeCompactionInfosSize)
{
    TBuildBlobContentAndMasksResult result;
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
                result.BlobContent.AddBlock(
                    {mark.BlockContent.data(), mark.BlockContent.size()});

                if (args.ChecksumsEnabled) {
                    result.BlockChecksums.push_back(ComputeDefaultDigest(
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

                if (args.ChecksumsEnabled) {
                    // checksums will be moved from the affected blob's meta
                    // later
                    result.ChecksumFixups.emplace_back(
                        result.BlockChecksums.size(),   // ChecksumIndex
                        mark.BlobId,
                        mark.BlobOffset);
                    result.BlockChecksums.push_back(std::nullopt);
                }

                if (zeroBlobId) {
                    result.ZeroBlobSkipMask.Set(
                        blockIndex - args.BlockRange.Start);
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

struct TBlobPatchingResult
{
    TPartialBlobId DataBlobId;
    TPartialBlobId PatchingCandidate;
};

TBlobPatchingResult ResolveBlobPatchingCandidate(
    TPartialBlobId dataBlobId,
    size_t dataBlockCount,
    ui32 maxDiffPercentageForBlobPatching,
    TTxPartition::TRangeCompaction& args,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state)
{
    TPartialBlobId patchingCandidate;
    ui32 patchingCandidateChangedBlockCount = 0;

    for (auto& mark: args.BlockMarks) {
        if (!mark.CommitId || mark.BlockContent ||
            IsDeletionMarker(mark.BlobId))
        {
            continue;
        }

        if (!patchingCandidate &&
            mark.BlobId.BlobSize() == dataBlobId.BlobSize())
        {
            patchingCandidate = mark.BlobId;
            ++patchingCandidateChangedBlockCount;
        } else if (patchingCandidate == mark.BlobId) {
            ++patchingCandidateChangedBlockCount;
        }
    }

    TBlobPatchingResult result{
        .DataBlobId = dataBlobId,
        .PatchingCandidate = patchingCandidate};

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

    TLogoBlobID realTargetBlobId =
        MakeBlobId(tabletStorageInfo.TabletID, targetBlobId);

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
        /*bitsForBruteForce=*/0xfe0000,
        originalGroup,
        patchedGroup);

    ui32 blockCount =
        result.PatchingCandidate.BlobSize() / state.GetBlockSize();
    ui32 patchingBlockCount =
        dataBlockCount - patchingCandidateChangedBlockCount;
    ui32 changedPercentage = 100 * patchingBlockCount / blockCount;

    if (found && (!maxDiffPercentageForBlobPatching ||
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

void RecreateBlobMetas(TTxPartition::TRangeCompaction& args, ui64 commitId)
{
    for (auto& [blobId, ab]: args.AffectedBlobs) {
        if (ab.MergedBlobsSpecificInfo) {
            auto& info = ab.MergedBlobsSpecificInfo.GetRef();
            auto& meta = ab.RecreatedBlobMeta.ConstructInPlace();
            auto* mergedBlocks = meta.MutableMergedBlocks();
            mergedBlocks->SetStart(info.BlockRange.Start);
            mergedBlocks->SetEnd(info.BlockRange.End);
            mergedBlocks->SetSkipped(info.SkippedBlocksCount);
            continue;
        }

        // we can recreate blob meta for mixed blobs only if they are fully
        // available for range compaction
        if (!IsBlobFullyAvailableForRangeCompaction(ab, commitId)) {
            continue;
        }

        auto& meta = ab.RecreatedBlobMeta.ConstructInPlace();
        auto* mixedBlocks = meta.MutableMixedBlocks();
        for (const auto& affectedBlock: ab.AffectedBlocks) {
            mixedBlocks->AddBlocks(affectedBlock.BlockIndex);
            mixedBlocks->AddCommitIds(affectedBlock.CommitId);
        }
    }
}

void PrepareRangeCompaction(
    const TStorageConfig& config,
    const ui32 maxSkippedBlobs,
    const ui64 commitId,
    const ui64 tabletId,
    const bool readBlockMaskOnCompactionOptimizationEnabled,
    bool& ready,
    TPartitionDatabase& db,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlockMasks,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlobMetas)
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
            state,
            args);
    }

    const ui32 checksumBoundary =
        config.GetDiskPrefixLengthWithBlockChecksumsInBlobs() /
        state.GetBlockSize();
    args.ChecksumsEnabled = args.BlockRange.Start < checksumBoundary;

    FillBlobsInfoToRead(
        commitId,
        readBlockMaskOnCompactionOptimizationEnabled,
        args,
        state,
        blobsToReadBlockMasks,
        blobsToReadBlobMetas);
}

void CompleteRangeCompaction(
    const bool blobPatchingEnabled,
    const ui32 mergedBlobThreshold,
    const ui64 commitId,
    const ui64 tabletId,
    TTabletStorageInfo& tabletStorageInfo,
    TPartitionState& state,
    TTxPartition::TRangeCompaction& args,
    TVector<TBlobCompactionRequest>& requests,
    TVector<TRangeCompactionInfo>& rangeCompactionInfos,
    ui32 maxDiffPercentageForBlobPatching)
{
    const EChannelPermissions compactionPermissions =
        EChannelPermission::SystemWritesAllowed;
    const size_t initialRequestsSize = requests.size();

    const auto [dataBlockCount, zeroBlockCount] =
        CountDataAndZeroBlocks(args.BlockMarks, tabletId);

    const auto resultBlobIds = DetermineCompactionResultBlobIds(
        dataBlockCount,
        zeroBlockCount,
        mergedBlobThreshold,
        commitId,
        compactionPermissions,
        state,
        args,
        rangeCompactionInfos.size());

    auto buildBlobContentResult = BuildBlobContentAndMasks(
        resultBlobIds.DataBlobId,
        resultBlobIds.ZeroBlobId,
        tabletStorageInfo,
        state,
        args,
        requests,
        rangeCompactionInfos.size());

    TBlobPatchingResult patchingResult{
        .DataBlobId = resultBlobIds.DataBlobId,
        .PatchingCandidate = {}};

    if (blobPatchingEnabled) {
        patchingResult = ResolveBlobPatchingCandidate(
            resultBlobIds.DataBlobId,
            dataBlockCount,
            maxDiffPercentageForBlobPatching,
            args,
            tabletStorageInfo,
            state);
    }

    RecreateBlobMetas(args, commitId);

    rangeCompactionInfos.emplace_back(
        args.BlockRange,
        patchingResult.PatchingCandidate,
        patchingResult.DataBlobId,
        buildBlobContentResult.DataBlobSkipMask,
        resultBlobIds.ZeroBlobId,
        buildBlobContentResult.ZeroBlobSkipMask,
        args.BlobsSkipped,
        args.BlocksSkipped,
        std::move(buildBlobContentResult.BlockChecksums),
        resultBlobIds.ChannelDataKind,
        std::move(buildBlobContentResult.BlobContent),
        std::move(buildBlobContentResult.ZeroBlocks),
        std::move(args.AffectedBlobs),
        std::move(args.AffectedBlocks),
        std::move(buildBlobContentResult.ChecksumFixups));

    if (!patchingResult.DataBlobId && !resultBlobIds.ZeroBlobId) {
        const auto rangeDescr = DescribeRange(args.BlockRange);
        STORAGE_VERIFY_C(
            false,
            TWellKnownEntityTypes::TABLET,
            tabletId,
            TStringBuilder() << "No blocks in compacted range: " << rangeDescr);
    }
    STORAGE_VERIFY(
        requests.size() - initialRequestsSize == dataBlockCount,
        TWellKnownEntityTypes::TABLET,
        tabletId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
