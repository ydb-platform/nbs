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
        if (fixup.BlobOffset < meta->BlockChecksumsSize()) {
            rc.BlockChecksums[fixup.ChecksumIndex] =
                meta->GetBlockChecksums(fixup.BlobOffset);
        }
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

void FillBlobsInfoToRead(
    ui64 commitId,
    bool readBlockMaskOnCompactionOptimizationEnabled,
    TTxPartition::TRangeCompaction& args,
    TPartitionState& state,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlockMasks,
    THashSet<TPartialBlobId, TPartialBlobIdHash>& blobsToReadBlobMetas)
{
    for (auto& kv: args.AffectedBlobs) {
        state.IncrementBlobsProcessedDuringCompaction();

        if (state.GetCleanupQueue().HasBlob(kv.first)) {
            kv.second.BlockMask = GetFullBlockMask();
            continue;
        }

        const bool blobOnlyInOneCompactRange =
            kv.second.CompactionRangeCount == 1;

        const bool blobFullyAvailableForRangeCompaction =
            kv.second.MaxCommitIdInCompactionRange <= commitId &&
            blobOnlyInOneCompactRange;

        if (!blobFullyAvailableForRangeCompaction ||
            !readBlockMaskOnCompactionOptimizationEnabled)
        {
            state.IncrementBlockMaskReadDuringCompaction();
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

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

    // at first we count number of data blocks
    size_t dataBlocksCount = 0, zeroBlocksCount = 0;

    for (const auto& mark: args.BlockMarks) {
        if (mark.CommitId) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            // there could be fresh block OR merged/mixed block
            Y_ABORT_UNLESS(!(isFresh && isMixedOrMerged));
            if (isFresh || isMixedOrMerged) {
                ++dataBlocksCount;
            } else {
                ++zeroBlocksCount;
            }
        }
    }

    // determine the results kind
    TPartialBlobId dataBlobId, zeroBlobId;
    TBlockMask dataBlobSkipMask, zeroBlobSkipMask;

    auto channelDataKind = EChannelDataKind::Merged;
    if (dataBlocksCount) {
        ui32 skipped = 0;
        for (const auto& mark: args.BlockMarks) {
            const bool isFresh = !mark.BlockContent.empty();
            const bool isMixedOrMerged = !IsDeletionMarker(mark.BlobId);
            if (!isFresh && !isMixedOrMerged) {
                ++skipped;
            }
        }

        const auto blobSize = (args.BlockRange.Size() - skipped) * state.GetBlockSize();
        if (blobSize < mergedBlobThreshold) {
            channelDataKind = EChannelDataKind::Mixed;
        }
        dataBlobId = state.GenerateBlobId(
            channelDataKind,
            compactionPermissions,
            commitId,
            blobSize,
            rangeCompactionInfos.size());
    }

    if (zeroBlocksCount) {
        // for zeroed region we will write blob without any data
        // XXX same commitId used for 2 blobs: data blob and zero blob
        // we differentiate between them by storing the last block index in
        // MergedBlocksIndex::RangeEnd not for the last block of the processed
        // compaction range but for the last actual block that's referenced by
        // the corresponding blob
        zeroBlobId = state.GenerateBlobId(
            channelDataKind,
            compactionPermissions,
            commitId,
            0,
            rangeCompactionInfos.size());
    }

    // now build the blob content for all blocks to be written
    TBlockBuffer blobContent(TProfilingAllocator::Instance());
    TVector<ui32> blockChecksums;
    TVector<TChecksumFixup> checksumFixups;
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
                    rangeCompactionInfos.size());

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
                    rangeCompactionInfos.size());

                // we will read this block later
                blobContent.AddBlock(state.GetBlockSize(), char(0));

                if (args.ChecksumsEnabled) {
                    // checksums will be moved from the affected blob's meta
                    // later
                    checksumFixups.emplace_back(
                        blockChecksums.size(),   // ChecksumIndex
                        mark.BlobId,
                        mark.BlobOffset);
                    blockChecksums.push_back(0);
                }

                if (zeroBlobId) {
                    zeroBlobSkipMask.Set(blockIndex - args.BlockRange.Start);
                }

                if (blobPatchingEnabled) {
                    if (!patchingCandidate &&
                        mark.BlobId.BlobSize() == dataBlobId.BlobSize())
                    {
                        patchingCandidate = mark.BlobId;
                        ++patchingCandidateChangedBlockCount;
                    } else if (patchingCandidate == mark.BlobId) {
                        ++patchingCandidateChangedBlockCount;
                    }
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

    rangeCompactionInfos.emplace_back(
        args.BlockRange,
        patchingCandidate,
        dataBlobId,
        dataBlobSkipMask,
        zeroBlobId,
        zeroBlobSkipMask,
        args.BlobsSkipped,
        args.BlocksSkipped,
        std::move(blockChecksums),
        channelDataKind,
        std::move(blobContent),
        std::move(zeroBlocks),
        std::move(args.AffectedBlobs),
        std::move(args.AffectedBlocks),
        std::move(checksumFixups));

    if (!dataBlobId && !zeroBlobId) {
        const auto rangeDescr = DescribeRange(args.BlockRange);
        Y_ABORT("No blocks in compacted range: %s", rangeDescr.c_str());
    }
    Y_ABORT_UNLESS(requests.size() - initialRequestsSize == dataBlocksCount);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
