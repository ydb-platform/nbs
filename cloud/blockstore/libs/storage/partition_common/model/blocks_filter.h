#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <deque>
#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * An in-memory block filter that determines whether a block range may have
 * entries in the mixed index at a given commit ID.
 *
 * A false result is exact and allows the caller to skip a mixed-index lookup. A
 * true result is conservative: the caller still has to query the mixed index.
 * The filter is split into compaction ranges and tracks writes made while
 * compactions are in flight so that a completed compaction can safely replace
 * the corresponding bitmap ranges.
 */
class TBlocksFilter
{
    struct TCompaction
    {
        // Sorted indices of all ranges processed by this compaction.
        TVector<ui32> RangeIndices;

        // The commit ID used by the compaction.
        ui64 CommitId = 0;

        // Blocks added to the mixed index at or after BaselineCommitId while
        // the compaction was in flight.
        THashSet<ui32> MixedBlocksAddedDuringCompaction;

        ui64 BaselineCommitId = 0;
    };

private:
    // Used to identify the affected tablet in invariant violations.
    const ui64 TabletId = 0;

    // Number of logical blocks in one compaction range.
    const ui64 BlocksPerRange = 0;

    // Number of logical blocks in the partition tracked by this filter.
    const ui64 BlockCount = 0;

    // A set bit means that the block may have a mixed-index entry at or after
    // the baseline commit ID of its range.
    TCompressedBitmap BlocksFilter;

    // Baseline commit ID for every range. An empty value means that the range
    // has not been initialized and must be treated conservatively.
    TVector<std::optional<ui64>> CompactionRangeCommitIds;

    // In-flight compactions, ordered by strictly increasing commit ID.
    std::deque<TCompaction> Compactions;

public:
    /**
     * Creates an empty filter. All ranges remain uninitialized until a
     * compaction for them completes.
     *
     * @param tabletId - Tablet ID used in invariant violations.
     * @param blocksPerRange - Number of logical blocks in a compaction range.
     * @param blockCount - Total number of logical blocks.
     */
    TBlocksFilter(ui64 tabletId, ui64 blocksPerRange, size_t blockCount);

    /**
     * Checks whether a mixed-index lookup may be necessary.
     *
     * @param range - Closed logical block range to check.
     * @param commitId - Commit ID at which the blocks are read.
     *
     * @return - True if any block may have a mixed-index entry, false if every
     *  block is known to be absent.
     */
    [[nodiscard]] bool MayHaveBlocksInMixedIndex(TBlockRange32 range,
                                                 ui64 commitId) const;

    /**
     * Records block added to the mixed index.
     *
     * @param blockIndex - Block index that was added.
     * @param commitId - Commit ID at which the block was added.
     *
     * @return - True if the filter changed, false if the block was already
     * present or is older than the range baseline.
     */
    bool BlocksAddedToMixedIndex(ui64 blockIndex, ui64 commitId);

    /**
     * Returns the number of tracked blocks in a compaction range.
     *
     * @param rangeIndex - Zero-based compaction range index.
     */
    [[nodiscard]] ui64 GetBlocksCount(ui32 rangeIndex) const;

    /**
     * Registers an in-flight compaction. Compactions must be registered in
     * strictly increasing commit-ID order.
     *
     * @param rangeIndices - Indices of all ranges processed by the compaction.
     * @param commitId - Snapshot commit ID used by the compaction.
     */
    void CompactionStarted(TVector<ui32> rangeIndices, ui64 commitId);

    /**
     * Publishes the result of the oldest in-flight compaction. Each compacted
     * range receives the compaction baseline commit ID as its new baseline and
     * retains only mixed blocks added while that compaction was in flight.
     */
    void CompactionFinished();

    /**
     * Discards tracking state for the oldest in-flight compaction without
     * changing the bitmap or range baselines.
     */
    void CompactionFailed();

    void UpdateCompactionBaselineCommitId(ui64 compactionCommitId,
                                          ui64 baselineCommitId);

    std::optional<ui64> GetRangeBaselineCommitId(ui32 rangeIndex) const;

    /** Restores the persistent bitmap during tablet startup. */
    void SetBlocksFilter(TCompressedBitmap blocksFilter);

    /** Restores a persistent range baseline during tablet startup. */
    void SetRangeBaselineCommitId(ui32 rangeIndex, ui64 baselineCommitId);

    /** Serializes all bitmap chunks intersecting [begin, end). */
    TCompressedBitmap::TRangeSerializer RangeSerializer(ui64 begin,
                                                        ui64 end) const;

    /**
     * Returns the memory allocated for the bitmap and range baselines.
     * Transient in-flight compaction state is intentionally excluded.
     */
    [[nodiscard]] ui64 GetMemoryUsage() const;
};

}   // namespace NCloud::NBlockStore::NStorage
