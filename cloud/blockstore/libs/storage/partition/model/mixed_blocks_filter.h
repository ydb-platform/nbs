#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <deque>
#include <optional>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

/**
 * An in-memory filter that determines whether a block range may have entries in
 * the mixed index at a given commit ID.
 *
 * A false result is exact and allows the caller to skip a mixed-index lookup. A
 * true result is conservative: the caller still has to query the mixed index.
 * The filter is split into compaction ranges and tracks writes made while
 * compactions are in flight so that a completed compaction can safely replace
 * the corresponding bitmap ranges.
 */
class TMixedBlocksFilter
{
    struct TCompaction
    {
        // Sorted indices of all ranges processed by this compaction.
        TVector<ui32> RangeIndices;

        // The commit ID used by the compaction.
        ui64 CommitId = 0;

        // Blocks added to the mixed index at or after CommitId while the
        // compaction was in flight.
        THashSet<ui32> MixedBlocksAddedDuringCompaction;
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
    TMixedBlocksFilter(ui64 tabletId, ui64 blocksPerRange, size_t blockCount);

    /**
     * Checks whether a mixed-index lookup may be necessary.
     *
     * @param range - Closed logical block range to check.
     * @param commitId - Commit ID at which the blocks are read.
     *
     * @return - True if any block may have a mixed-index entry, false if every
     *  block is known to be absent.
     */
    [[nodiscard]] bool MayHaveBlocksInMixedIndex(
        TBlockRange32 range,
        ui64 commitId) const;

    /**
     * Records block added to the mixed index.
     *
     * @param blockIndex - Block index that was added.
     * @param commitId - Commit ID at which the block was added.
     */
    void BlocksAddedToMixedIndex(ui64 blockIndex, ui64 commitId);

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
     * range receives the compaction commit ID as its new baseline and retains
     * only mixed blocks added while that compaction was in flight.
     */
    void CompactionFinished();

    /**
     * Discards tracking state for the oldest in-flight compaction without
     * changing the bitmap or range baselines.
     */
    void CompactionFailed();

    /**
     * Returns the memory allocated for the bitmap and range baselines.
     * Transient in-flight compaction state is intentionally excluded.
     */
    [[nodiscard]] ui64 GetMemoryUsage() const;

    /**
     * Checks if the range is initialized.
     *
     * @param compactionRangeIndex - Index of the compaction range to check.
     *
     * @return - True if the compaction range is initialized, false otherwise.
     */
    [[nodiscard]] bool IsCompactionRangeInitialized(
        ui64 compactionRangeIndex) const;

    /**
     * Initializes the compaction range.
     *
     * @param compactionRangeIndex - Index of the compaction range to
     * initialize.
     * @param commitId - Commit ID at which the compaction range is initialized.
     */
    void InitializeCompactionRange(ui64 compactionRangeIndex, ui64 commitId);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
