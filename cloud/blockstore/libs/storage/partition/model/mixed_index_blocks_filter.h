#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <deque>
#include <optional>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TMixedBlocksFilter
{

    struct TCompaction
    {
        TVector<ui32> RangesForCompaction;
        ui64 CommitId = 0;
        THashSet<ui32> MixedBlocksWrittenAfterCompaction;
    };

private:
    TCompressedBitmap Blocks;
    TVector<std::optional<ui64>> CommitIdsPerRange;

    std::deque<TCompaction> Compactions;

    ui64 BlocksPerRange = 0;

public:
    explicit TMixedBlocksFilter(ui64 blocksPerRange, size_t blockCount);

    [[nodiscard]] bool MayHaveBlocksInMixedIndex(
        TBlockRange32 range,
        ui64 commitId) const;

    void AddBlocksToMixedIndex(ui32 blockIndex, ui64 commitId);

    void StartCompactionRange(ui32 rangeIndex, ui64 commitId);

    void StartCompaction(TVector<ui32> rangeIndices, ui64 commitId);

    void CompactionFinished();

    void CompactionFailed();

    void UpdateChunk(TCompressedBitmap::TSerializedChunk chunk);

    void UpdateRangeCommitId(ui32 rangeIndex, ui64 commitId);

    [[nodiscard]] ui64 GetMemoryUsage() const;

    [[nodiscard]] bool IsRangeInitialized(ui32 rangeIndex) const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
