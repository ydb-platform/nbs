#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <optional>
#include <deque>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TMixedBlocksFilter
{
    struct TCompactionRangeInfo
    {
        ui64 CommitId = 0;
        TCompressedBitmap FilterAfterCompaction;
    };

private:
    TCompressedBitmap Blocks;
    TVector<std::optional<ui64>> StartCommitIdsPerRange;
    THashMap<ui32, std::deque<TCompactionRangeInfo>>
        RangeIndexToCompactionRangeInfos;

    ui64 BlocksPerRange = 0;

public:
    explicit TMixedBlocksFilter(ui64 blocksPerRange, size_t blockCount);

    [[nodiscard]] bool MayHaveBlocksInMixedIndex(
        TBlockRange32 range,
        ui64 commitId) const;

    void AddBlocksToMixedIndex(ui32 blockIndex, ui64 commitId);

    void StartCompactionRange(ui32 rangeIndex, ui64 commitId);

    void CompactionRangeFinished(ui32 rangeIndex);

    void CompactionRangeFailed(ui32 rangeIndex);

    void UpdateChunk(TCompressedBitmap::TSerializedChunk chunk);

    void UpdateRangeCommitId(ui32 rangeIndex, ui64 commitId);

    [[nodiscard]] ui64 GetMemoryUsage() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
