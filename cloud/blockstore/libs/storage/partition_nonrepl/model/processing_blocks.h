#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/model/common_constants.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TProcessingBlocks
{
private:
    ui64 BlockCount;
    ui32 BlockSize;
    std::unique_ptr<TCompressedBitmap> BlockMap;
    std::optional<ui64> LastReportedProcessingIndex;
    ui64 CurrentProcessingIndex = 0;
    ui64 NextProcessingIndex = 0;

public:
    TProcessingBlocks(
        ui64 blockCount,
        ui32 blockSize,
        ui64 initialProcessingIndex);

    TProcessingBlocks(
        ui64 blockCount,
        ui32 blockSize,
        TCompressedBitmap blockMap);

    TProcessingBlocks(TProcessingBlocks&& other) noexcept;
    TProcessingBlocks& operator=(TProcessingBlocks&& other) noexcept;

public:
    void AbortProcessing();
    bool IsProcessing() const;
    bool IsProcessingDone() const;
    bool IsProcessed(TBlockRange64 range) const;
    void MarkProcessed(TBlockRange64 range);
    bool SkipProcessedRanges();
    bool AdvanceProcessingIndex();
    TBlockRange64 BuildProcessingRange() const;
    ui64 GetBlockCountNeedToBeProcessed() const;
    ui64 GetProcessedBlockCount() const;

    std::optional<ui64> GetLastReportedProcessingIndex() const
    {
        return LastReportedProcessingIndex;
    }

    void SetLastReportedProcessingIndex(ui64 i)
    {
        LastReportedProcessingIndex = i;
    }

private:
    ui64 CalculateNextProcessingIndex() const;
};

}   // namespace NCloud::NBlockStore::NStorage
