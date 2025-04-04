#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// We process 4 MB of data at a time.
// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui64 ProcessingRangeSize = 4_MB;

////////////////////////////////////////////////////////////////////////////////

class TProcessingBlocks
{
private:
    ui64 BlockCount;
    ui32 BlockSize;
    std::unique_ptr<TCompressedBitmap> BlockMap;

    struct TInfoToReport {
        ui64 LastReportedProcessingIndex = 0;
        ui64 NextIndexToReport = 0;
        ui64 BlocksToProcessCount = 0;
    };

    std::optional<TInfoToReport> ReportInfo;

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
    bool IsProcessed(TBlockRange64 range) const;
    void MarkProcessed(TBlockRange64 range);
    bool SkipProcessedRanges();
    bool AdvanceProcessingIndex();
    TBlockRange64 BuildProcessingRange() const;
    ui64 GetBlockCountNeedToBeProcessed() const;
    ui64 GetProcessedBlockCount() const;

    std::optional<ui64> GetLastReportedProcessingIndex() const
    {
        if (ReportInfo) {
            return ReportInfo->LastReportedProcessingIndex;
        }
        return std::nullopt;
    }

    void SetLastReportedProcessingIndex(ui64 i)
    {
        if (!ReportInfo) {
            ReportInfo.emplace();
        }
        ReportInfo->LastReportedProcessingIndex = i;
    }

    [[nodiscard]] std::optional<TInfoToReport> GetReportInfo()
    {
        return ReportInfo;
    }

    void CalculateNextReportedProcessingIndex(ui64 minStep, ui64 threshold);

private:
    ui64 CalculateNextProcessingIndex() const;
};

}   // namespace NCloud::NBlockStore::NStorage
