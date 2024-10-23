#include "processing_blocks.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TProcessingBlocks::TProcessingBlocks(
        ui64 blockCount,
        ui32 blockSize,
        ui64 initialProcessingIndex)
    : BlockCount(blockCount)
    , BlockSize(blockSize)
    , BlockMap(std::make_unique<TCompressedBitmap>(BlockCount))
    , CurrentProcessingIndex(initialProcessingIndex)
    , NextProcessingIndex(CalculateNextProcessingIndex())
{
    if (CurrentProcessingIndex) {
        MarkProcessed(TBlockRange64::WithLength(0, CurrentProcessingIndex));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TProcessingBlocks::AbortProcessing()
{
    BlockMap.reset();
    CurrentProcessingIndex = 0;
    NextProcessingIndex = 0;
}

bool TProcessingBlocks::IsProcessing() const
{
    return !!BlockMap;
}

bool TProcessingBlocks::IsProcessed(TBlockRange64 range) const
{
    return BlockMap->Count(range.Start, range.End + 1) == range.Size();
}

void TProcessingBlocks::MarkProcessed(TBlockRange64 range)
{
    BlockMap->Set(
        range.Start,
        Min(BlockCount, range.End + 1)
    );
}

bool TProcessingBlocks::AdvanceProcessingIndex()
{
    auto range = BuildProcessingRange();
    MarkProcessed(range);

    CurrentProcessingIndex = NextProcessingIndex;
    return SkipProcessedRanges();
}

bool TProcessingBlocks::SkipProcessedRanges()
{
    // skipping long contiguous ranges of set bits
    while (CurrentProcessingIndex < BlockCount) {
        ui64 chunkEnd = AlignDown<ui64>(
            CurrentProcessingIndex + TCompressedBitmap::CHUNK_SIZE,
            TCompressedBitmap::CHUNK_SIZE);
        const auto bits = BlockMap->Count(CurrentProcessingIndex, chunkEnd);
        if (bits != chunkEnd - CurrentProcessingIndex) {
            break;
        }

        CurrentProcessingIndex = chunkEnd;
    }

    while (CurrentProcessingIndex < BlockCount
            && BlockMap->Test(CurrentProcessingIndex))
    {
        ++CurrentProcessingIndex;
    }

    NextProcessingIndex = CalculateNextProcessingIndex();
    if (NextProcessingIndex == CurrentProcessingIndex) {
        // processing finished
        BlockMap.reset();
        return false;
    }

    return true;
}

TBlockRange64 TProcessingBlocks::BuildProcessingRange() const
{
    return TBlockRange64::WithLength(
        CurrentProcessingIndex,
        NextProcessingIndex - CurrentProcessingIndex
    );
}

ui64 TProcessingBlocks::GetBlockCountNeedToBeProcessed() const
{
    return BlockCount - GetProcessedBlockCount();
}

ui64 TProcessingBlocks::GetProcessedBlockCount() const
{
    if (BlockMap) {
        return BlockMap->Count();
    }
    return BlockCount;
}

ui64 TProcessingBlocks::CalculateNextProcessingIndex() const
{
    return Min(
        BlockCount,
        CurrentProcessingIndex + ProcessingRangeSize / BlockSize);
}

}   // namespace NCloud::NBlockStore::NStorage
