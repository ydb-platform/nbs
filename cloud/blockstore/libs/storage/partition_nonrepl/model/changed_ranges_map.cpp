#include "changed_ranges_map.h"

#include <util/system/align.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TChangedRangesMap::TChangedRangesMap(
    ui64 blockCount,
    ui32 blockSize,
    ui32 rangeSize)
    : BlockCount(blockCount)
    , BlockSize(blockSize)
    , RangeSize(rangeSize)
    , BlocksPerRange(RangeSize / BlockSize)
{
    Y_DEBUG_ABORT_UNLESS(RangeSize % BlockSize == 0);

    const auto rangesCount = GetRangeIndex(blockCount);
    ChangedRangesMap.Reserve(rangesCount);
    ChangedRangesMap.Set(0, rangesCount);
}

void TChangedRangesMap::MarkChanged(TBlockRange64 range)
{
    Mark(range, true);
}

void TChangedRangesMap::MarkNotChanged(TBlockRange64 range)
{
    Mark(range, false);
}

TString TChangedRangesMap::GetChangedBlocks(TBlockRange64 range) const
{
    constexpr size_t BitsInByte = 8;
    const auto resultSize = AlignUp<size_t>(range.Size(), BitsInByte);

    // We know a map of changed blocks with resolution = RangeSize.
    // Let's convert this map to resolution = BlockSize.
    TDynBitMap map;
    map.Reserve(resultSize);
    // Initially, we assume that all blocks have been changed.
    map.Set(0, resultSize);

    for (size_t i = range.Start; i <= range.End;) {
        // We find the block index for the current range and mark many blocks at
        // once.
        size_t rangeIndex = GetRangeIndex(i);
        size_t rangeStartIndex = rangeIndex * BlocksPerRange;
        size_t rangeEndIndex = rangeStartIndex + BlocksPerRange;

        size_t trimmedStart = Max(range.Start, rangeStartIndex);
        size_t trimmedEnd = Min(range.End + 1, rangeEndIndex);

        const bool isOutOfRange = rangeIndex >= BlockCount / BlocksPerRange;
        const bool isChanged = isOutOfRange || ChangedRangesMap.Get(rangeIndex);
        if (!isChanged) {
            map.Reset(trimmedStart - range.Start, trimmedEnd - range.Start);
        }
        i = trimmedEnd;
    }

    // Convert the TDynBitMap to a TString.
    TString result;
    result.resize(resultSize / BitsInByte);
    for (size_t i = 0; i < result.size(); ++i) {
        ui8 tmp = 0;
        map.Export(i * BitsInByte, tmp);
        result[i] = static_cast<char>(tmp);
    }
    return result;
}

void TChangedRangesMap::Mark(TBlockRange64 range, bool changed)
{
    range.End = Min(range.End, BlockCount - 1);

    if (changed) {
        // When mark changed to a part of range occurs, we mark the entire
        // range.
        const auto begin = GetRangeIndex(range.Start);
        const auto end = GetRangeIndex(range.End) + 1;
        ChangedRangesMap.Set(begin, end);
    } else {
        // When the part of the range is marked as clean, we mark only the
        // ranges that completely overlap with input range
        auto alignedStart = AlignUp<ui64>(range.Start, BlocksPerRange);
        auto alignedEnd = AlignDown<ui64>(range.End + 1, BlocksPerRange);
        if (alignedEnd > alignedStart) {
            // Converting the indexes of the beginning and end to the indexes of
            // the ranges.
            const auto begin = GetRangeIndex(alignedStart);
            const auto end = GetRangeIndex(alignedEnd);
            ChangedRangesMap.Reset(begin, end);
        }
    }
}

size_t TChangedRangesMap::GetRangeIndex(ui64 blockIndex) const
{
    return blockIndex / BlocksPerRange;
}

}   // namespace NCloud::NBlockStore::NStorage
