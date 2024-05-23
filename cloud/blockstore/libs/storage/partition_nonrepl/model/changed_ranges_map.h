#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/bitmap.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Stores and manages a map of changed blocks with a reduced resolution.
// The RangeSize divided by BlockSize determines how many blocks are stored in
// one bit.
class TChangedRangesMap
{
private:
    const ui64 BlockCount;
    const ui32 BlockSize;
    const ui32 RangeSize;
    const ui32 BlocksPerRange;

    TDynBitMap ChangedRangesMap;

public:
    TChangedRangesMap(ui64 blockCount, ui32 blockSize, ui32 rangeSize);

    // The entire region becomes changed if at least one block has been changed.
    void MarkChanged(TBlockRange64 range);

    // All blocks of the range must be marked as unchanged in order to remove
    // the mark from the region.
    void MarkNotChanged(TBlockRange64 range);

    // Returns a map of modified blocks, where each bit is one block.
    [[nodiscard]] TString GetChangedBlocks(TBlockRange64 range) const;

private:
    void Mark(TBlockRange64 range, bool changed);
    [[nodiscard]] size_t GetRangeIndex(ui64 blockIndex) const;
};

}   // namespace NCloud::NBlockStore::NStorage
