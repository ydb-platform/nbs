#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/bitmap.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

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

    void MarkChanged(TBlockRange64 range);
    void MarkNotChanged(TBlockRange64 range);
    [[nodiscard]] TString GetChangedBlocks(TBlockRange64 range) const;

private:
    void Mark(TBlockRange64 range, bool changed);
    [[nodiscard]] size_t GetRangeIndex(ui64 blockIndex) const;
};

}   // namespace NCloud::NBlockStore::NStorage
