#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

class TFreshBlocksInFlight
{
private:
    THashMap<ui32, size_t> BlockIndexToCount;
    TMap<ui64, size_t, TGreater<ui64>> CommitIdToCount;

    size_t BlockCount = 0;

public:
    TFreshBlocksInFlight() = default;

    void AddBlockRange(TBlockRange32 blockRange, ui64 commitId);
    void RemoveBlockRange(TBlockRange32 blockRange, ui64 commitId);

    size_t Size() const;

    bool HasBlocksAt(ui32 blockIndex) const;
    bool HasBlocksUntil(ui64 commitId) const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
