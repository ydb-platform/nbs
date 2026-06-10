#pragma once

#include "contrib/ydb/library/actors/util/shared_data.h"
#include "util/system/types.h"

namespace NCloud::NBlockStore::NStorage {

class TBlocksBloomFilter
{
public:
    TBlocksBloomFilter(ui32 elementsCount, double errorRate);

    void Add(ui32 blockIndex);
    [[nodiscard]] bool Test(ui32 blockIndex) const;

private:
    ui16 Hashes;
    ui64 Items;
    NActors::TSharedData Raw;
    TArrayRef<ui64> Array;
};

}   // namespace NCloud::NBlockStore::NStorage
