#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void MergeStripedBitMask(
    const TBlockRange64& originalRange,
    const TBlockRange64& stripeRange,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount,
    const ui32 partitionId,
    const TString& srcMask,
    TString& dstMask);

}   // namespace NCloud::NBlockStore::NStorage
