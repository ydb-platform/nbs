#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TStripeInfo
{
    TBlockRange64 BlockRange;
    ui32 PartitionId = 0;

    TStripeInfo() = default;
    TStripeInfo(TBlockRange64 blockRange, ui32 partitionId)
        : BlockRange(blockRange)
        , PartitionId(partitionId)
    {}
};

TStripeInfo ConvertToRelativeBlockRange(
    const ui32 blocksPerStripe,
    const TBlockRange64& original,
    const ui32 partitionCount,
    const ui32 requestNo);

ui64 RelativeToGlobalIndex(
    const ui32 blocksPerStripe,
    const ui64 relativeIndex,
    const ui32 partitionCount,
    const ui32 partitionId);

TBlockRange64 CalculateStripeRange(
    const ui32 blocksPerStripe,
    const ui64 globalIndex);

ui32 CalculateRequestCount(
    const ui32 blocksPerStripe,
    const TBlockRange64& original,
    const ui32 partitionCount);

}   // namespace NCloud::NBlockStore::NStorage
