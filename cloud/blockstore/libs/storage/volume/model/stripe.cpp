#include "stripe.h"

#include <util/generic/cast.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TStripeInfo ConvertToRelativeBlockRange(
    const ui32 blocksPerStripe,
    const TBlockRange64& original,
    const ui32 partitionCount,
    const ui32 requestNo)
{
    const auto firstStripe = original.Start / blocksPerStripe;
    const auto lastStripe = original.End / blocksPerStripe;
    const auto firstRequestStripe = firstStripe + requestNo;
    const auto lastRequestStripe =
        firstRequestStripe +
        partitionCount * ((lastStripe - firstRequestStripe) / partitionCount);

    TBlockRange64 result;
    result.Start = (firstRequestStripe / partitionCount) * blocksPerStripe;
    if (firstRequestStripe == firstStripe) {
        result.Start += original.Start - firstStripe * blocksPerStripe;
    }

    result.End = (lastRequestStripe / partitionCount) * blocksPerStripe;
    if (lastRequestStripe == lastStripe) {
        result.End += original.End - lastStripe * blocksPerStripe;
    } else {
        result.End += blocksPerStripe - 1;
    }

    return {result, IntegerCast<ui32>(firstRequestStripe % partitionCount)};
}

ui64 RelativeToGlobalIndex(
    const ui32 blocksPerStripe,
    const ui64 relativeIndex,
    const ui32 partitionCount,
    const ui32 partitionId)
{
    const auto relativeStripe = relativeIndex / blocksPerStripe;
    const auto offsetInStripe =
        relativeIndex - relativeStripe * blocksPerStripe;
    const auto stripe = relativeStripe * partitionCount + partitionId;
    return stripe * blocksPerStripe + offsetInStripe;
}

TBlockRange64 CalculateStripeRange(
    const ui32 blocksPerStripe,
    const ui64 globalIndex)
{
    const auto stripeInd = globalIndex / blocksPerStripe;
    return TBlockRange64::WithLength(
        stripeInd * blocksPerStripe,
        blocksPerStripe);
}

ui32 CalculateRequestCount(
    const ui32 blocksPerStripe,
    const TBlockRange64& original,
    const ui32 partitionCount)
{
    const auto firstStripe = original.Start / blocksPerStripe;
    const auto lastStripe = original.End / blocksPerStripe;
    return Min(IntegerCast<ui32>(lastStripe - firstStripe + 1), partitionCount);
}

}   // namespace NCloud::NBlockStore::NStorage
