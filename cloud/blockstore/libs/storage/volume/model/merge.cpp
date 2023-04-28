#include "merge.h"

#include <cloud/blockstore/libs/storage/volume/model/stripe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void MergeStripedBitMask(
    const TBlockRange64& originalRange,
    const TBlockRange64& stripeRange,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount,
    const ui32 partitionId,
    const TString& srcMask,
    TString& dstMask)
{
    const auto actualDstSize = (originalRange.Size() + 7) / 8;

    if (dstMask.size() < actualDstSize) {
        dstMask.resize(actualDstSize, 0);
    }

    for (ui32 i = 0; i < static_cast<ui32>(srcMask.size()); ++i) {
        for (ui32 bitIdx = 0; bitIdx < 8; ++bitIdx) {
            auto bit = srcMask[i] & (1 << bitIdx);
            if (!bit) {
                continue;
            }

            const auto index = RelativeToGlobalIndex(
                blocksPerStripe,
                stripeRange.Start + i * 8 + bitIdx,
                partitionsCount,
                partitionId
            );

            int blockIdx = index - originalRange.Start;
            int maskChunk = blockIdx / 8;
            int maskPos = blockIdx % 8;

            dstMask[maskChunk] = dstMask[maskChunk] | (1 << maskPos);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
