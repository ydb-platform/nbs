
#include "block_range_splitter.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/algorithm.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore::NStorage {
void TBlockRangeSplitter::Reset(const NProto::TVolumeMeta& meta)
{
    BlockSize = meta.GetVolumeConfig().GetBlockSize();

    if (meta.GetDevices().size() > 1) {
        const ui64 firstDeviceBlockCount =
            meta.GetDevices().Get(0).GetBlocksCount();
        if (AllOf(
                meta.GetDevices(),
                [firstDeviceBlockCount](const NProto::TDeviceConfig& device)
                { return device.GetBlocksCount() == firstDeviceBlockCount; }))
        {
            BlocksPerStripe = firstDeviceBlockCount;
        } else {
            ui64 blockIndex = 0;
            for (const auto& device: meta.GetDevices()) {
                BlockIndices.push_back(blockIndex);
                blockIndex += device.GetBlocksCount();
            }
            BlockIndices.push_back(blockIndex);
        }
    }
}

size_t TBlockRangeSplitter::CalculateRequestCount(
    TBlockRange64 blockRange,
    TVector<TBlockRange64>* splittedRanges) const
{
    if (!BlocksPerStripe && BlockIndices.empty()) {
        if (splittedRanges) {
            splittedRanges->push_back(blockRange);
        }
        return 1;
    }

    if (BlocksPerStripe) {
        size_t first = blockRange.Start / BlocksPerStripe;
        size_t last = blockRange.End / BlocksPerStripe;
        if (splittedRanges) {
            for (auto i = first; i <= last; ++i) {
                auto stripePiece = TBlockRange64::WithLength(
                    (first + i) * BlocksPerStripe,
                    BlocksPerStripe);
                splittedRanges->push_back(blockRange.Intersect(stripePiece));
            }
        }
        return last - first + 1;
    }

    const auto fi =
        UpperBound(BlockIndices.begin(), BlockIndices.end(), blockRange.Start);
    const auto li =
        UpperBound(BlockIndices.begin(), BlockIndices.end(), blockRange.End);

    const auto first = std::distance(BlockIndices.begin(), fi) - 1;
    const auto last = std::distance(BlockIndices.begin(), li) - 1;
    if (splittedRanges) {
        for (auto i = first; i <= last; ++i) {
            auto stripePiece = TBlockRange64::MakeClosedInterval(
                BlockIndices[i],
                BlockIndices[i + 1] - 1);
            splittedRanges->push_back(blockRange.Intersect(stripePiece));
        }
    }
    return last - first + 1;
}

}   // namespace NCloud::NBlockStore::NStorage
