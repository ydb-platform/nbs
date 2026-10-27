#include "huge_slot_sizes.h"

#include <util/generic/algorithm.h>

#include <cmath>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TVector<ui32>> CalculateHugeSlotSizes(
    const NProto::THugeSlotConfig& config)
{
    const ui64 appendBlockSize = config.GetAppendBlockSize();
    const ui64 minBlobSize = config.GetMinHugeBlobSize();
    const ui64 maxBlobSize = config.GetMaxBlobSize();
    if (!appendBlockSize || !minBlobSize || minBlobSize >= maxBlobSize) {
        return MakeError(E_ARGUMENT, "Invalid huge-slot size bounds");
    }

    const ui64 left = minBlobSize / appendBlockSize;
    const ui64 right = (maxBlobSize + appendBlockSize - 1) / appendBlockSize;
    TVector<ui32> sizes;
    const auto append = [&](ui64 blocks) -> bool
    {
        const ui64 bytes = blocks * appendBlockSize;
        if (bytes > Max<ui32>()) {
            return false;
        }
        sizes.push_back(static_cast<ui32>(bytes));
        return true;
    };

    if (config.GetLayout() == NProto::THugeSlotConfig::V1) {
        const ui64 milestoneSize = config.GetMilestoneBlobSize();
        const ui64 overhead = config.GetOverhead();
        if (!overhead || minBlobSize > milestoneSize ||
            milestoneSize >= maxBlobSize)
        {
            return MakeError(E_ARGUMENT, "Invalid V1 huge-slot parameters");
        }

        const ui64 milestone = milestoneSize / appendBlockSize;
        for (auto current = milestone; current > left;) {
            if (!append(current)) {
                return MakeError(E_ARGUMENT, "Huge slot size exceeds ui32");
            }
            current = current * overhead / (overhead + 1);
        }
        Reverse(sizes.begin(), sizes.end());
        for (auto current = milestone; current < right;) {
            current += Max<ui64>(current / overhead, 1);
            if (!append(current)) {
                return MakeError(E_ARGUMENT, "Huge slot size exceeds ui32");
            }
        }
        return sizes;
    }

    if (config.GetLayout() != NProto::THugeSlotConfig::V2 &&
        config.GetLayout() != NProto::THugeSlotConfig::TINY_DISK_V2)
    {
        return MakeError(E_ARGUMENT, "Unknown huge-slot layout");
    }

    const ui64 chunkSize = config.GetChunkSize();
    const ui32 steps =
        config.GetLayout() == NProto::THugeSlotConfig::TINY_DISK_V2
            ? 3
            : config.GetStepsBetweenPowersOf2();
    // Limit the number of steps and ensure each floating-point step advances.
    if (!left || !chunkSize || chunkSize % appendBlockSize ||
        maxBlobSize > chunkSize || !steps || steps > 64)
    {
        return MakeError(E_ARGUMENT, "Invalid V2 huge-slot parameters");
    }

    const double powerStep = std::pow(2.0, 1.0 / steps);
    const ui64 blocksInChunk = chunkSize / appendBlockSize;
    ui64 powerOf2 = 1;
    while (powerOf2 < left * appendBlockSize) {
        powerOf2 *= 2;
    }
    for (auto previous = left; previous < right; powerOf2 *= 2) {
        auto size = powerOf2;
        for (ui32 step = 0; step < steps && previous < right; ++step) {
            const ui64 blocks = (size + appendBlockSize - 1) / appendBlockSize;
            const ui64 slotsInChunk = blocksInChunk / blocks;
            if (!slotsInChunk) {
                return MakeError(E_ARGUMENT, "Huge slot does not fit in chunk");
            }
            const ui64 finalBlocks = blocksInChunk / slotsInChunk;
            if (finalBlocks != previous) {
                if (!append(finalBlocks)) {
                    return MakeError(E_ARGUMENT, "Huge slot size exceeds ui32");
                }
                previous = finalBlocks;
            }
            // Match YDB's truncation after every multiplication.
            size = static_cast<ui64>(size * powerStep);
        }
    }
    return sizes;
}

}   // namespace NCloud::NBlockStore::NStorage
