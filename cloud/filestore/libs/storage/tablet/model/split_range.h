#pragma once

#include "public.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void SplitRange(
    ui32 blockIndex,
    ui32 blocksCount,
    ui32 maxBlocksCount,
    T&& processRange)
{
    ui32 blockOffset = 0;
    ui32 padding = blockIndex % maxBlocksCount;

    ui32 headBlocks = Min(maxBlocksCount - padding, blocksCount);
    if (headBlocks) {
        processRange(blockOffset, headBlocks);
        blockOffset += headBlocks;
    }

    ui32 rangesCount = (blocksCount + padding) / maxBlocksCount;
    for (size_t i = 1; i < rangesCount; ++i) {
        processRange(blockOffset, maxBlocksCount);
        blockOffset += maxBlocksCount;
    }

    ui32 tailBlocks = blocksCount - blockOffset;
    if (tailBlocks) {
        processRange(blockOffset, tailBlocks);
        blockOffset += tailBlocks;
    }

    Y_VERIFY(blockOffset == blocksCount);
}

}   // namespace NCloud::NFileStore::NStorage
