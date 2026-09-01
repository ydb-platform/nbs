#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

using TBlockMask = TBitMap<MaxBlocksCount>;

TBlockMask BlockMaskFromString(TStringBuf s);
TStringBuf BlockMaskAsString(const TBlockMask& mask);

bool IsBlockMaskFull(const TBlockMask& mask, ui32 blockCount);

TBlockMask GetFullBlockMask(ui32 blockCount);

void SetSkippedBlockIds(
    NProto::TBlobMeta2::TMergedBlocks& mergedBlocks,
    const TBlockMask& skipMask);

TBlockMask GetSkippedBlockMask(
    const NProto::TBlobMeta2::TMergedBlocks& mergedBlocks);

ui32 GetSkippedBlockCount(
    const NProto::TBlobMeta2::TMergedBlocks& mergedBlocks);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
