#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/public.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

using TBlockMask = TBitMap<MaxBlocksCount>;

TBlockMask BlockMaskFromString(TStringBuf s);
TStringBuf BlockMaskAsString(const TBlockMask& mask);

bool IsBlockMaskFull(const TBlockMask& mask, ui32 blockCount);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
