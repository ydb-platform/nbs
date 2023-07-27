#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/size_literals.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// TODO: increase x4?
constexpr ui64 ResyncRangeSize = 4_MB;

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, ui32> BlockRange2RangeId(TBlockRange64 range, ui32 blockSize);
TBlockRange64 RangeId2BlockRange(ui32 rangeId, ui32 blockSize);

}   // namespace NCloud::NBlockStore::NStorage
