#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// TODO: increase x4? or even x10?
// or is it actually better to run 10 resyncs in parallel? will cause less freezes upon random reads
constexpr ui64 ResyncRangeBlockCount = 1024;

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, ui32> BlockRange2RangeId(TBlockRange64 range);
TBlockRange64 RangeId2BlockRange(ui32 rangeId);

}   // namespace NCloud::NBlockStore::NStorage
