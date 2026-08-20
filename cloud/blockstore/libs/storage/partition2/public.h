#pragma once

#include <util/system/types.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

enum class ELevelIndex: ui32
{
    L0 = 0,
    L1 = 1,
};

struct TLevelIndexRangeState
{
    ELevelIndex Level = ELevelIndex::L0;
    ui32 RangeIndex = 0;
    ui32 BlobCount = 0;
    ui32 BlockCount = 0;
    std::optional<ui64> BlocksFilterBaselineCommitId;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
