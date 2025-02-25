#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>

#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <functional>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRebaseResult
{
    ui32 LiveBlocksCount = 0;
    ui32 GarbageBlocksCount = 0;
    ui32 CheckpointBlocksCount = 0;

    TSet<ui64> UsedCheckpoints;
};

////////////////////////////////////////////////////////////////////////////////

using TFindCheckpoint = std::function<ui64(ui64 nodeId, ui64 commitId)>;
using TFindBlock = std::function<bool(ui64 nodeId, ui32 blockIndex)>;

////////////////////////////////////////////////////////////////////////////////

TRebaseResult RebaseBlocks(
    TVector<TBlock>& blocks,
    ui64 lastCommitId,
    const TFindCheckpoint& findCheckpoint,
    const TFindBlock& findBlock);

}   // namespace NCloud::NFileStore::NStorage
