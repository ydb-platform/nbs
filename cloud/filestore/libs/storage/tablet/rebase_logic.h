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
    ui32 LiveBlocks = 0;
    ui32 GarbageBlocks = 0;
    ui32 CheckpointBlocks = 0;

    TSet<ui64> UsedCheckpoints;
};

////////////////////////////////////////////////////////////////////////////////

using TFindCheckpoint = std::function<ui64(ui64 nodeId, ui64 commitId)>;
using TFindBlock = std::function<bool(ui64 nodeId, ui32 blockIndex)>;

////////////////////////////////////////////////////////////////////////////////

TRebaseResult RebaseMixedBlocks(
    TVector<TBlock>& blocks,
    ui64 lastCommitId,
    TFindCheckpoint findCheckpoint,
    TFindBlock findBlock);

}   // namespace NCloud::NFileStore::NStorage
