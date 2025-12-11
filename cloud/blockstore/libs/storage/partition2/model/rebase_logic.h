#pragma once

#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct IPivotalCommitStorage
{
    virtual ~IPivotalCommitStorage()
    {}

    virtual ui64 RebaseCommitId(ui64 commitId) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlock;

////////////////////////////////////////////////////////////////////////////////

struct TBlockCounts
{
    ui16 LiveBlocks = 0;
    ui16 GarbageBlocks = 0;
    ui16 CheckpointBlocks = 0;
};

TBlockCounts RebaseBlocks(
    const IPivotalCommitStorage& pivotalCommitStorage,
    const std::function<bool(ui32)>& isFrozenBlock,
    const ui64 lastCommitId,
    TVector<TBlock>& blocks,
    TSet<ui64>& pivotalCommitIds);

ui32 MarkOverwrittenBlocks(
    const TVector<TBlock>& overwritten,
    TVector<TBlock>& blocks);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
