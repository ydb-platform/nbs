#pragma once

#include "public.h"

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlock
{
    TBlock Meta;
    TStringBuf Content;

    TFreshBlock(TBlock meta, TStringBuf content)
        : Meta(meta)
        , Content(content)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TBlockIndex
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TBlockIndex(IAllocator* allocator = TDefaultAllocator::Instance());
    ~TBlockIndex();

    bool AddBlock(
        ui32 blockIndex,
        TStringBuf blockContent,
        ui64 minCommitId,
        ui64 maxCommitId = InvalidCommitId);
    bool RemoveBlock(ui32 blockIndex, ui64 minCommitId);

    ui64 AddDeletedBlock(ui32 blockIndex, ui64 maxCommitId);

    bool HasBlockAt(ui32 blockIndex) const;
    const TFreshBlock* FindBlock(ui32 blockIndex) const;
    const TFreshBlock* FindBlock(ui32 blockIndex, ui64 checkpointId) const;

    TVector<TFreshBlock> FindBlocks() const;
    TVector<TFreshBlock> FindBlocks(ui64 checkpointId) const;

    TVector<TFreshBlock> FindBlocks(const TBlockRange32& blockRange) const;
    TVector<TFreshBlock> FindBlocks(
        const TBlockRange32& blockRange,
        ui64 checkpointId) const;

    size_t GetBlockCount() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
