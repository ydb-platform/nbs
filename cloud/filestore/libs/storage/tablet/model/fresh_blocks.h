#pragma once

#include "public.h"

#include "alloc.h"
#include "block.h"

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TFreshBlocks
{
    using TFreshBlockMap = TMap<TBlock, TStringBuf, TBlockCompare>;

private:
    IAllocator* Allocator;
    TFreshBlockMap Blocks;

public:
    TFreshBlocks(IAllocator* allocator);

    ~TFreshBlocks();

    size_t GetBlocksCount() const
    {
        return Blocks.size();
    }

    bool AddBlock(
        ui64 nodeId,
        ui32 blockIndex,
        TStringBuf blockData,
        ui32 blockSize,
        ui64 minCommitId,
        ui64 maxCommitId = InvalidCommitId);

    ui64 MarkBlockDeleted(ui64 nodeId, ui32 blockIndex, ui64 commitId);

    using TVersionedBlock = std::pair<ui32, ui64>;
    TVector<TVersionedBlock> MarkBlocksDeleted(
        ui64 nodeId,
        ui32 blockIndex,
        ui32 blocksCount,
        ui64 commitId);

    bool RemoveBlock(ui64 nodeId, ui32 blockIndex, ui64 commitId);

    bool FindBlock(ui64 nodeId, ui32 blockIndex) const;

    TMaybe<TFreshBlock>
    FindBlock(ui64 nodeId, ui32 blockIndex, ui64 commitId) const;

    void FindBlocks(IFreshBlockVisitor& visitor) const;
    void FindBlocks(IFreshBlockVisitor& visitor, ui64 commitId) const;

    void FindBlocks(
        IFreshBlockVisitor& visitor,
        ui64 nodeId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    void FindBlocks(
        IFreshBlockVisitor& visitor,
        ui64 nodeId,
        ui32 blockIndex,
        ui32 blocksCount,
        ui64 commitId) const;

private:
    TStringBuf AllocateBlock(TStringBuf content, ui32 blockSize);
    void ReleaseBlock(TStringBuf content);
};

}   // namespace NCloud::NFileStore::NStorage
