#pragma once

#include "public.h"

#include "block.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct IFreshBlocksIndexVisitor
{
    virtual ~IFreshBlocksIndexVisitor() = default;

    virtual bool Visit(const TFreshBlock& block) = 0;
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

    // TODO: pass blockContent by value
    bool AddBlock(
        ui32 blockIndex,
        ui64 commitId,
        bool isStoredInDb,
        TStringBuf blockContent);

    bool RemoveBlock(ui32 blockIndex, ui64 commitId, bool isStoredInDb);

    void FindBlocks(
        IFreshBlocksIndexVisitor& visitor,
        const TBlockRange32& blockRange,
        ui64 checkpointId) const;

    void GetCommitIds(ui32 blockIndex, TVector<ui64>& commitIds);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
