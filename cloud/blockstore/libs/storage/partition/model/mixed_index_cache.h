#pragma once

#include "public.h"

#include "block.h"

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/memory/alloc.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlock
{
    TPartialBlobId BlobId;
    ui64 CommitId;
    ui32 BlockIndex;
    ui16 BlobOffset;

    TMixedBlock(
        TPartialBlobId blobId,
        ui64 commitId,
        ui32 blockIndex,
        ui16 blobOffset)
        : BlobId(blobId)
        , CommitId(commitId)
        , BlockIndex(blockIndex)
        , BlobOffset(blobOffset)
    {}

    bool operator==(const TMixedBlock& other) const
    {
        return BlockIndex == other.BlockIndex && CommitId == other.CommitId &&
               BlobId == other.BlobId && BlobOffset == other.BlobOffset;
    }
};

static_assert(sizeof(TMixedBlock) == 32);

////////////////////////////////////////////////////////////////////////////////

class TMixedIndexCache
{
public:
    enum class ERangeTemperature
    {
        Cold,
        Warm,
        Hot
    };

    // BlockIndex and CommitId.
    using TBlockKey = std::pair<ui32, ui64>;

    struct IInserter
    {
        virtual ~IInserter() = default;

        virtual void Insert(TMixedBlock block) = 0;
    };

    using TInserterPtr = std::unique_ptr<IInserter>;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TMixedIndexCache(ui32 maxSize, IAllocator* = TDefaultAllocator::Instance());

    ~TMixedIndexCache();

    ERangeTemperature GetRangeTemperature(ui32 rangeIdx) const;
    void RaiseRangeTemperature(ui32 rangeIdx);

    TInserterPtr GetInserterForRange(ui32 rangeIdx);
    void InsertBlockIfHot(ui32 rangeIdx, TMixedBlock block);
    void EraseBlockIfHot(ui32 rangeIndex, TBlockKey key);

    bool VisitBlocksIfHot(ui32 rangeIdx, IBlocksIndexVisitor& visitor);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
