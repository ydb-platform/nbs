#pragma once

#include "public.h"

#include "alloc.h"
#include "block.h"

#include <cloud/storage/core/libs/common/byte_vector.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IBlockIterator
{
    virtual ~IBlockIterator() = default;
    virtual bool Next() = 0;

    TBlock Block;
    ui32 BlobOffset = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBlockList
{
private:
    TByteVector EncodedBlocks;
    TByteVector EncodedDeletionMarkers;

public:
    TBlockList(TByteVector encodedBlocks, TByteVector encodedDeletionMarkers)
        : EncodedBlocks(std::move(encodedBlocks))
        , EncodedDeletionMarkers(std::move(encodedDeletionMarkers))
    {}

    const TByteVector& GetEncodedBlocks() const
    {
        return EncodedBlocks;
    }

    const TByteVector& GetEncodedDeletionMarkers() const
    {
        return EncodedDeletionMarkers;
    }

    IBlockIteratorPtr FindBlocks() const;

    IBlockIteratorPtr FindBlocks(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    struct TStats
    {
        size_t BlockEntries;
        size_t BlockGroups;

        size_t DeletionMarkers;
        size_t DeletionGroups;
    };

    TStats GetStats() const;

    TVector<TBlock> DecodeBlocks() const;

    static TBlockList EncodeBlocks(
        const TBlock& block,
        ui32 blocksCount,
        IAllocator* alloc);

    static TBlockList EncodeBlocks(
        const TVector<TBlock>& blocks,
        IAllocator* alloc);
};

}   // namespace NCloud::NFileStore::NStorage
