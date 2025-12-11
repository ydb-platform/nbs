#pragma once

#include "public.h"

#include "alloc.h"
#include "block.h"

#include <cloud/storage/core/libs/common/byte_vector.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TBlockMark
{
    ui16 BlobOffset = 0;
    ui32 BlockIndex = 0;
    ui64 MinCommitId = 0;
    bool Zeroed = false;

    TBlockMark() = default;

    TBlockMark(ui16 blobOffset, ui32 blockIndex, ui64 minCommitId, bool zeroed)
        : BlobOffset(blobOffset)
        , BlockIndex(blockIndex)
        , MinCommitId(minCommitId)
        , Zeroed(zeroed)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TDeletedBlockMark
{
    ui16 BlobOffset = 0;
    ui64 MaxCommitId = 0;

    TDeletedBlockMark() = default;

    TDeletedBlockMark(ui16 blobOffset, ui64 maxCommitId)
        : BlobOffset(blobOffset)
        , MaxCommitId(maxCommitId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TBlockList
{
    class TBlockBuilder;
    class TDeletedBlockBuilder;

private:
    TByteVector Blocks{{GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList)}};
    TByteVector DeletedBlocks{
        {GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList)}};

public:
    TBlockList() = default;

    TBlockList(TByteVector blocks, TByteVector deletedBlocks)
        : Blocks(std::move(blocks))
        , DeletedBlocks(std::move(deletedBlocks))
    {}

    void Init(TByteVector blocks, TByteVector deletedBlocks)
    {
        Blocks = std::move(blocks);
        DeletedBlocks = std::move(deletedBlocks);
    }

    const TByteVector& EncodedBlocks() const
    {
        return Blocks;
    }

    const TByteVector& EncodedDeletedBlocks() const
    {
        return DeletedBlocks;
    }

    TMaybe<TBlockMark> FindBlock(
        ui32 blockIndex,
        ui64 maxCommitId = InvalidCommitId) const;

    ui64 FindDeletedBlock(ui16 blobOffset) const;

    TVector<TBlock> GetBlocks() const;
    TVector<TDeletedBlockMark> GetDeletedBlocks() const;

    ui32 CountBlocks() const;

private:
    template <typename T>
    void DecodeBlocks(T& builder) const;

    template <typename T>
    void DecodeDeletedBlocks(T& builder) const;
};

////////////////////////////////////////////////////////////////////////////////

class TBlockListBuilder
{
private:
    TVector<TBlockMark> Blocks;
    TVector<TDeletedBlockMark> DeletedBlocks;

public:
    TBlockListBuilder() = default;

    TBlockListBuilder(
        TVector<TBlockMark> blocks,
        TVector<TDeletedBlockMark> deletedBlocks)
        : Blocks(std::move(blocks))
        , DeletedBlocks(std::move(deletedBlocks))
    {}

    void
    AddBlock(ui16 blobOffset, ui32 blockIndex, ui64 minCommitId, bool zeroed)
    {
        Blocks.emplace_back(blobOffset, blockIndex, minCommitId, zeroed);
    }

    void AddDeletedBlock(ui16 blobOffset, ui64 maxCommitId)
    {
        DeletedBlocks.emplace_back(blobOffset, maxCommitId);
    }

    TBlockList Finish();

private:
    TByteVector EncodeBlocks();
    TByteVector EncodeDeletedBlocks();
};

////////////////////////////////////////////////////////////////////////////////

TBlockList BuildBlockList(const TVector<TBlock>& blocks);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
