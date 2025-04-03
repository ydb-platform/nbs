#pragma once

#include "public.h"

#include "blob.h"

#include <cloud/filestore/libs/storage/model/public.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline ui32 CalculateMaxBlocksInBlob(ui32 maxBlobSize, ui32 blockSize)
{
    return blockSize != 0
        ? Min(MaxBlocksCount, maxBlobSize / blockSize)
        : maxBlobSize;
}

////////////////////////////////////////////////////////////////////////////////

class TMixedBlobBuilder final
    : public IFreshBlockVisitor
{
    struct TRange
    {
        TVector<TBlock> Blocks;
        TString BlobContent;
    };

private:
    const IBlockLocation2RangeIndex& Hasher;
    const ui32 BlockSize;
    const ui32 MaxBlocksInBlob;

    THashMap<ui32, TRange> Ranges;
    TVector<TMixedBlob> Blobs;

    size_t BlocksCount = 0;
    size_t BlobsCount = 0;

public:
    TMixedBlobBuilder(
            const IBlockLocation2RangeIndex& hasher,
            ui32 blockSize,
            ui32 maxBlocksCount)
        : Hasher(hasher)
        , BlockSize(blockSize)
        , MaxBlocksInBlob(maxBlocksCount)
    {}

    void Accept(const TBlock& block, TStringBuf blockData) override;

    TVector<TMixedBlob> Finish();

    size_t GetBlocksCount() const
    {
        return BlocksCount;
    }

    size_t GetBlobsCount() const
    {
        return BlobsCount;
    }

private:
    void AddBlock(TRange& range, const TBlock& block, TStringBuf blockData);
    void CompleteBlob(TRange& range);
};

////////////////////////////////////////////////////////////////////////////////

class TMergedBlobBuilder
{
private:
    const ui32 BlockSize;

    TVector<TMergedBlob> Blobs;

    size_t BlocksCount = 0;
    size_t BlobsCount = 0;

public:
    TMergedBlobBuilder(ui32 blockSize)
        : BlockSize(blockSize)
    {}

    void Accept(
        const TBlock& block,
        ui32 blocksCount,
        ui32 blockOffset,
        IBlockBuffer& buffer);

    TVector<TMergedBlob> Finish();

    size_t GetBlocksCount() const
    {
        return BlocksCount;
    }

    size_t GetBlobsCount() const
    {
        return BlobsCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionBlobBuilder
{
private:
    const ui32 MaxBlocksInBlob;

    TVector<TBlockDataRef> Blocks;
    TVector<TCompactionBlob> Blobs;

    size_t BlocksCount = 0;
    size_t BlobsCount = 0;

public:
    TCompactionBlobBuilder(ui32 maxBlocksCount)
        : MaxBlocksInBlob(maxBlocksCount)
    {}

    void Accept(const TBlockDataRef& block);

    TVector<TCompactionBlob> Finish();

    size_t GetBlocksCount() const
    {
        return BlocksCount;
    }

    size_t GetBlobsCount() const
    {
        return BlobsCount;
    }

private:
    void CompleteBlob();
};

////////////////////////////////////////////////////////////////////////////////

class TFlushBytesBlobBuilder
{
    struct TRange
    {
        TVector<TBlockWithBytes> Blocks;
    };

private:
    const IBlockLocation2RangeIndex& Hasher;
    const ui32 MaxBlocksInBlob;

    THashMap<ui32, TRange> Ranges;
    TVector<TFlushBytesBlob> Blobs;

public:
    TFlushBytesBlobBuilder(
            const IBlockLocation2RangeIndex& hasher,
            ui32 maxBlocksCount)
        : Hasher(hasher)
        , MaxBlocksInBlob(maxBlocksCount)
    {}

    void Accept(TBlockWithBytes block);

    TVector<TFlushBytesBlob> Finish();

private:
    void CompleteBlob(TRange& range);
};

}   // namespace NCloud::NFileStore::NStorage
