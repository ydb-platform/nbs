#pragma once

#include "public.h"

#include "block.h"

#include <cloud/filestore/libs/storage/tablet/model/blob_compression.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlobStats
{
    ui32 GarbageBlocks = 0;
    ui32 CheckpointBlocks = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlobMeta
{
    TPartialBlobId BlobId;
    TVector<TBlock> Blocks;
    TBlobCompressionInfo BlobCompressionInfo;

    TMixedBlobMeta() = default;

    TMixedBlobMeta(
            const TPartialBlobId& blobId,
            TVector<TBlock> blocks,
            TBlobCompressionInfo blobCompressionInfo)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
        , BlobCompressionInfo(std::move(blobCompressionInfo))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlob: TMixedBlobMeta
{
    TString BlobContent;

    TMixedBlob() = default;

    TMixedBlob(
            const TPartialBlobId& blobId,
            TVector<TBlock> blocks,
            TBlobCompressionInfo blobCompressionInfo,
            TString blobContent)
        : TMixedBlobMeta(
            blobId,
            std::move(blocks),
            std::move(blobCompressionInfo))
        , BlobContent(std::move(blobContent))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TMergedBlobMeta
{
    TPartialBlobId BlobId;
    TBlock Block;
    ui32 BlocksCount = 0;

    TMergedBlobMeta() = default;

    TMergedBlobMeta(
            const TPartialBlobId& blobId,
            const TBlock& block,
            ui32 blocksCount)
        : BlobId(blobId)
        , Block(block)
        , BlocksCount(blocksCount)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TMergedBlob: TMergedBlobMeta
{
    TString BlobContent;

    TMergedBlob() = default;

    TMergedBlob(
            const TPartialBlobId& blobId,
            const TBlock& block,
            ui32 blocksCount,
            TString blobContent)
        : TMergedBlobMeta(blobId, block, blocksCount)
        , BlobContent(std::move(blobContent))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TCompactionBlob
{
    TPartialBlobId BlobId;
    TVector<TBlockDataRef> Blocks;
    TBlobCompressionInfo BlobCompressionInfo;

    TCompactionBlob() = default;

    TCompactionBlob(
            const TPartialBlobId& blobId,
            TVector<TBlockDataRef> blocks)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushBytesBlob
{
    TPartialBlobId BlobId;
    TVector<TBlockWithBytes> Blocks;

    TFlushBytesBlob() = default;

    TFlushBytesBlob(
            const TPartialBlobId& blobId,
            TVector<TBlockWithBytes> blocks)
        : BlobId(blobId)
        , Blocks(std::move(blocks))
    {}
};

}   // namespace NCloud::NFileStore::NStorage
