#include "blob_builder.h"

#include <cloud/filestore/libs/storage/model/block_buffer.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TMixedBlobBuilder::Accept(const TBlock& block, std::pair<TStringBuf, IBlockBufferPtr> blockData)
{
    Y_ABORT_UNLESS(blockData.first.size() == BlockSize);

    auto& range = Ranges[GetMixedRangeIndex(
        Hasher,
        block.NodeId,
        block.BlockIndex)];
    AddBlock(range, block, std::move(blockData));

    if (range.Blocks.size() == MaxBlocksInBlob) {
        CompleteBlob(range);
    }
}

void TMixedBlobBuilder::AddBlock(
    TRange& range,
    const TBlock& block,
    std::pair<TStringBuf, IBlockBufferPtr> blockData)
{
    if (!range.BlobContent) {
        range.BlobContent.reserve(MaxBlocksInBlob * BlockSize);
    }

    range.Blocks.push_back(block);
    if (blockData.second) {
        range.BlobContent.emplace_back(std::move(blockData));
    } else {
        Y_ABORT_UNLESS(blockData.first.size() == BlockSize);
        auto blockBuffer = CreateBlockBuffer(
            TByteRange(0, blockData.first.size(), blockData.first.size()),
            TString(blockData.first));
        range.BlobContent.emplace_back(
            blockBuffer->GetBlock(0),
            std::move(blockBuffer));
    }

    ++BlocksCount;

    if (range.Blocks.size() == MaxBlocksInBlob) {
        CompleteBlob(range);
    }
}

void TMixedBlobBuilder::CompleteBlob(TRange& range)
{
    Blobs.emplace_back(
        TPartialBlobId(), // need to generate BlobId later
        std::move(range.Blocks),
        std::move(range.BlobContent));

    ++BlobsCount;

    range.Blocks.clear();
    range.BlobContent.clear();
}

TVector<TMixedBlob> TMixedBlobBuilder::Finish()
{
    for (auto& [rangeId, range]: Ranges) {
        if (range.Blocks) {
            CompleteBlob(range);
        }
    }

    return std::move(Blobs);
}

////////////////////////////////////////////////////////////////////////////////

void TMergedBlobBuilder::Accept(
    const TBlock& block,
    ui32 blocksCount,
    ui32 blockOffset,
    IBlockBuffer& buffer)
{
    TString blobContent;
    blobContent.reserve(blocksCount * BlockSize);

    for (size_t i = 0; i < blocksCount; ++i) {
        auto blockData = buffer.GetBlock(blockOffset + i);
        Y_ABORT_UNLESS(blockData.size() == BlockSize);

        blobContent.append(blockData);
    }

    BlocksCount += blocksCount;

    Blobs.emplace_back(
        TPartialBlobId(), // need to generate BlobId later
        block,
        blocksCount,
        std::move(blobContent));

    ++BlobsCount;
}

TVector<TMergedBlob> TMergedBlobBuilder::Finish()
{
    return std::move(Blobs);
}

////////////////////////////////////////////////////////////////////////////////

void TCompactionBlobBuilder::Accept(const TBlockDataRef& block)
{
    Blocks.emplace_back(block);
    ++BlocksCount;

    if (Blocks.size() == MaxBlocksInBlob) {
        CompleteBlob();
    }
}

TVector<TCompactionBlob> TCompactionBlobBuilder::Finish()
{
    if (Blocks) {
        CompleteBlob();
    }

    return std::move(Blobs);
}

void TCompactionBlobBuilder::CompleteBlob()
{
    Blobs.emplace_back(TPartialBlobId(), std::move(Blocks));
    ++BlobsCount;

    Blocks.clear();
}

////////////////////////////////////////////////////////////////////////////////

void TFlushBytesBlobBuilder::Accept(TBlockWithBytes block)
{
    auto& range = Ranges[GetMixedRangeIndex(
        Hasher,
        block.NodeId,
        block.BlockIndex)];
    range.Blocks.push_back(std::move(block));

    if (range.Blocks.size() == MaxBlocksInBlob) {
        CompleteBlob(range);
    }
}

void TFlushBytesBlobBuilder::CompleteBlob(TRange& range)
{
    Blobs.emplace_back(TPartialBlobId(), std::move(range.Blocks));

    range.Blocks.clear();
}

TVector<TFlushBytesBlob> TFlushBytesBlobBuilder::Finish()
{
    for (auto& [rangeId, range]: Ranges) {
        if (range.Blocks) {
            CompleteBlob(range);
        }
    }

    return std::move(Blobs);
}

}   // namespace NCloud::NFileStore::NStorage
