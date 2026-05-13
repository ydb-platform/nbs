#include "flush_blocks_visitor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

TFlushBlocksVisitor::TFlushBlocksVisitor(
        ui32 blockSize,
        ui32 flushBlobSizeThreshold,
        ui32 maxBlobRangeSize,
        ui32 maxBlocksInBlob,
        ui64 diskPrefixLengthWithBlockChecksumsInBlobs,
        const TCompactionMap& compactionMap,
        bool readBlockMaskOnCompactionOptimizationEnabled,
        TVector<TBlob>& blobs)
    : BlockSize(blockSize)
    , FlushBlobSizeThreshold(flushBlobSizeThreshold)
    , MaxBlobRangeSize(maxBlobRangeSize)
    , MaxBlocksInBlob(maxBlocksInBlob)
    , DiskPrefixLengthWithBlockChecksumsInBlobs(
          diskPrefixLengthWithBlockChecksumsInBlobs)
    , CompactionMap(compactionMap)
    , ReadBlockMaskOnCompactionOptimizationEnabled(
          readBlockMaskOnCompactionOptimizationEnabled)
    , Blobs(blobs)
{}

bool TFlushBlocksVisitor::Visit(const TFreshBlock& block)
{
    if (block.Content) {
        // NBS-299: we do not want to mix blocks that are too far from each
        // other
        if (GetBlobRangeSize(Blocks, block.Meta.BlockIndex) >
            MaxBlobRangeSize / BlockSize)
        {
            FlushBlob(
                std::move(BlobContent),
                std::move(Blocks),
                std::move(Checksums));
        }

        BlobContent.AddBlock({block.Content.data(), block.Content.size()});
        Blocks.emplace_back(
            block.Meta.BlockIndex,
            block.Meta.CommitId,
            block.Meta.IsStoredInDb);

        const ui32 checksumBoundary =
            DiskPrefixLengthWithBlockChecksumsInBlobs / BlockSize;
        const bool checksumsEnabled =
            block.Meta.BlockIndex < checksumBoundary;

        if (checksumsEnabled) {
            Checksums.resize(Blocks.size());
            Checksums[Blocks.size() - 1] =
                ComputeDefaultDigest(BlobContent.GetBlocks().back());
        }

        if (Blocks.size() == MaxBlocksInBlob) {
            FlushBlob(
                std::move(BlobContent),
                std::move(Blocks),
                std::move(Checksums));
        }
    } else {
        const auto blobRangeSize =
            GetBlobRangeSize(ZeroBlocks, block.Meta.BlockIndex);
        if (blobRangeSize > MaxBlobRangeSize / BlockSize) {
            FlushZeroBlob(std::move(ZeroBlocks));
        }

        ZeroBlocks.emplace_back(
            block.Meta.BlockIndex,
            block.Meta.CommitId,
            block.Meta.IsStoredInDb);

        if (ZeroBlocks.size() == MaxBlocksInBlob) {
            FlushZeroBlob(std::move(ZeroBlocks));
        }
    }

    return true;
}

void TFlushBlocksVisitor::Finish()
{
    const auto dataSize = Blocks.size() * BlockSize;
    if (Blocks && (!Blobs || dataSize >= FlushBlobSizeThreshold)) {
        FlushBlob(
            std::move(BlobContent),
            std::move(Blocks),
            std::move(Checksums));
    }

    if (ZeroBlocks &&
        (!Blobs || ZeroBlocks.size() >= FlushBlobSizeThreshold))
    {
        FlushZeroBlob(std::move(ZeroBlocks));
    }
}

ui8 TFlushBlocksVisitor::CalculateCompactionRangeCount(
    const TVector<TBlock>& blocks) const
{
    const ui32 compactionRangeCount =
        CompactionMap.GetRangeIndex(blocks.back().BlockIndex) -
        CompactionMap.GetRangeIndex(blocks.front().BlockIndex) + 1;

    return compactionRangeCount <= Max<ui8>() ? compactionRangeCount : 0;
}

void TFlushBlocksVisitor::FlushZeroBlob(TVector<TBlock> blocks)
{
    FlushBlob({}, std::move(blocks), {});
}

void TFlushBlocksVisitor::FlushBlob(
    TBlockBuffer blobContent,
    TVector<TBlock> blocks,
    TVector<ui32> checksums)
{
    const ui8 compactionRangeCount =
        ReadBlockMaskOnCompactionOptimizationEnabled
            ? CalculateCompactionRangeCount(blocks)
            : 0;
    Blobs.emplace_back(
        std::move(blobContent),
        std::move(blocks),
        std::move(checksums),
        compactionRangeCount);
}

ui32 TFlushBlocksVisitor::GetBlobRangeSize(
    const TVector<TBlock>& blocks,
    ui32 blockIndex)
{
    if (blocks) {
        ui32 firstBlockIndex = blocks.front().BlockIndex;
        Y_ABORT_UNLESS(firstBlockIndex <= blockIndex);
        return blockIndex - firstBlockIndex;
    }
    return 0;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
