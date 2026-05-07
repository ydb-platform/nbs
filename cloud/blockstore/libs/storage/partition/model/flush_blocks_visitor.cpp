#include "flush_blocks_visitor.h"
#include "library/cpp/containers/stack_vector/stack_vec.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

constexpr size_t MaxOnStackTmpVectorSize = 128;

////////////////////////////////////////////////////////////////////////////////

template <typename TOutputContainer>
TOutputContainer SplitBlocksByCompactionRangeBorders(
    const TVector<TBlock>& blocks,
    const TCompactionMap& compactionMap)
{
    TOutputContainer blocksByRanges;
    size_t start = 0;
    while (start < blocks.size()) {
        size_t end = start + 1;
        while (end < blocks.size() &&
               compactionMap.GetRangeStart(blocks[end - 1].BlockIndex) ==
                   compactionMap.GetRangeStart(blocks[end].BlockIndex))
        {
            ++end;
        }

        blocksByRanges.emplace_back(start, end);
        start = end;
    }
    return blocksByRanges;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFlushBlocksVisitor::TBlob::TBlob(
        TBlockBuffer blobContent,
        TVector<TBlock> blocks,
        TVector<ui32> checksums,
        ui8 compactionRangeCount)
    : BlobContent(std::move(blobContent))
    , Blocks(std::move(blocks))
    , Checksums(std::move(checksums))
    , CompactionRangeCount(compactionRangeCount)
{}

TFlushBlocksVisitor::TFlushBlocksVisitor(
        TVector<TBlob>& blobs,
        ui32 blockSize,
        ui32 flushBlobSizeThreshold,
        ui32 maxBlobRangeSize,
        ui32 maxBlocksInBlob,
        ui64 diskPrefixLengthWithBlockChecksumsInBlobs,
        const TCompactionMap& compactionMap,
        bool readBlockMaskOnCompactionOptimizationEnabled,
        ui64 splitByCompactionRangeMaxBlobCount,
        ui64 tabletId)
    : Blobs(blobs)
    , BlockSize(blockSize)
    , FlushBlobSizeThreshold(flushBlobSizeThreshold)
    , MaxBlobRangeSize(maxBlobRangeSize)
    , MaxBlocksInBlob(maxBlocksInBlob)
    , DiskPrefixLengthWithBlockChecksumsInBlobs(
          diskPrefixLengthWithBlockChecksumsInBlobs)
    , CompactionMap(compactionMap)
    , ReadBlockMaskOnCompactionOptimizationEnabled(
          readBlockMaskOnCompactionOptimizationEnabled)
    , SplitByCompactionRangeMaxBlobCount(splitByCompactionRangeMaxBlobCount)
    , TabletId(tabletId)
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

void TFlushBlocksVisitor::AppendDataBlob(
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

void TFlushBlocksVisitor::FlushZeroBlob(TVector<TBlock> blocks)
{
    FlushBlob({}, std::move(blocks), {});
}

void TFlushBlocksVisitor::FlushBlob(
    TBlockBuffer blobContent,
    TVector<TBlock> blocks,
    TVector<ui32> checksums)
{
    const size_t maxCompactionRangeCountPerBlob =
        MaxBlobRangeSize / CompactionMap.GetRangeSize();

    // With standard compaction range size and max blob range size
    // the number of compaction ranges per blob will not exceed
    // MaxOnStackTmpVectorSize
    if (Y_LIKELY(maxCompactionRangeCountPerBlob <= MaxOnStackTmpVectorSize)) {
        FlushBlobImpl<
            TStackVec<std::pair<size_t, size_t>, MaxOnStackTmpVectorSize>>(
            std::move(blobContent),
            std::move(blocks),
            std::move(checksums));
    } else {
        FlushBlobImpl<TVector<std::pair<size_t, size_t>>>(
            std::move(blobContent),
            std::move(blocks),
            std::move(checksums));
    }
}

template <typename TTmpContainerType>
void TFlushBlocksVisitor::FlushBlobImpl(
    TBlockBuffer blobContent,
    TVector<TBlock> blocks,
    TVector<ui32> checksums)
{
    STORAGE_VERIFY(blocks, TWellKnownEntityTypes::TABLET, TabletId);

    if (!SplitByCompactionRangeMaxBlobCount ||
        !ReadBlockMaskOnCompactionOptimizationEnabled)
    {
        AppendDataBlob(
            std::move(blobContent),
            std::move(blocks),
            std::move(checksums));
        return;
    }

    auto blocksByRanges =
        SplitBlocksByCompactionRangeBorders<TTmpContainerType>(
            blocks,
            CompactionMap);

    const ui64 compactionRangeCount = blocksByRanges.size();
    const bool splitByCompactionBorders =
        compactionRangeCount <= SplitByCompactionRangeMaxBlobCount;

    if (!splitByCompactionBorders || compactionRangeCount == 1) {
        AppendDataBlob(
            std::move(blobContent),
            std::move(blocks),
            std::move(checksums));
        return;
    }

    const auto& dataRefs = blobContent.GetBlocks();
    Y_DEBUG_ABORT_UNLESS((dataRefs.size() == blocks.size()) || (!dataRefs));

    for (const auto& [start, end]: blocksByRanges) {
        TVector<TBlock> pieceBlocks(
            blocks.begin() + start,
            blocks.begin() + end);

        TBlockBuffer pieceBuf{TProfilingAllocator::Instance()};
        if (dataRefs) {
            for (size_t i = start; i < end; ++i) {
                pieceBuf.AddBlock(dataRefs[i]);
            }
        }

        TVector<ui32> pieceChecksums;
        if (checksums) {
            size_t checksumsStart = Min(start, checksums.size());
            size_t checksumsEnd = Min(end, checksums.size());

            pieceChecksums = TVector<ui32>(
                checksums.begin() + checksumsStart,
                checksums.begin() + checksumsEnd);
        }

        AppendDataBlob(
            std::move(pieceBuf),
            std::move(pieceBlocks),
            std::move(pieceChecksums));
    }
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
