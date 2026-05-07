#pragma once

#include "block_index.h"

#include <cloud/blockstore/libs/storage/core/compaction_map.h>

#include <cloud/storage/core/libs/common/block_buffer.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TFlushBlocksVisitor final: public IFreshBlocksIndexVisitor
{
public:
    struct TBlob
    {
        TBlockBuffer BlobContent;
        TVector<TBlock> Blocks;
        TVector<ui32> Checksums;
        ui8 CompactionRangeCount;

        TBlob(
            TBlockBuffer blobContent,
            TVector<TBlock> blocks,
            TVector<ui32> checksums,
            ui8 compactionRangeCount);
    };

private:
    TVector<TBlob>& Blobs;
    const ui32 BlockSize;
    const ui32 FlushBlobSizeThreshold;
    const ui32 MaxBlobRangeSize;
    const ui32 MaxBlocksInBlob;
    const ui64 DiskPrefixLengthWithBlockChecksumsInBlobs;
    const TCompactionMap& CompactionMap;
    const bool ReadBlockMaskOnCompactionOptimizationEnabled;
    const ui64 SplitByCompactionRangeMaxBlobCount;
    const ui64 TabletId;

    TBlockBuffer BlobContent{TProfilingAllocator::Instance()};

    TVector<TBlock> Blocks;
    TVector<ui32> Checksums;
    TVector<TBlock> ZeroBlocks;

public:
    TFlushBlocksVisitor(
        TVector<TBlob>& blobs,
        ui32 blockSize,
        ui32 flushBlobSizeThreshold,
        ui32 maxBlobRangeSize,
        ui32 maxBlocksInBlob,
        ui64 diskPrefixLengthWithBlockChecksumsInBlobs,
        const TCompactionMap& compactionMap,
        bool readBlockMaskOnCompactionOptimizationEnabled,
        ui64 splitByCompactionRangeMaxBlobCount,
        ui64 tabletId);

    bool Visit(const TFreshBlock& block) override;

    void Finish();

private:
    [[nodiscard]] ui8 CalculateCompactionRangeCount(
        const TVector<TBlock>& blocks) const;

    void AppendDataBlob(
        TBlockBuffer blobContent,
        TVector<TBlock> blocks,
        TVector<ui32> checksums);

    void FlushZeroBlob(TVector<TBlock> blocks);

    void FlushBlob(
        TBlockBuffer blobContent,
        TVector<TBlock> blocks,
        TVector<ui32> checksums);

    template <typename TTmpContainerType>
    void FlushBlobImpl(
        TBlockBuffer blobContent,
        TVector<TBlock> blocks,
        TVector<ui32> checksums);

    static ui32 GetBlobRangeSize(const TVector<TBlock>& blocks, ui32 blockIndex);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
