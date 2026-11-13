#pragma once

#include <cloud/blockstore/libs/storage/partition_common/blob_buffer.h>
#include <cloud/blockstore/libs/storage/partition_common/model/block_index.h>

#include <cloud/blockstore/libs/storage/core/compaction_map.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

// Plans the output blob layout without retaining or copying fresh payload.
// Run TFlushBlocksMaterializer over the same fresh index before using the blobs.
class TFlushBlocksVisitor final: public IFreshBlocksIndexVisitor
{
public:
    struct TBlob
    {
        TBlobBuffer BlobContent;
        TVector<TBlock> Blocks;
        TVector<ui32> Checksums;
        ui8 CompactionRangeCount;
    };

private:
    const ui32 BlockSize;
    const ui32 FlushBlobSizeThreshold;
    const ui32 MaxBlobRangeSize;
    const ui32 MaxBlocksInBlob;
    const ui64 DiskPrefixLengthWithBlockChecksumsInBlobs;
    const TCompactionMap& CompactionMap;
    const bool ReadBlockMaskOnCompactionOptimizationEnabled;
    const ui64 SplitByCompactionRangeMaxBlobCount;
    const ui64 TabletId;
    const ui64 WriteBlobSizeThreshold;

    TVector<TBlob>& Blobs;

    TVector<TBlock> Blocks;
    TVector<ui32> Checksums;
    TVector<TBlock> ZeroBlocks;

public:
    TFlushBlocksVisitor(
        ui32 blockSize,
        ui32 flushBlobSizeThreshold,
        ui32 maxBlobRangeSize,
        ui32 maxBlocksInBlob,
        ui64 diskPrefixLengthWithBlockChecksumsInBlobs,
        const TCompactionMap& compactionMap,
        bool readBlockMaskOnCompactionOptimizationEnabled,
        ui64 splitByCompactionRangeMaxBlobCount,
        ui64 tabletId,
        ui64 writeBlobSizeThreshold,
        TVector<TBlob>& blobs);

    bool Visit(const TFreshBlock& block) override;

    void Finish();

private:
    [[nodiscard]] ui8 CalculateCompactionRangeCount(
        const TVector<TBlock>& blocks) const;

    void AppendDataBlob(
        bool hasData,
        TVector<TBlock> blocks,
        TVector<ui32> checksums);

    void FlushZeroBlob(TVector<TBlock> blocks);

    void FlushBlob(
        bool hasData,
        TVector<TBlock> blocks,
        TVector<ui32> checksums);

    static ui32 GetBlobRangeSize(
        const TVector<TBlock>& blocks,
        ui32 blockIndex);
};

// Copies each fresh block into its final owned destination during Visit().
// No source view survives the call; the source index must stay unchanged between
// planning and materialization. Both passes run synchronously in StartFlush.
class TFlushBlocksMaterializer final: public IFreshBlocksIndexVisitor
{
private:
    const ui32 BlockSize;
    TVector<TFlushBlocksVisitor::TBlob>& Blobs;
    size_t BlobIndex = 0;

public:
    TFlushBlocksMaterializer(
        ui32 blockSize,
        TVector<TFlushBlocksVisitor::TBlob>& blobs);

    bool Visit(const TFreshBlock& block) override;
    void Finish() const;

private:
    void SkipZeroBlobs();
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
