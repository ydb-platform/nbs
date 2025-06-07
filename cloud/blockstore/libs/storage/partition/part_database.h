#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/compaction_map.h>
#include <cloud/blockstore/libs/storage/partition/model/blob_index.h>
#include <cloud/blockstore/libs/storage/partition/model/blob_to_confirm.h>
#include <cloud/blockstore/libs/storage/partition/model/block.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>
#include <cloud/blockstore/libs/storage/partition/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition/model/cleanup_queue.h>
#include <cloud/blockstore/libs/storage/partition/model/mixed_index_cache.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <contrib/ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TPartitionDatabase
    : public NKikimr::NIceDb::TNiceDb
{
public:
    enum class EBlobIndexScanProgress
    {
        NotReady,
        Completed,
        Partial
    };

public:
    TPartitionDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();

    //
    // Meta
    //

    void WriteMeta(const NProto::TPartitionMeta& meta);
    bool ReadMeta(TMaybe<NProto::TPartitionMeta>& meta);

    //
    // FreshBlocksIndex
    //

    void WriteFreshBlock(
        ui32 blockIndex,
        ui64 commitId,
        TBlockDataRef blockContent);

    void DeleteFreshBlock(ui32 blockIndex, ui64 commitId);

    bool ReadFreshBlocks(TVector<TOwningFreshBlock>& blocks);

    //
    // MixedBlocksIndex
    //

    void WriteMixedBlock(TMixedBlock block);

    void WriteMixedBlocks(
        const TPartialBlobId& blobId,
        const TVector<ui32>& blocks);

    void DeleteMixedBlock(ui32 blockIndex, ui64 commitId);

    bool FindMixedBlocks(
        IBlocksIndexVisitor& visitor,
        const TBlockRange32& readRange,
        bool precharge,
        ui64 maxCommitId = Max());

    bool FindMixedBlocks(
        IBlocksIndexVisitor& visitor,
        const TVector<ui32>& blocks,
        ui64 maxCommitId = Max());

    //
    // MergedBlocksIndex
    //

    void WriteMergedBlocks(
        const TPartialBlobId& blobId,
        const TBlockRange32& blockRange,
        const TBlockMask& skipMask);

    void DeleteMergedBlocks(
        const TPartialBlobId& blobId,
        const TBlockRange32& blockRange);

    bool FindMergedBlocks(
        IBlocksIndexVisitor& visitor,
        const TBlockRange32& readRange,
        bool precharge,
        ui32 maxBlocksInBlob,
        ui64 maxCommitId = Max());

    bool FindMergedBlocks(
        IBlocksIndexVisitor& visitor,
        const TVector<ui32>& blocks,
        ui32 maxBlocksInBlob,
        ui64 maxCommitId = Max());

    //
    // BlobsIndex
    //

    void WriteBlobMeta(
        const TPartialBlobId& blobId,
        const NProto::TBlobMeta& blobMeta);

    void DeleteBlobMeta(const TPartialBlobId& blobId);

    bool ReadBlobMeta(
        const TPartialBlobId& blobId,
        TMaybe<NProto::TBlobMeta>& blobMeta);

    bool ReadNewBlobs(
        TVector<TPartialBlobId>& blobIds,
        ui64 minCommitId = 0);

    void WriteBlockMask(
        const TPartialBlobId& blobId,
        const TBlockMask& blockMask);

    bool ReadBlockMask(
        const TPartialBlobId& blobId,
        TMaybe<TBlockMask>& blockMask);

    bool FindBlocksInBlobsIndex(
        IExtendedBlocksIndexVisitor& visitor,
        const ui32 maxBlocksInBlob,
        const TBlockRange32& blockRange);

    bool FindBlocksInBlobsIndex(
        IExtendedBlocksIndexVisitor& visitor,
        const ui32 maxBlocksInBlob,
        const TPartialBlobId& blobId);

    EBlobIndexScanProgress FindBlocksInBlobsIndex(
        IBlobsIndexVisitor& visitor,
        TPartialBlobId startBlobId,
        TPartialBlobId finalBlobId,
        ui64 prechargeRowCount);

    //
    // CompactionMap
    //

    void WriteCompactionMap(ui32 blockIndex, ui32 blobCount, ui32 blockCount);
    void DeleteCompactionMap(ui32 blockIndex);

    bool ReadCompactionMap(TVector<TCompactionCounter>& compactionMap);
    bool ReadCompactionMap(
        TBlockRange32 rangeBlockIndices,
        TVector<TCompactionCounter>& compactionMap);

    //
    // UsedBlocks
    //

    void WriteUsedBlocks(const TCompressedBitmap::TSerializedChunk& chunk);
    void WriteLogicalUsedBlocks(const TCompressedBitmap::TSerializedChunk& chunk);

    bool ReadUsedBlocks(TCompressedBitmap& usedBlocks);
    bool ReadLogicalUsedBlocks(TCompressedBitmap& usedBlocks, bool& read);

    bool ReadUsedBlocksRaw(std::function<void(TCompressedBitmap::TSerializedChunk)> onChunk);

    //
    // Checkpoints
    //

    void WriteCheckpoint(const TCheckpoint& checkpoint, bool withoutData);

    void DeleteCheckpoint(const TString& checkpointId, bool deleteOnlyData);

    bool ReadCheckpoints(
        TVector<TCheckpoint>& checkpoints,
        THashMap<TString, ui64>& checkpointId2CommitId);

    //
    // CleanupQueue
    //

    void WriteCleanupQueue(const TPartialBlobId& blobId, ui64 commitId);
    void DeleteCleanupQueue(const TPartialBlobId& blobId, ui64 commitId);

    bool ReadCleanupQueue(TVector<TCleanupQueueItem>& items);

    //
    // GarbageBlobs
    //

    void WriteGarbageBlob(const TPartialBlobId& blobId);
    void DeleteGarbageBlob(const TPartialBlobId& blobId);

    bool ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds);

    //
    // UnconfirmedBlobs
    //

    void WriteUnconfirmedBlob(
        const TPartialBlobId& blobId,
        const TBlobToConfirm& blob);
    void DeleteUnconfirmedBlob(const TPartialBlobId& blobId);

    bool ReadUnconfirmedBlobs(TCommitIdToBlobsToConfirm& blobs);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
