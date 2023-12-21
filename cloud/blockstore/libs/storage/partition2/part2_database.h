#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/compaction_map.h>
#include <cloud/blockstore/libs/storage/partition2/model/blob.h>
#include <cloud/blockstore/libs/storage/partition2/model/block_list.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

class TPartitionDatabase
    : public NKikimr::NIceDb::TNiceDb
{
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
    // FreshBlockUpdates
    //

    bool ReadFreshBlockUpdates(TFreshBlockUpdates& updates);
    void AddFreshBlockUpdate(TFreshBlockUpdate update);
    void TrimFreshBlockUpdates(
        TFreshBlockUpdates::const_iterator first,
        TFreshBlockUpdates::const_iterator last);

    //
    // Blobs
    //

    struct TBlobMeta
    {
        TPartialBlobId BlobId;
        NProto::TBlobMeta2 BlobMeta;

        TBlobMeta() = default;

        TBlobMeta(TPartialBlobId blobId, NProto::TBlobMeta2 blobMeta)
            : BlobId(std::move(blobId))
            , BlobMeta(std::move(blobMeta))
        {
        }
    };

    void WriteGlobalBlob(
        const TPartialBlobId& blobId,
        const NProto::TBlobMeta2& meta);
    void DeleteGlobalBlob(const TPartialBlobId& blobId);
    void WriteZoneBlob(
        ui32 zoneId,
        const TPartialBlobId& blobId,
        const NProto::TBlobMeta2& meta);
    void DeleteZoneBlob(
        ui32 zoneId,
        const TPartialBlobId& blobId);

    bool ReadGlobalBlobs(TVector<TBlobMeta>& blobs);
    bool ReadZoneBlobs(ui32 zoneId, TVector<TBlobMeta>& blobs);

    bool ReadKnownBlobIds(TVector<TPartialBlobId>& blobIds);
    bool ReadAllZoneBlobIds(TVector<TPartialBlobId>& blobIds, ui64 commitId);

    //
    // BlockLists
    //

    void WriteBlockList(const TPartialBlobId& blobId, const TBlockList& blockList);
    void DeleteBlockList(const TPartialBlobId& blobId);

    bool ReadBlockList(const TPartialBlobId& blobId, TMaybe<TBlockList>& blockList);

    //
    // BlobUpdates
    //

    void WriteGlobalBlobUpdate(
        ui64 deletionId,
        ui64 commitId,
        const TBlockRange32& blockRange);
    void DeleteGlobalBlobUpdate(ui64 deletionId);
    void WriteZoneBlobUpdate(
        ui32 zoneId,
        ui64 deletionId,
        ui64 commitId,
        const TBlockRange32& blockRange);
    void DeleteZoneBlobUpdate(
        ui32 zoneId,
        ui64 deletionId);

    bool ReadGlobalBlobUpdates(TVector<TBlobUpdate>& updates);
    bool ReadZoneBlobUpdates(ui32 zoneId, TVector<TBlobUpdate>& updates);

    //
    // BlobGarbage
    //

    struct TBlobGarbage
    {
        TPartialBlobId BlobId;
        ui16 BlockCount = 0;

        TBlobGarbage() = default;

        TBlobGarbage(TPartialBlobId blobId, ui16 blockCount)
            : BlobId(std::move(blobId))
            , BlockCount(blockCount)
        {
        }
    };

    void WriteGlobalBlobGarbage(const TBlobGarbage& garbage);
    void DeleteGlobalBlobGarbage(const TPartialBlobId& blobId);
    void WriteZoneBlobGarbage(
        ui32 zoneId,
        const TBlobGarbage& garbage);
    void DeleteZoneBlobGarbage(
        ui32 zoneId,
        const TPartialBlobId& blobId);

    bool ReadGlobalBlobGarbage(TVector<TBlobGarbage>& garbage);
    bool ReadZoneBlobGarbage(ui32 zoneId, TVector<TBlobGarbage>& garbage);

    //
    // Checkpoints
    //

    void WriteCheckpoint(const NProto::TCheckpointMeta& meta);
    void DeleteCheckpoint(ui64 commitId);

    bool ReadCheckpoint(ui64 commitId, TMaybe<NProto::TCheckpointMeta>& meta);
    bool ReadCheckpoints(TVector<NProto::TCheckpointMeta>& checkpoints);

    //
    // CheckpointBlobs
    //

    struct TCheckpointBlob
    {
        ui64 CommitId;
        TPartialBlobId BlobId;
    };

    void WriteCheckpointBlob(ui64 commitId, const TPartialBlobId& blobId);
    void DeleteCheckpointBlob(ui64 commitId, const TPartialBlobId& blobId);

    bool ReadCheckpointBlobs(TVector<TCheckpointBlob>& blobs);
    bool ReadCheckpointBlobs(ui64 commitId, TVector<TPartialBlobId>& blobIds);

    //
    // CompactionMap
    //

    void WriteCompactionMap(ui32 blockIndex, ui32 blobCount, ui32 blockCount);
    void DeleteCompactionMap(ui32 blockIndex);

    bool ReadCompactionMap(TVector<TCompactionCounter>& compactionMap);

    //
    // GarbageBlobs
    //

    void WriteGarbageBlob(const TPartialBlobId& blobId);
    void DeleteGarbageBlob(const TPartialBlobId& blobId);

    bool ReadGarbageBlobs(TVector<TPartialBlobId>& blobIds);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
