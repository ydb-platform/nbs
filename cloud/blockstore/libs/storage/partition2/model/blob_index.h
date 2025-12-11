#pragma once

#include "public.h"

#include "blob.h"
#include "block.h"
#include "block_list.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

using TBlobCounter = std::pair<TPartialBlobId, size_t>;

template <typename T1, typename T2>
TVector<T1> SelectFirst(const TVector<std::pair<T1, T2>>& items)
{
    TVector<T1> result(Reserve(items.size()));
    for (const auto& kv: items) {
        result.push_back(kv.first);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 InvalidZoneId = Max<ui32>();
constexpr ui32 GlobalZoneId = Max<ui32>() - 1;

////////////////////////////////////////////////////////////////////////////////

struct TAddedBlobInfo
{
    bool Added;
    ui32 ZoneId;

    TAddedBlobInfo(bool added, ui32 zoneId)
        : Added(added)
        , ZoneId(zoneId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TMutableFoundBlobInfo
{
    TBlob* Blob;
    ui32 ZoneId;

    TMutableFoundBlobInfo(TBlob* blob, ui32 zoneId)
        : Blob(blob)
        , ZoneId(zoneId)
    {}
};

struct TFoundBlobInfo
{
    const TBlob* Blob;
    ui32 ZoneId;

    TFoundBlobInfo(const TBlob* blob, ui32 zoneId)
        : Blob(blob)
        , ZoneId(zoneId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TDeletionsInfo
{
    TVector<TBlobUpdate> BlobUpdates;
    ui32 ZoneId = GlobalZoneId;
};

////////////////////////////////////////////////////////////////////////////////

struct TGarbageInfo
{
    TVector<TBlobCounter> BlobCounters;
    ui32 ZoneId = GlobalZoneId;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobIndex
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TBlobIndex(
        ui32 zoneCount,
        ui32 zoneBlockCount,
        ui32 blockListCacheSize,
        ui32 maxBlocksInBlob,
        EOptimizationMode optimizationMode);
    ~TBlobIndex();

    //
    // Blobs
    //

    size_t GetGlobalBlobCount() const;
    size_t GetZoneBlobCount() const;
    size_t GetRangeCount() const;
    size_t GetBlockCount() const;

    TAddedBlobInfo AddBlob(
        const TPartialBlobId& blobId,
        const TBlockRanges& blockRanges,
        ui16 blockCount,
        ui16 checkpointBlockCount);

    bool RemoveBlob(ui32 zoneHint, const TPartialBlobId& blobId);

    TMutableFoundBlobInfo AccessBlob(
        ui32 zoneHint,
        const TPartialBlobId& blobId);
    TFoundBlobInfo FindBlob(ui32 zoneHint, const TPartialBlobId& blobId) const;

    TVector<const TBlob*> FindBlobs(const TBlockRange32& blockRange) const;

    //
    // MixedIndex
    //

    void WriteOrUpdateMixedBlock(const TBlockAndLocation& b);

    TVector<TBlockAndLocation> FindAllMixedBlocks(
        const TBlockRange32& blockRange) const;
    TVector<TBlockAndLocation> FindMixedBlocks(
        const TBlockRange32& blockRange,
        ui64 commitId) const;

    //
    // BlockLists
    //

    size_t GetBlockListCount() const;
    size_t GetBlockListsWeight() const;

    bool AddBlockList(
        ui32 zoneHint,
        const TPartialBlobId& blobId,
        const TBlockList& blocks,
        ui16 blockCount);
    void SetPruneBlockListCache(bool prune);
    void PruneStep();

    const TBlockList* FindBlockList(
        ui32 zoneHint,
        const TPartialBlobId& blobId) const;

    //
    // GarbageBlocks
    //

    size_t GetGarbageBlockCount() const;

    bool
    AddGarbage(ui32 zoneHint, const TPartialBlobId& blobId, ui16 blockCount);

    size_t FindGarbage(ui32 zoneHint, const TPartialBlobId& blobId) const;

    TGarbageInfo GetTopGarbage(size_t maxBlobs, size_t maxBlocks) const;

    //
    // DeletedBlocks
    //

    ui32 MarkBlocksDeleted(TBlobUpdate blobUpdate);

    ui32 ApplyUpdates(
        const TBlockRange32& blockRange,
        TVector<TBlock>& blocks) const;

    TVector<TDeletedBlock> FindDeletedBlocks(
        const TBlockRange32& blockRange,
        ui64 maxCommitId = InvalidCommitId) const;

    size_t GetPendingUpdates() const;
    ui32 SelectZoneToCleanup();
    void SeparateChunkForCleanup(ui64 commitId);
    TVector<TPartialBlobId> ExtractDirtyBlobs();
    TDeletionsInfo CleanupDirtyRanges();
    void OnCheckpoint(ui64 checkpointId);
    void OnCheckpoint(ui32 z, ui64 checkpointId);
    void OnCheckpointDeletion(ui64 checkpointId);

    //
    // Zones
    //

    ui32 GetZoneCount() const;
    ui32 GetMixedZoneCount() const;
    std::pair<ui32, ui32> ToZoneRange(const TBlockRange32& blockRange) const;
    TBlockRange32 ToBlockRange(ui32 z) const;
    bool IsGlobalZone(ui32 z) const;

    void FinishGlobalDataInitialization();
    void InitializeZone(ui32 z);
    void ResetZone(ui32 z);
    bool IsZoneInitialized(ui32 z) const;
    bool IsMixedIndex(ui32 z) const;

    TVector<const TBlob*> FindZoneBlobs(ui32 z) const;

    // Applies time-related discount and fetches zone usage score (discounted
    // request count)
    double UpdateAndGetZoneUsageScore(ui32 z, TInstant now);
    // Applies time-related discount and fetches total usage score (discounted
    // request count)
    double UpdateAndGetTotalUsageScore(TInstant now);
    // Converts range map + block lists to mixed index for this zone
    // All block lists for this zone should be added before this call
    void ConvertToMixedIndex(ui32 z, const TVector<ui64>& checkpointIds);
    // Drops mixed index for this zone
    void ConvertToRangeMap(ui32 z);

private:
    TImpl& GetImpl();
    const TImpl& GetImpl() const;
};

////////////////////////////////////////////////////////////////////////////////

TBlockRanges BuildRanges(const TVector<TBlock>& blocks, ui32 maxRanges);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
