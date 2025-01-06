#pragma once

#include "public.h"

#include "blob.h"
#include "block.h"
#include "block_list.h"
#include "deletion_markers.h"

#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMixedBlocks
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TMixedBlocks(IAllocator* allocator);
    ~TMixedBlocks();

    void Reset(ui64 offloadedRangesCapacity);

    bool IsLoaded(ui32 rangeId) const;

    void RefRange(ui32 rangeId);
    void UnRefRange(ui32 rangeId);

    bool AddBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        TBlockList blockList,
        const TMixedBlobStats& stats = {});

    bool RemoveBlocks(
        ui32 rangeId,
        const TPartialBlobId& blobId,
        TMixedBlobStats* stats = nullptr);

    void FindBlocks(
        IMixedBlockVisitor& visitor,
        ui32 rangeId,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;

    void AddDeletionMarker(ui32 rangeId, TDeletionMarker deletionMarker);

    TVector<TDeletionMarker> ExtractDeletionMarkers(ui32 rangeId);

    void ApplyDeletionMarkers(
        const IBlockLocation2RangeIndex& hasher,
        TVector<TBlock>& blocks) const;

    TVector<TMixedBlobMeta> ApplyDeletionMarkers(ui32 rangeId) const;

    struct TDeletionMarkerApplicationResult
    {
        TMixedBlobMeta BlobMeta;
        bool Affected = false;
    };

    // returns metas for all blobs belonging to this range
    TVector<TDeletionMarkerApplicationResult> ApplyDeletionMarkersAndGetMetas(
        ui32 rangeId) const;

    TVector<TMixedBlobMeta> GetBlobsForCompaction(ui32 rangeId) const;

    TMixedBlobMeta FindBlob(ui32 rangeId, TPartialBlobId blobId) const;

    ui32 CalculateGarbageBlockCount(ui32 rangeId) const;
};

}   // namespace NCloud::NFileStore::NStorage
