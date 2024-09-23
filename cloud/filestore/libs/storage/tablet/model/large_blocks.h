#pragma once

#include "public.h"

#include "block.h"
#include "deletion_markers.h"

#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TLargeBlocks
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TLargeBlocks(IAllocator* allocator);
    ~TLargeBlocks();

    void AddDeletionMarker(TDeletionMarker deletionMarker);

    // applies the deletion markers to the provided blocks
    // returns true if at least one of the blocks was affected
    bool ApplyDeletionMarkers(TVector<TBlock>& blocks) const;
    // marks the corresponding parts of the stored deletion markers as processed
    void MarkProcessed(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount);

    // returns one of the deletion markers which haven't been fully processed
    // yet
    TDeletionMarker GetOne() const;
    // returns deletion markers which are not needed anymore and can be safely
    // deleted
    TVector<TDeletionMarker> ExtractProcessedDeletionMarkers();

    // iterates the deletion subset that satisfies the provided filters
    void FindBlocks(
        ILargeBlockVisitor& visitor,
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount) const;
};

}   // namespace NCloud::NFileStore::NStorage
