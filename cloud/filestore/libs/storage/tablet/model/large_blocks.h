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
    void ApplyDeletionMarkers(TVector<TBlock>& blocks) const;
    // applies the deletion markers to the provided blocks AND marks the
    // corresponding parts of the deletion markers as processed
    void ApplyAndUpdateDeletionMarkers(TVector<TBlock>& blocks);

    // returns one of the deletion markers which haven't been fully processed
    // yet - no assumptions should be made regarding which marker it is
    TDeletionMarker GetOne() const;
    // returns deletion markers which are not needed anymore and can be safely
    // deleted
    TVector<TDeletionMarker> ExtractProcessedDeletionMarkers();
};

}   // namespace NCloud::NFileStore::NStorage
