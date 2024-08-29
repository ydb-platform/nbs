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

    void ApplyDeletionMarkers(TVector<TBlock>& blocks) const;
    void ApplyAndUpdateDeletionMarkers(TVector<TBlock>& blocks);

    TVector<TDeletionMarker> ExtractProcessedDeletionMarkers();
};

}   // namespace NCloud::NFileStore::NStorage
