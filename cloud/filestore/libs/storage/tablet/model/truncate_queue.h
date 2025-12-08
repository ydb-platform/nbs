#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/byte_range.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TTruncateQueue
{
public:
    struct TEntry {
        ui64 NodeId = 0;
        TByteRange Range;
    };

private:
    TVector<TEntry> PendingOps;
    THashSet<ui64> ActiveOps;

public:
    void EnqueueOperation(ui64 nodeId, TByteRange range);
    TEntry DequeueOperation();
    bool HasPendingOperations() const;

    void CompleteOperation(ui64 nodeId);
    bool HasActiveOperation(ui64 nodeId) const;
};

}   // namespace NCloud::NFileStore::NStorage
