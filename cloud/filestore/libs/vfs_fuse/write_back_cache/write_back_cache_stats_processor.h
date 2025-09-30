#pragma once

#include "write_back_cache.h"

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>
#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheStatsProcessor
{
private:
    const IWriteBackCacheStatsPtr Stats;

    // Used to track the earliest Flush start time to detect hanging Flush
    TMultiSet<TInstant> FlushStartTimeSet;

public:
    explicit TWriteBackCacheStatsProcessor(IWriteBackCacheStatsPtr stats);

    void FlushScheduled(TInstant startTime);
    void FlushCompleted(TInstant startTime);
    void FlushFailed();

    // Persistent queue stats should be reported after a call (or a series of
    // calls) to AllocateBack or PopFront
    void UpdatePersistentQueueStats(const TFileRingBuffer& buffer);

    void UpdateNodeCount(ui64 value);
    void UpdateCachedRequestCount(ui64 value);
    void UpdatePendingRequestCount(ui64 value);

    void AddWriteDataRequestStats(
        TInstant pendingTime,
        TInstant cachedTime,
        TInstant startFlushTime,
        TInstant completeFlushTime) const;
};

}   // namespace NCloud::NFileStore::NFuse
