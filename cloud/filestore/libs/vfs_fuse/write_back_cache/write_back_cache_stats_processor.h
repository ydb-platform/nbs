#pragma once

#include "write_back_cache.h"

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>
#include <util/system/types.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TStatsProcessor
{
private:
    const IWriteBackCacheStatsPtr Stats;

    // Used to track the earliest Flush start time to detect hanging Flush
    TMultiSet<TInstant> FlushStartTimeSet;

public:
    explicit TStatsProcessor(IWriteBackCacheStatsPtr stats);

    void FlushStarted(ui64 writeDataRequestCount, TInstant startTime);
    void FlushCompleted(ui64 writeDataRequestCount, TInstant startTime);
    void FlushFailed();

    void UpdateNodeCount(ui64 value);

    void UpdatePendingQueueStatus(
        const TDeque<std::unique_ptr<TWriteDataEntry>>& pendingEntries);

    void UpdateCachedQueueStatus(
        const TDeque<std::unique_ptr<TWriteDataEntry>>& cachedEntries);

    void AddWriteDataRequestPendingTime(TDuration pendingTime);
    void AddWriteDataRequestCachedTime(TDuration cachedTime);
    void AddWriteDataRequestFlushTime(TDuration flushTime);

    // Persistent queue stats should be reported after a call (or a series of
    // calls) to AllocateBack or PopFront
    void UpdatePersistentQueueStats(const TFileRingBuffer& buffer);
};

}   // namespace NCloud::NFileStore::NFuse
