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

    // Used to track the earliest Flush start time to calculate MaxFlushTime
    TMultiSet<TInstant> FlushStartTimeSet;
    ui64 FlushWriteDataRequestCount = 0;

public:
    explicit TStatsProcessor(IWriteBackCacheStatsPtr stats);

    void FlushStarted(ui64 writeDataRequestCount, TInstant startTime);
    void FlushCompleted(ui64 writeDataRequestCount, TInstant startTime);
    void FlushFailed();

    void UpdateNodeCount(ui64 value);

    void UpdatePendingQueueStats(
        const TDeque<std::unique_ptr<TWriteDataEntry>>& pendingEntries);

    void UpdateCachedQueueStats(
        const TDeque<std::unique_ptr<TWriteDataEntry>>& cachedEntries);

    void AddWriteDataRequestPendingDuration(TDuration pendingDuration);
    void AddWriteDataRequestWaitingDuration(TDuration waitingDuration);
    void AddWriteDataRequestFlushDuration(TDuration flushDuration);

    // Persistent queue stats should be reported after a call (or a series of
    // calls) to AllocateBack or PopFront
    void UpdatePersistentQueueStats(const TFileRingBuffer& buffer);
};

}   // namespace NCloud::NFileStore::NFuse
