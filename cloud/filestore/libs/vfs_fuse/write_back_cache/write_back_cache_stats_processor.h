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

public:
    explicit TStatsProcessor(IWriteBackCacheStatsPtr stats);

    void FlushStarted(TInstant startTime);
    void FlushCompleted(TInstant startTime);
    void FlushFailed();

    void IncrementNodeCount();
    void DecrementNodeCount();

    void UpdatePendingQueueStats(
        const TDeque<std::unique_ptr<TWriteDataEntry>>& pendingEntries);

    void UpdateCachedQueueStats(
        const TDeque<std::unique_ptr<TWriteDataEntry>>& cachedEntries);

    void WriteDataRequestEnteredState(
        IWriteBackCacheStats::EWriteDataRequestState state);

    void WriteDataRequestExitedState(
        IWriteBackCacheStats::EWriteDataRequestState state,
        TDuration duration);

    void AddReadDataStats(
        IWriteBackCacheStats::EReadDataRequestCacheState state,
        TDuration waitDuration);

    // Persistent queue stats should be reported after a call (or a series of
    // calls) to AllocateBack or PopFront
    void UpdatePersistentQueueStats(const TFileRingBuffer& buffer);
};

}   // namespace NCloud::NFileStore::NFuse
