#pragma once

#include "write_back_cache.h"

#include <cloud/filestore/libs/diagnostics/request_stats.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats
    : public IUpdateableStats
{
    enum class EReadDataRequestCacheStatus;
    struct TPersistentQueueStats;

    virtual void ResetNonDerivativeCounters() = 0;

    virtual void FlushStarted() = 0;
    virtual void FlushCompleted() = 0;
    virtual void FlushFailed() = 0;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;

    virtual void WriteDataRequestEnteredStatus(
        TWriteBackCache::EWriteDataRequestStatus status) = 0;

    virtual void WriteDataRequestExitedStatus(
        TWriteBackCache::EWriteDataRequestStatus status,
        TDuration duration) = 0;

    virtual void WriteDataRequestUpdateMinTime(
        TWriteBackCache::EWriteDataRequestStatus status,
        TInstant minTime) = 0;

    virtual void AddReadDataStats(
        IWriteBackCacheStats::EReadDataRequestCacheStatus status,
        TDuration pendingDuration) = 0;

    virtual void UpdatePersistentQueueStats(
        const TPersistentQueueStats& stats) = 0;
};

////////////////////////////////////////////////////////////////////////////////

enum class IWriteBackCacheStats::EReadDataRequestCacheStatus
{
    // A request wasn't served from the cache
    Miss,

    // A request was partially served from the cache
    PartialHit,

    // A request was fully served from the cache
    FullHit
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats::TPersistentQueueStats
{
    ui64 RawCapacity = 0;
    ui64 RawUsedBytesCount = 0;
    ui64 MaxAllocationBytesCount = 0;
    bool IsCorrupted = false;
};

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateWriteBackCacheStats(
    NMonitoring::TDynamicCounters& counters,
    ITimerPtr timer);

IWriteBackCacheStatsPtr CreateWriteBackCacheStatsStub();

}   // namespace NCloud::NFileStore::NFuse
