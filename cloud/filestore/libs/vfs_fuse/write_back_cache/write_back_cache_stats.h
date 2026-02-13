#pragma once

#include "persistent_storage.h"

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

/**
 * WriteData request life cycle:
 * Initial -> Pending -> Unflushed -> Flushed
 *
 * For each NodeId it is guaranteed that there are no requests with out-of-order
 * statuses: if two requests A and B have the same NodeId, and the request A was
 * added to the queue later than B, then A.Status <= B.Status.
 */

enum class EWriteDataRequestStatus
{
    // The object has just been created and does not hold a request.
    Initial,

    // Restoration from the persistent buffer was failed.
    // The request will not be processed further.
    Corrupted,

    // Write request is waiting until there is enough space in the persistent
    // storage to store the request.
    Pending,

    // Write request has been stored in the persistent storage and is waiting
    // for flushing
    Unflushed,

    // Write request has been written to the session and can be removed from
    // the persistent storage
    Flushed
};

////////////////////////////////////////////////////////////////////////////////

enum class EReadDataRequestCacheStatus
{
    // A request wasn't served from the cache
    Miss,

    // A request was partially served from the cache
    PartialHit,

    // A request was fully served from the cache
    FullHit
};

////////////////////////////////////////////////////////////////////////////////

struct IWriteBackCacheStats: public IPersistentStorageStats
{
    ~IWriteBackCacheStats() override = default;

    virtual void ResetNonDerivativeCounters() = 0;

    virtual void FlushStarted() = 0;
    virtual void FlushCompleted() = 0;
    virtual void FlushFailed() = 0;

    virtual void IncrementNodeCount() = 0;
    virtual void DecrementNodeCount() = 0;

    virtual void WriteDataRequestEnteredStatus(
        EWriteDataRequestStatus status) = 0;

    virtual void WriteDataRequestExitedStatus(
        EWriteDataRequestStatus status,
        TDuration duration) = 0;

    virtual void WriteDataRequestUpdateMinTime(
        EWriteDataRequestStatus status,
        TInstant minTime) = 0;

    virtual void AddReadDataStats(EReadDataRequestCacheStatus status) = 0;
};

using IWriteBackCacheStatsPtr = std::shared_ptr<IWriteBackCacheStats>;

IWriteBackCacheStatsPtr CreateDummyWriteBackCacheStats();

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
