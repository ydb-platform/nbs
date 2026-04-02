#pragma once

#include <util/datetime/base.h>

#include <memory>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

/**
 * WriteData request life cycle:
 * [Pending] -> Unflushed -> Flushed
 *
 * For each NodeId it is guaranteed that there are no requests with out-of-order
 * statuses: if two requests A and B have the same NodeId, and the request A was
 * added to the queue later than B, then A.Status <= B.Status.
 */

enum class EWriteDataRequestStatus
{
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

struct IWriteDataRequestManagerStats
{
    virtual ~IWriteDataRequestManagerStats() = default;

    virtual void WriteDataRequestEnteredStatus(
        EWriteDataRequestStatus status) = 0;

    virtual void WriteDataRequestExitedStatus(
        EWriteDataRequestStatus status,
        TDuration duration) = 0;

    virtual void WriteDataRequestUpdateMinTime(
        EWriteDataRequestStatus status,
        TInstant minTime) = 0;
};

using IWriteDataRequestManagerStatsPtr =
    std::shared_ptr<IWriteDataRequestManagerStats>;

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
