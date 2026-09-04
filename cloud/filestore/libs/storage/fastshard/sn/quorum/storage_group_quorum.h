#pragma once

#include "storage_group.h"

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Returns an IStorageGroup which mirrors each write but acks it as soon
 * as a majority of them has it. Reads are served by any replica that has caught
 * up to the last quorum-acked state. Write error takes the whole group out
 * of service until it is recreated.
 *
 * @param devices - Storage devices to mirror the data across.
 * @param retryPolicy - Retry policy for storage node requests.
 * @param timer - Time source for the retry deadline checks and backoff
 *                sleeps. Production callers should pass the timer returned
 *                by CreateFiberTimer(). Tests can pass TTestTimer to make
 *                retries deterministic.
 * @return - The constructed group.
 */
IStorageGroupPtr CreateQuorumMirroredStorageGroup(
    TVector<TStorageDevice> devices,
    TStorageGroupRetryPolicy retryPolicy,
    ITimerPtr timer);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
