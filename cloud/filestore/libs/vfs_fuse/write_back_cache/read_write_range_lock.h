#pragma once

#include "overlapping_interval_set.h"

#include <functional>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Manages read and write locks on arbitrary ranges, supporting
 * overlapping intervals.
 *
 * Provides mechanism to acquire and release read and write locks on specified
 * ranges. If a lock cannot be acquired immediately due to conflicts,
 * the request is queued and processed when possible. Supports concurrent read
 * and write locks on overlapping ranges, but read locks are exclusive with
 * write locks. Write locks are prioritized over read locks.
 *
 * @note All ranges are specified as [begin, end).
 *
 * @section Usage
 * - Use LockRead() and LockWrite() to request locks.
 * - Provide a callback action that will be invoked when the lock is acquired.
 * - Use UnlockRead() and UnlockWrite() to release locks.
 * - The Empty() method checks if there are no active locks or pending requests.
 */
class TReadWriteRangeLock
{
private:
    TOverlappingIntervalSet ReadLocks;
    TOverlappingIntervalSet WriteLocks;

    struct TPendingLock
    {
        ui64 Begin = 0;
        ui64 End = 0;
        std::function<void()> Action;
        bool IsWrite = false;
    };

    TVector<TPendingLock> PendingLocks;

public:
    // Requests a read lock on the specified non-empty range [begin, end).
    // If the lock can be acquired immediately, 'action' is called.
    // Otherwise, the request is queued.
    void LockRead(ui64 begin, ui64 end, std::function<void()> action);

    // Requests a write lock on the specified non-empty range [begin, end).
    // If the lock can be acquired immediately, 'action' is called.
    // Otherwise, the request is queued.
    void LockWrite(ui64 begin, ui64 end, std::function<void()> action);

    // Releases a read lock on the specified non-empty range [begin, end).
    // The lock must be acquired (it is not possible to cancel pending locks).
    // If there are any pending lock requests that can now be granted as
    // a result of this unlock, they will be processed.
    void UnlockRead(ui64 begin, ui64 end);

    // Releases a write lock on the specified non-empty range [begin, end).
    // The lock must be acquired (it is not possible to cancel pending locks).
    // If there are any pending lock requests that can now be granted as
    // a result of this unlock, they will be processed.
    void UnlockWrite(ui64 begin, ui64 end);

    // Checks if there are no active locks or pending requests
    bool Empty() const;

private:
    void ProcessPendingLocks();
};

}   // namespace NCloud::NFileStore::NFuse
