#include "read_write_range_lock.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadWriteRangeLockTest)
{
    Y_UNIT_TEST(SimpleLockAndUnlock)
    {
        TReadWriteRangeLock rangeLock;

        UNIT_ASSERT(rangeLock.Empty());

        // Test read lock
        bool readLock = false;
        rangeLock.LockRead(0, 10, [&]() {
            readLock = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock);

        rangeLock.UnlockRead(0, 10);

        UNIT_ASSERT(rangeLock.Empty());

        // Test write lock
        bool writeLock = false;
        rangeLock.LockWrite(0, 10, [&]() {
            writeLock = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock);

        rangeLock.UnlockWrite(0, 10);

        UNIT_ASSERT(rangeLock.Empty());
    }

    Y_UNIT_TEST(OverlappingNonInterferringLocks)
    {
        TReadWriteRangeLock rangeLock;

        int readLockCount = 0;
        int writeLockCount = 0;

        auto readLockAction = [&]() {
            readLockCount++;
        };

        auto writeLockAction = [&]() {
            writeLockCount++;
        };

        rangeLock.LockRead(0, 10, readLockAction);
        rangeLock.LockRead(0, 10, readLockAction);
        rangeLock.LockRead(5, 15, readLockAction);

        UNIT_ASSERT_EQUAL(3, readLockCount);

        rangeLock.LockWrite(20, 30, writeLockAction);
        rangeLock.LockWrite(20, 30, writeLockAction);
        rangeLock.LockWrite(25, 35, writeLockAction);

        UNIT_ASSERT_EQUAL(3, writeLockCount);
    }

    Y_UNIT_TEST(OverlappingReadWriteReadLocks)
    {
        TReadWriteRangeLock rangeLock;

        UNIT_ASSERT(rangeLock.Empty());

        // Obtaining read lock for [0, 10) is done immediately
        // RangeLock status:
        // - read locks: [0, 10)
        bool readLock1 = false;
        rangeLock.LockRead(0, 10, [&]() {
            readLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock1);

        // Obtaining second read lock for [0, 10) is done immediately
        // RangeLock status:
        // - read locks: [0, 10), [0, 10)
        readLock1 = false;
        rangeLock.LockRead(0, 10, [&]() {
            readLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock1);

        // A request to obtain write lock for [5, 15) becomes pending because
        // it intersects with the existing read lock [0, 10)
        // RangeLock status:
        // - read locks: [0, 10), [0, 10)
        // - pending write locks: [5, 15)
        bool writeLock1 = false;
        rangeLock.LockWrite(5, 15, [&]() {
            writeLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!writeLock1);

        // A request to obtain read lock for [10, 20) becomes pending because
        // it intersects with the pending write lock [5, 15).
        // Pending write locks affect read lock acquision because write
        // lock requests have priority over read lock requests.
        // RangeLock status:
        // - read locks: [0, 10), [0, 10)
        // - pending read locks: [10, 20)
        // - pending write locks: [5, 15)
        bool readLock2 = false;
        rangeLock.LockRead(10, 20, [&]() {
            readLock2 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!readLock2);

        // Unlocking the first read lock [0, 10) will trigger nothing because
        // there is second lock at the same interval
        // RangeLock status:
        // - read locks: [0, 10)
        // - pending read locks: [10, 20)
        // - pending write locks: [5, 15)
        rangeLock.UnlockRead(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!writeLock1);
        UNIT_ASSERT(!readLock2);

        // Unlocking read lock [0, 10) will trigger write lock acquisition
        // RangeLock status:
        // - write locks: [5, 15)
        // - pending read locks: [10, 20)
        rangeLock.UnlockRead(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(writeLock1);
        UNIT_ASSERT(!readLock2);

        // Unlocking write lock [5, 15) will trigger read lock acquisition
        // RangeLock status:
        // - read locks: [10, 20)
        rangeLock.UnlockWrite(5, 15);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock2);

        // Unlock the last lock
        rangeLock.UnlockRead(10, 20);
        UNIT_ASSERT(rangeLock.Empty());
    }

    Y_UNIT_TEST(OverlappingWriteReadWriteLocks)
    {
        TReadWriteRangeLock rangeLock;

        UNIT_ASSERT(rangeLock.Empty());

        // Obtaining write lock for [0, 10) is done immediately
        // RangeLock status:
        // - write locks: [0, 10)
        bool writeLock1 = false;
        rangeLock.LockWrite(0, 10, [&]() {
            writeLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(writeLock1);

        // A request to obtain read lock for [5, 15) becomes pending because
        // it intersects with the existing wrote lock [0, 10)
        // RangeLock status:
        // - write locks: [0, 10)
        // - pending read locks: [5, 15)
        bool readLock1 = false;
        rangeLock.LockRead(5, 15, [&]() {
            readLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!readLock1);

        // Obtaining another write lock for [0, 10) is done immediately
        // because there are no intersecting read locks.
        // Pending read locks do not affect write lock acquision because write
        // lock requests have priority over read lock requests.
        // It is also allowed to have overlapping write locks.
        // RangeLock status:
        // - write locks: [0, 10), [0, 10)
        // - pending read locks: [5, 15)
        bool writeLock2 = false;
        rangeLock.LockWrite(0, 10, [&]() {
            writeLock2 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(writeLock2);

        // Unlocking the first write lock [0, 10) will trigger nothing because
        // there is second lock at the same interval
        // RangeLock status:
        // - write locks: [0, 10)
        // - pending read locks: [5, 15)
        rangeLock.UnlockWrite(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!readLock1);

        // Unlocking write lock [0, 10) will trigger read lock acquisition
        // RangeLock status:
        // - read locks: [5, 15)
        rangeLock.UnlockWrite(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock1);

        // Unlock the last lock
        rangeLock.UnlockRead(5, 15);
        UNIT_ASSERT(rangeLock.Empty());
    }
}

}   // namespace NCloud::NFileStore::NFuse
