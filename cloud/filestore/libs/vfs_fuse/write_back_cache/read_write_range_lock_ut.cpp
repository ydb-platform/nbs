#include "read_write_range_lock.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFileStore::NFuse {

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

        auto readLockIncrementer = [&]() {
            readLockCount++;
        };

        auto writeLockIncrementer = [&]() {
            writeLockCount++;
        };

        rangeLock.LockRead(0, 10, readLockIncrementer);
        rangeLock.LockRead(0, 10, readLockIncrementer);
        rangeLock.LockRead(5, 15, readLockIncrementer);

        UNIT_ASSERT_EQUAL(3, readLockCount);

        rangeLock.LockWrite(20, 30, writeLockIncrementer);
        rangeLock.LockWrite(20, 30, writeLockIncrementer);
        rangeLock.LockWrite(25, 35, writeLockIncrementer);

        UNIT_ASSERT_EQUAL(3, writeLockCount);
    }

    Y_UNIT_TEST(OverlappingReadWriteReadLocks)
    {
        TReadWriteRangeLock rangeLock;

        UNIT_ASSERT(rangeLock.Empty());

        // Obtain read lock for [0, 10)
        bool readLock1 = false;
        rangeLock.LockRead(0, 10, [&]() {
            readLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock1);

        // Try to obtain write lock for [0, 10)
        // It will become pending because of the read lock
        bool writeLock1 = false;
        rangeLock.LockWrite(0, 10, [&]() {
            writeLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!writeLock1);

        // Try to obtain read lock for [0, 10)
        // It will become pending because of the pending write lock
        bool readLock2 = false;
        rangeLock.LockRead(0, 10, [&]() {
            readLock2 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!readLock2);

        // Unlock the first read lock - write lock will be obtained
        rangeLock.UnlockRead(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(writeLock1);
        UNIT_ASSERT(!readLock2);

        // Unlock the write lock - read lock will be obtained
        rangeLock.UnlockWrite(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock2);

        // Unlock the last lock
        rangeLock.UnlockRead(0, 10);
        UNIT_ASSERT(rangeLock.Empty());
    }

    Y_UNIT_TEST(OverlappingWriteReadWriteLocks)
    {
        TReadWriteRangeLock rangeLock;

        UNIT_ASSERT(rangeLock.Empty());

        // Obtain write lock for [0, 10)
        bool writeLock1 = false;
        rangeLock.LockWrite(0, 10, [&]() {
            writeLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(writeLock1);

        // Try to obtain read lock for [0, 10)
        // It will become pending because of the write lock
        bool readLock1 = false;
        rangeLock.LockRead(0, 10, [&]() {
            readLock1 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!readLock1);

        // Obtain write lock for [0, 10)
        // It is possible because write request have priority over read requests
        bool writeLock2 = false;
        rangeLock.LockWrite(0, 10, [&]() {
            writeLock2 = true;
        });

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(writeLock2);

        // Unlock the first write lock
        rangeLock.UnlockWrite(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(!readLock1);

        // Unlock the second write lock - read lock will be obtained
        rangeLock.UnlockWrite(0, 10);

        UNIT_ASSERT(!rangeLock.Empty());
        UNIT_ASSERT(readLock1);

        // Unlock the last lock
        rangeLock.UnlockRead(0, 10);
        UNIT_ASSERT(rangeLock.Empty());
    }

    Y_UNIT_TEST(PrioritizeWriteOverRead)
    {
        TReadWriteRangeLock rangeLock;

        UNIT_ASSERT(rangeLock.Empty());

        // Obtain read lock for [0, 20)
        rangeLock.LockRead(0, 20, [&](){});

        // Try to obtain write lock for [0, 10)
        bool writeLock1 = false;
        rangeLock.LockWrite(5, 15, [&]() {
            writeLock1 = true;
        });

        // Try to obtain read lock for [5, 15)
        bool readLock1 = false;
        rangeLock.LockRead(5, 15, [&]() {
            readLock1 = true;
        });

        // Try to obtain write lock for [10, 20)
        bool writeLock2 = false;
        rangeLock.LockWrite(10, 20, [&]() {
            writeLock2 = true;
        });

        UNIT_ASSERT(!readLock1);
        UNIT_ASSERT(!writeLock1);
        UNIT_ASSERT(!writeLock2);

        // Release read lock for [0, 20)
        // Only write locks should proceed
        rangeLock.UnlockRead(0, 20);

        UNIT_ASSERT(!readLock1);
        UNIT_ASSERT(writeLock1);
        UNIT_ASSERT(writeLock2);
    }
}

}   // namespace NCloud::NFileStore::NFuse
