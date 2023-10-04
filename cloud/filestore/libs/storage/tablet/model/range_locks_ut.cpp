#include "range_locks.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRangeLocksTest)
{
    static const TString Session1 = "session1";
    static const TString Session2 = "session2";
    static const TString Session3 = "session3";

    static const ui64 Owner1 = 1;
    static const ui64 Owner2 = 2;
    static const ui64 Owner3 = 3;

    static const ui64 Lock1 = 1;
    static const ui64 Lock2 = 2;
    static const ui64 Lock3 = 3;

    static const ui64 NodeId = 1;

    Y_UNIT_TEST(ShouldKeepTrackOfLocks)
    {
        TRangeLocks locks;

        // first lock
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 300,
            };

            TVector<ui64> removedLocks;
            bool acquired = locks.Acquire(
                Session1,
                Lock1,
                range,
                ELockMode::Exclusive,
                removedLocks);

            UNIT_ASSERT(acquired);
            UNIT_ASSERT(removedLocks.empty());
        }

        // can't lock subrange
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner2,
                .Offset = 100,
                .Length = 100,
            };

            TLockRange conflicting;
            bool couldBeAcquired = locks.Test(
                Session2,
                range,
                ELockMode::Exclusive,
                &conflicting);

            UNIT_ASSERT(!couldBeAcquired);
            UNIT_ASSERT_VALUES_EQUAL(conflicting.OwnerId, Owner1);
            UNIT_ASSERT_VALUES_EQUAL(conflicting.NodeId, NodeId);
            UNIT_ASSERT_VALUES_EQUAL(conflicting.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(conflicting.Length, 300);
        }

        // removing subrange from Lock1, Lock1's range gets split into 2 ranges
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 100,
                .Length = 100,
            };

            TVector<ui64> removedLocks;
            locks.Release(
                Session1,
                range,
                removedLocks);

            UNIT_ASSERT(removedLocks.empty());
        }

        // now Lock2 can be acquired
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner2,
                .Offset = 100,
                .Length = 100,
            };

            TVector<ui64> removedLocks;
            bool acquired = locks.Acquire(
                Session2,
                Lock2,
                range,
                ELockMode::Exclusive,
                removedLocks);

            UNIT_ASSERT(acquired);
        }

        // attempt to lock file till the end of it should fail
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner3,
                .Offset = 100,
                .Length = 0,
            };

            TLockRange conflicting;
            bool couldBeAcquired = locks.Test(
                Session3,
                range,
                ELockMode::Exclusive,
                &conflicting);

            UNIT_ASSERT(!couldBeAcquired);
        }

        // releasing Lock2
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner2,
                .Offset = 100,
                .Length = 100,
            };

            TVector<ui64> removedLocks;
            locks.Release(
                Session2,
                range,
                removedLocks);

            UNIT_ASSERT_VALUES_EQUAL(1, removedLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(Lock2, removedLocks[0]);
        }

        // attempt to lock file till the end of it should still fail
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner3,
                .Offset = 100,
                .Length = 0,
            };

            TLockRange conflicting;
            bool couldBeAcquired = locks.Test(
                Session3,
                range,
                ELockMode::Exclusive,
                &conflicting);

            UNIT_ASSERT(!couldBeAcquired);
        }

        // releasing Lock1 completely
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 300,
            };

            TVector<ui64> removedLocks;
            locks.Release(
                Session1,
                range,
                removedLocks);

            UNIT_ASSERT_VALUES_EQUAL(1, removedLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(Lock1, removedLocks[0]);
        }

        // attempt to lock file till the end of it should succeed
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner3,
                .Offset = 100,
                .Length = 0,
            };

            TLockRange conflicting;
            bool couldBeAcquired = locks.Test(
                Session3,
                range,
                ELockMode::Exclusive,
                &conflicting);

            UNIT_ASSERT(couldBeAcquired);
        }

        // locking file till the end of it
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner3,
                .Offset = 100,
                .Length = 0,
            };

            TVector<ui64> removedLocks;
            bool acquired = locks.Acquire(
                Session3,
                Lock3,
                range,
                ELockMode::Exclusive,
                removedLocks);

            UNIT_ASSERT(acquired);
            UNIT_ASSERT(removedLocks.empty());
        }

        // attempt to lock subrange after offset 100 should fail
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 300,
                .Length = 10,
            };

            TLockRange conflicting;
            bool couldBeAcquired = locks.Test(
                Session1,
                range,
                ELockMode::Exclusive,
                &conflicting);

            UNIT_ASSERT(!couldBeAcquired);
        }

        // but we can still lock ranges before offset 100
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 100,
            };

            TLockRange conflicting;
            bool couldBeAcquired = locks.Test(
                Session1,
                range,
                ELockMode::Exclusive,
                &conflicting);

            UNIT_ASSERT(couldBeAcquired);
        }
    }

    Y_UNIT_TEST(ShouldReplaceOwnedLocks)
    {
        TRangeLocks locks;

        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 100,
            };

            TVector<ui64> removedLocks;
            bool acquired = locks.Acquire(
                Session1,
                Lock1,
                range,
                ELockMode::Exclusive,
                removedLocks);

            UNIT_ASSERT(acquired);
            UNIT_ASSERT(removedLocks.empty());
        }

        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 100,
                .Length = 100,
            };

            TVector<ui64> removedLocks;
            bool acquired = locks.Acquire(
                Session1,
                Lock2,
                range,
                ELockMode::Exclusive,
                removedLocks);

            UNIT_ASSERT(acquired);
            UNIT_ASSERT(removedLocks.empty());
        }

        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 200,
            };
            TVector<ui64> removedLocks;
            bool acquired = locks.Acquire(
                Session1,
                Lock3,
                range,
                ELockMode::Exclusive,
                removedLocks);

            UNIT_ASSERT(acquired);
            UNIT_ASSERT(removedLocks.size() == 2);
            UNIT_ASSERT(removedLocks[0] = Lock1);
            UNIT_ASSERT(removedLocks[1] = Lock2);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
