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

        // first exclusive lock
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 300,
                .LockMode = ELockMode::Exclusive,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Acquire(Session1, Lock1, range);
            UNIT_ASSERT(result.Succeeded());
            UNIT_ASSERT(result.RemovedLockIds().empty());
        }

        // can't lock subrange
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner2,
                .Offset = 100,
                .Length = 100,
                .LockMode = ELockMode::Shared,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Test(Session2, range);
            UNIT_ASSERT(result.Failed());
            UNIT_ASSERT(result.IncompatibleHolds<TLockRange>());

            const auto& conflicting = result.IncompatibleAs<TLockRange>();
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
                .LockMode = ELockMode::Exclusive,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Release(Session1, range);
            UNIT_ASSERT(result.Succeeded());
            UNIT_ASSERT(result.RemovedLockIds().empty());
        }

        // now Lock2 can be acquired
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner2,
                .Offset = 100,
                .Length = 100,
                .LockMode = ELockMode::Shared,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Acquire(Session2, Lock2, range);
            UNIT_ASSERT(result.Succeeded());
        }

        // attempt to lock file till the end of it should fail
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner3,
                .Offset = 100,
                .Length = 0,
                .LockMode = ELockMode::Shared,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Test(Session3, range);
            UNIT_ASSERT(result.Failed());
        }

        // releasing Lock2
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner2,
                .Offset = 100,
                .Length = 100,
                .LockMode = ELockMode::Shared,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Release(Session2, range);
            UNIT_ASSERT(result.Succeeded());

            const auto& removedLocks = result.RemovedLockIds();
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
                .LockMode = ELockMode::Shared,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Test(Session3, range);
            UNIT_ASSERT(result.Failed());
            UNIT_ASSERT(result.IncompatibleHolds<TLockRange>());
        }

        // releasing Lock1 completely
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 300,
                .LockMode = ELockMode::Exclusive,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Release(Session1, range);
            UNIT_ASSERT(result.Succeeded());

            const auto& removedLocks = result.RemovedLockIds();
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

            auto result = locks.Test(Session3, range);
            UNIT_ASSERT(result.Succeeded());
        }

        // locking file till the end of it
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner3,
                .Offset = 100,
                .Length = 0,
                .LockMode = ELockMode::Exclusive,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Acquire(Session3, Lock3, range);
            UNIT_ASSERT(result.Succeeded());
            UNIT_ASSERT(result.RemovedLockIds().empty());
        }

        // attempt to lock subrange after offset 100 should fail
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 300,
                .Length = 10,
                .LockMode = ELockMode::Shared,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Test(Session1, range);
            UNIT_ASSERT(result.Failed());
            UNIT_ASSERT(result.IncompatibleHolds<TLockRange>());
        }

        // but we can still lock ranges before offset 100
        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 100,
                .LockMode = ELockMode::Shared,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Test(Session1, range);
            UNIT_ASSERT(result.Succeeded());
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
                .LockMode = ELockMode::Exclusive,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Acquire(Session1, Lock1, range);
            UNIT_ASSERT(result.Succeeded());
            UNIT_ASSERT(result.RemovedLockIds().empty());
        }

        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 100,
                .Length = 100,
                .LockMode = ELockMode::Exclusive,
                .LockOrigin = ELockOrigin::Fcntl,
            };

            auto result = locks.Acquire(Session1, Lock2, range);

            UNIT_ASSERT(result.Succeeded());
            UNIT_ASSERT(result.RemovedLockIds().empty());
        }

        {
            TLockRange range = {
                .NodeId = NodeId,
                .OwnerId = Owner1,
                .Offset = 0,
                .Length = 200,
                .LockMode = ELockMode::Exclusive,
                .LockOrigin = ELockOrigin::Fcntl,
            };
            auto result = locks.Acquire(Session1, Lock3, range);
            UNIT_ASSERT(result.Succeeded());

            const auto& removedLocks = result.RemovedLockIds();
            UNIT_ASSERT(removedLocks.size() == 2);
            UNIT_ASSERT(removedLocks[0] == Lock1);
            UNIT_ASSERT(removedLocks[1] == Lock2);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
