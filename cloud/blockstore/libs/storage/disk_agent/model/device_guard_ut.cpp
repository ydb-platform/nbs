#include "device_guard.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFiles: public NUnitTest::TBaseFixture
{
    const TFsPath Nvme1 = TFsPath(GetWorkPath()) / "nvme1";
    const TFsPath Nvme1n1 = TFsPath(GetWorkPath()) / "nvme1n1";
    const TFsPath Nvme1n1p1 = TFsPath(GetWorkPath()) / "nvme1n1p1";
    const TFsPath Nvme1n1p2 = TFsPath(GetWorkPath()) / "nvme1n1p2";

    const TFsPath Nvme2 = TFsPath(GetWorkPath()) / "nvme2";
    const TFsPath Nvme2n1 = TFsPath(GetWorkPath()) / "nvme2n1";
    const TFsPath Nvme2n1p1 = TFsPath(GetWorkPath()) / "nvme2n1p1";
    const TFsPath Nvme2n1p2 = TFsPath(GetWorkPath()) / "nvme2n1p2";

    const TFsPath Nvme3 = TFsPath(GetWorkPath()) / "nvme3";
    const TFsPath Nvme3n1 = TFsPath(GetWorkPath()) / "nvme3n1";
    const TFsPath Nvme3n1p1 = TFsPath(GetWorkPath()) / "nvme3n1p1";
    const TFsPath Nvme3n1p2 = TFsPath(GetWorkPath()) / "nvme3n1p2";

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Nvme1.Touch();
        Nvme1n1.Touch();
        Nvme1n1p1.Touch();
        Nvme1n1p2.Touch();

        Nvme2.Touch();
        Nvme2n1.Touch();
        Nvme2n1p1.Touch();
        Nvme2n1p2.Touch();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Nvme1.DeleteIfExists();
        Nvme1n1.DeleteIfExists();
        Nvme1n1p1.DeleteIfExists();
        Nvme1n1p2.DeleteIfExists();

        Nvme2.DeleteIfExists();
        Nvme2n1.DeleteIfExists();
        Nvme2n1p1.DeleteIfExists();
        Nvme2n1p2.DeleteIfExists();

        Nvme3.DeleteIfExists();
        Nvme3n1.DeleteIfExists();
        Nvme3n1p1.DeleteIfExists();
        Nvme3n1p2.DeleteIfExists();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceGuardTest)
{
    Y_UNIT_TEST_F(ShouldLockExistentPaths, TFiles)
    {
        TDeviceGuard locker1;
        UNIT_ASSERT(locker1.Lock(Nvme1));

        TDeviceGuard locker2;
        UNIT_ASSERT(!locker2.Lock(Nvme1));
    }

    Y_UNIT_TEST_F(ShouldLockExistentPathsRecursively, TFiles)
    {
        TDeviceGuard locker1;
        UNIT_ASSERT(locker1.Lock(Nvme1));

        TDeviceGuard locker2;
        UNIT_ASSERT(!locker2.Lock(Nvme1));

        UNIT_ASSERT(locker1.Lock(Nvme1));
        UNIT_ASSERT(!locker2.Lock(Nvme1));
    }

    Y_UNIT_TEST_F(ShouldLockExistentPathsWithCommonPrefix, TFiles)
    {
        TDeviceGuard locker1;
        UNIT_ASSERT(locker1.Lock(Nvme1n1p1));

        TDeviceGuard locker2;
        UNIT_ASSERT(!locker2.Lock(Nvme1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1p1));

        // Cannot lock nvme1n1p2, because nvme1n1 and nvme1 locked by locker1
        UNIT_ASSERT(!locker2.Lock(Nvme1n1p2));

        // Locker1 should be able to lock nvme1n1p2,
        // because locker2 didn't do this
        UNIT_ASSERT(locker1.Lock(Nvme1n1p2));
    }

    Y_UNIT_TEST_F(ShouldUnlockExistentPaths, TFiles)
    {
        TDeviceGuard locker1;
        UNIT_ASSERT(locker1.Lock(Nvme1));

        TDeviceGuard locker2;
        UNIT_ASSERT(!locker2.Lock(Nvme1));

        UNIT_ASSERT(locker1.Unlock(Nvme1));
        UNIT_ASSERT(locker2.Lock(Nvme1));
    }

    Y_UNIT_TEST_F(ShouldCorrectlyUnlockRecursivelyLockedPaths, TFiles)
    {
        TDeviceGuard locker1;
        UNIT_ASSERT(locker1.Lock(Nvme1));
        UNIT_ASSERT(locker1.Lock(Nvme1));

        TDeviceGuard locker2;
        UNIT_ASSERT(!locker2.Lock(Nvme1));

        UNIT_ASSERT(locker1.Unlock(Nvme1));
        UNIT_ASSERT(!locker2.Lock(Nvme1));

        UNIT_ASSERT(locker1.Unlock(Nvme1));
        UNIT_ASSERT(locker2.Lock(Nvme1));

        UNIT_ASSERT(!locker1.Unlock(Nvme1));
    }

    Y_UNIT_TEST_F(ShouldUnlockPathsWithCommonPrefix, TFiles)
    {
        TDeviceGuard locker1;
        UNIT_ASSERT(locker1.Lock(Nvme1n1p1));

        TDeviceGuard locker2;
        UNIT_ASSERT(!locker2.Lock(Nvme1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1p1));

        UNIT_ASSERT(locker1.Unlock(Nvme1n1p1));
        UNIT_ASSERT(locker2.Lock(Nvme1n1p2));
    }

    Y_UNIT_TEST_F(
        ShouldCorrectlyUnlockRecursivelyLockedPathsWithCommonPrefix,
        TFiles)
    {
        TDeviceGuard locker1;
        UNIT_ASSERT(locker1.Lock(Nvme1n1p1));
        UNIT_ASSERT(locker1.Lock(Nvme1n1));

        TDeviceGuard locker2;
        UNIT_ASSERT(!locker2.Lock(Nvme1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1p1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1p2));

        UNIT_ASSERT(locker1.Unlock(Nvme1n1));
        UNIT_ASSERT(!locker2.Lock(Nvme1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1p1));
        UNIT_ASSERT(!locker2.Lock(Nvme1n1p2));

        UNIT_ASSERT(locker1.Unlock(Nvme1n1p1));
        UNIT_ASSERT(locker2.Lock(Nvme1));
        UNIT_ASSERT(locker2.Lock(Nvme1n1));
        UNIT_ASSERT(locker2.Lock(Nvme1n1p1));
        UNIT_ASSERT(locker2.Lock(Nvme1n1p2));
    }

    Y_UNIT_TEST_F(ShouldNotLockNonExistentPaths, TFiles)
    {
        TDeviceGuard locker;
        UNIT_ASSERT(!locker.Lock(Nvme3));
    }

    Y_UNIT_TEST_F(ShouldNotLockNonExistentPathsWithExistentCommonPrefix, TFiles)
    {
        Nvme3.Touch();

        TDeviceGuard locker1;
        UNIT_ASSERT(!locker1.Lock(Nvme3n1));
        UNIT_ASSERT(!locker1.Lock(Nvme3n1p1));

        TDeviceGuard locker2;
        UNIT_ASSERT(locker2.Lock(Nvme3));
    }

    Y_UNIT_TEST_F(ShouldNotUnlockNonExistentPaths, TFiles)
    {
        TDeviceGuard locker;
        UNIT_ASSERT(!locker.Unlock(Nvme3));
    }

    Y_UNIT_TEST_F(ShouldNotUnlockDeletedPaths, TFiles)
    {
        TDeviceGuard locker;
        UNIT_ASSERT(locker.Lock(Nvme1));

        Nvme1.DeleteIfExists();

        UNIT_ASSERT(!locker.Unlock(Nvme1));
        UNIT_ASSERT(!locker.Lock(Nvme1));
        UNIT_ASSERT(!locker.Unlock(Nvme1));
    }

    Y_UNIT_TEST_F(ShouldUnlockPathsOnDestruction, TFiles)
    {
        TDeviceGuard locker1;

        {
            TDeviceGuard locker2;
            UNIT_ASSERT(locker2.Lock(Nvme1));

            UNIT_ASSERT(!locker1.Lock(Nvme1));
        }

        UNIT_ASSERT(locker1.Lock(Nvme1));
    }

    Y_UNIT_TEST_F(ShouldCorrectlyUnlockRecursivePathsOnDestruction, TFiles)
    {
        TDeviceGuard locker1;

        {
            TDeviceGuard locker2;
            UNIT_ASSERT(locker2.Lock(Nvme1));
            UNIT_ASSERT(locker2.Lock(Nvme1));

            UNIT_ASSERT(!locker1.Lock(Nvme1));
        }

        UNIT_ASSERT(locker1.Lock(Nvme1));
    }

    Y_UNIT_TEST_F(ShouldNotUnlockPathsOnMove, TFiles)
    {
        TDeviceGuard locker1, locker2;

        {
            TDeviceGuard locker3;
            UNIT_ASSERT(locker3.Lock(Nvme1));

            UNIT_ASSERT(!locker1.Lock(Nvme1));
            UNIT_ASSERT(!locker2.Lock(Nvme1));

            locker2 = std::move(locker3);
        }

        UNIT_ASSERT(!locker1.Lock(Nvme1));
        UNIT_ASSERT(locker2.Lock(Nvme1));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
