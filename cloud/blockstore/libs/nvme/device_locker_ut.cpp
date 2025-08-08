#include "device_locker.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/system/env.h>
#include <util/system/file.h>

#include <chrono>

namespace NCloud::NBlockStore::NNvme {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;

    const TTempDir DevicesFolder;

    static constexpr ui32 AvailableDevicesCount = 8;

    static inline const TDeviceLocker::TRetryOptions FastRetryOptions{
        .RetryCount = 3,
        .SleepDuration = 100ms,
        .SleepIncrement = 0s};

    void SetUp(NUnitTest::TTestContext& /*testContext*/) final
    {
        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Logging->Start();

        for (ui32 i = 0; i != AvailableDevicesCount; ++i) {
            TFile file(
                DevicesFolder.Path() / ("nvme0n" + ToString(i + 1)),
                EOpenModeFlag::CreateNew);
            Y_UNUSED(file);
        }
    }

    void TearDown(NUnitTest::TTestContext& /* context */) final
    {
        Logging->Stop();
    }

    TDeviceLocker CreateBookkeeper(TStringBuf mask) const
    {
        return {Logging, DevicesFolder.Path(), mask};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceLockerTest)
{
    Y_UNIT_TEST_F(ShouldAcquireDevices, TFixture)
    {
        TDeviceLocker locker0 =
            CreateBookkeeper(TDeviceLocker::DefaultNameMask);

        UNIT_ASSERT_VALUES_EQUAL(
            AvailableDevicesCount,
            locker0.AvailableDevicesCount());

        TDeviceLocker locker1 = CreateBookkeeper("nvme[0-9]n(1|2)");
        UNIT_ASSERT_VALUES_EQUAL(2, locker1.AvailableDevicesCount());

        TDeviceLocker locker2 = CreateBookkeeper("nvme[0-9]n(2|3)");
        UNIT_ASSERT_VALUES_EQUAL(2, locker2.AvailableDevicesCount());

        TVector<TString> paths;

        for (ui32 i = 0; i != locker1.AvailableDevicesCount(); ++i) {
            auto [path, error] = locker1.AcquireDevice();
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            paths.push_back(path);
        }

        // locker1 is exhausted
        {
            auto [_, error] = locker1.AcquireDevice(FastRetryOptions);
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
        }

        {
            auto [path, error] = locker2.AcquireDevice();
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            paths.push_back(path);
        }

        // locker2 is exhausted
        {
            auto [_, error] = locker2.AcquireDevice(FastRetryOptions);
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
        }

        // nvme0n1, nvme0n2, nvme0n3
        UNIT_ASSERT_VALUES_EQUAL(3, paths.size());

        SortUnique(paths);

        UNIT_ASSERT_VALUES_EQUAL(3, paths.size());
        UNIT_ASSERT_VALUES_EQUAL(DevicesFolder.Path() / "nvme0n1", paths[0]);
        UNIT_ASSERT_VALUES_EQUAL(DevicesFolder.Path() / "nvme0n2", paths[1]);
        UNIT_ASSERT_VALUES_EQUAL(DevicesFolder.Path() / "nvme0n3", paths[2]);

        // acquire remaining devices: nvme0n4, nvme0n5, ...
        for (ui32 i = 0; i != locker0.AvailableDevicesCount() - paths.size();
             ++i)
        {
            auto [path, error] = locker0.AcquireDevice();
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        // locker0 is exhausted
        {
            auto [_, error] = locker0.AcquireDevice(FastRetryOptions);
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
        }

        locker1.ReleaseDevice(paths[0]);    // release nvme0n1

        // locker2 is still exhausted
        {
            auto [_, error] = locker2.AcquireDevice(FastRetryOptions);
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
        }

        locker1.ReleaseDevice(paths[1]);    // release nvme0n2

        // acquire nvme0n2 with locker2
        {
            auto [path, error] = locker2.AcquireDevice();
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(paths[1], path);
        }

        // locker2 is exhausted again
        {
            auto [_, error] = locker2.AcquireDevice(FastRetryOptions);
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, error.GetCode());
        }
    }
}

}   // namespace NCloud::NBlockStore::NNvme
