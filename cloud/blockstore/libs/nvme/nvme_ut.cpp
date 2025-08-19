#include "nvme.h"

#include "testing/device_locker.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/file.h>

#include <chrono>
#include <memory>

namespace NCloud::NBlockStore::NNvme {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFree
{
    void operator () (void* ptr) const
    {
        std::free(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;
    INvmeManagerPtr NvmeManager;

    TDeviceLocker DeviceLocker;
    TFsPath DevicePath;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) final
    {
        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Logging->Start();

        NvmeManager = CreateNvmeManager(15s);

        DeviceLocker = TDeviceLocker::CreateFromEnv(Logging);
        DevicePath = GetDevice();
    }

    void TearDown(NUnitTest::TTestContext& /* context */) final
    {
        DeviceLocker.ReleaseDevice(DevicePath);

        Logging->Stop();
    }

    TFsPath GetDevice()
    {
        TFsPath path;

        auto r = DeviceLocker.AcquireDevice();
        UNIT_ASSERT_C(!HasError(r), FormatError(r.GetError()));
        UNIT_ASSERT(r.GetResult());
        return r.ExtractResult();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNvmeManagerTest)
{
    Y_UNIT_TEST_F(ShouldGetSerialNumber, TFixture)
    {
        auto [sn, error] = NvmeManager->GetSerialNumber(DevicePath);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            error.GetCode(),
            FormatError(error));
        UNIT_ASSERT(sn);
    }

    Y_UNIT_TEST_F(ShouldBeSsd, TFixture)
    {
        auto r = NvmeManager->IsSsd(DevicePath);
        UNIT_ASSERT_C(!HasError(r), FormatError(r.GetError()));
        UNIT_ASSERT(r.GetResult());
    }

    Y_UNIT_TEST_F(ShouldDeallocate, TFixture)
    {
        const ui64 len = GetFileLength(DevicePath);
        UNIT_ASSERT_GT(1_MB, len);

        const ui64 alignment = 4_KB;
        const ui64 offsetBytes = 8_KB;
        const ui64 sizeBytes = 32_KB;
        const int expectedValue = 'X';

        TFile file{
            DevicePath,
            EOpenModeFlag::OpenExisting | EOpenModeFlag::DirectAligned |
                EOpenModeFlag::Sync | EOpenModeFlag::RdWr};

        std::unique_ptr<char, TFree> buffer(
            static_cast<char*>(std::aligned_alloc(alignment, sizeBytes)));

        // prepare data

        std::memset(buffer.get(), expectedValue, sizeBytes);
        file.Pwrite(buffer.get(), sizeBytes, offsetBytes);

        // ensure
        {
            std::memset(buffer.get(), 0, sizeBytes);
            const size_t read =
                file.Pread(buffer.get(), sizeBytes, offsetBytes);
            UNIT_ASSERT_VALUES_EQUAL(read, sizeBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                sizeBytes,
                std::count(
                    buffer.get(),
                    buffer.get() + sizeBytes,
                    expectedValue));
        }

        // deallocate

        auto future =
            NvmeManager->Deallocate(DevicePath, offsetBytes, sizeBytes);
        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        // check
        {
            const size_t read =
                file.Pread(buffer.get(), sizeBytes, offsetBytes);
            UNIT_ASSERT_VALUES_EQUAL(read, sizeBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                sizeBytes,
                std::count(buffer.get(), buffer.get() + sizeBytes, 0));
        }
    }
}

}   // namespace NCloud::NBlockStore::NNvme
