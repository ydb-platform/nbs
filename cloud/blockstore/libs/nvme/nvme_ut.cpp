#include "nvme.h"

#include "nvme_allocator.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>
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

TNvmeAllocator CreateNvmeAllocator(ILoggingServicePtr logging)
{
    auto devicesFolder = GetEnv("NVME_LOOP_DEVICES_FOLDER");
    UNIT_ASSERT(devicesFolder);

    auto locksFolder = GetEnv("NVME_LOOP_LOCKS_FOLDER");
    UNIT_ASSERT(locksFolder);

    return {std::move(logging), devicesFolder, locksFolder};
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;

    std::unique_ptr<TNvmeAllocator> Allocator;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) final
    {
        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Logging->Start();

        Allocator =
            std::make_unique<TNvmeAllocator>(CreateNvmeAllocator(Logging));
    }

    void TearDown(NUnitTest::TTestContext& /* context */) final
    {
        Logging->Stop();
    }

    TFsPath GetNvme()
    {
        TFsPath path;

        auto r = Allocator->AcquireNvme(30s, 1s);
        UNIT_ASSERT_C(!HasError(r), FormatError(r.GetError()));
        UNIT_ASSERT(r.GetResult());
        return r.ExtractResult();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNvmeAllocatorTest)
{
    Y_UNIT_TEST_F(ShouldAllocateDevices, TFixture)
    {
        const size_t size = Allocator->AvailableDevicesCount();
        UNIT_ASSERT_GT(size, 0);

        // allocate all available devices
        for (size_t i = 0; i != size; ++i) {
            auto [path, error] = Allocator->AcquireNvme();
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT(path);
        }

        {
            auto [_, error] = Allocator->AcquireNvme();
            UNIT_ASSERT_C(HasError(error), FormatError(error));
        }

        // release all devices

        Allocator.reset();

        // all devices can be acquired again

        TNvmeAllocator alloc1 = CreateNvmeAllocator(Logging);
        TNvmeAllocator alloc2 = CreateNvmeAllocator(Logging);

        THashSet<TString> devices1;
        THashSet<TString> devices2;

        for (size_t i = 0; i != size / 2; ++i) {
            {
                auto [path, error] = alloc1.AcquireNvme();
                UNIT_ASSERT_C(!HasError(error), FormatError(error));
                UNIT_ASSERT(path);

                devices1.insert(path);
            }

            {
                auto [path, error] = alloc2.AcquireNvme();
                UNIT_ASSERT_C(!HasError(error), FormatError(error));
                UNIT_ASSERT(path);

                devices2.insert(path);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(size / 2, devices1.size());
        UNIT_ASSERT_VALUES_EQUAL(size / 2, devices2.size());
        UNIT_ASSERT(devices1 != devices2);

        {
            auto [_, error] = alloc1.AcquireNvme();
            UNIT_ASSERT_C(HasError(error), FormatError(error));
        }

        {
            auto [_, error] = alloc2.AcquireNvme();
            UNIT_ASSERT_C(HasError(error), FormatError(error));
        }
    }
}

Y_UNIT_TEST_SUITE(TNvmeManagerTest)
{
    Y_UNIT_TEST_F(ShouldGetSerialNumber, TFixture)
    {
        const TFsPath path = GetNvme();
        auto manager = CreateNvmeManager(15s);

        {
            auto [sn, error] = manager->GetSerialNumber(path);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT(sn);
        }
    }

    Y_UNIT_TEST_F(ShouldDeallocate, TFixture)
    {
        const TFsPath path = GetNvme();
        auto manager = CreateNvmeManager(15s);

        const ui64 len = GetFileLength(path);
        UNIT_ASSERT_GT(1_MB, len);

        const ui64 alignment = 4_KB;
        const ui64 offsetBytes = 8_KB;
        const ui64 sizeBytes = 32_KB;
        const int expectedValue = 'X';

        TFile file{
            path,
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

        auto future = manager->Deallocate(path, offsetBytes, sizeBytes);
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

        {
            auto r = manager->IsSsd(path);
            UNIT_ASSERT_C(!HasError(r), FormatError(r.GetError()));
            UNIT_ASSERT(r.GetResult());
        }
    }
}

}   // namespace NCloud::NBlockStore::NNvme
