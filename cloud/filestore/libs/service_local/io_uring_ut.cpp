#include "io_uring.h"

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/generic/array_ref.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/system/file.h>

#include <atomic>
#include <chrono>

namespace NCloud::NFileStore {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4_KB;
constexpr const ui64 BlockCount = 1024;
constexpr const ui32 SubmissionQueueSize = 32;

////////////////////////////////////////////////////////////////////////////////

TFsPath TryGetRamDrivePath()
{
    auto p = GetRamDrivePath();
    return !p ? GetSystemTempDir() : p;
}

[[nodiscard]] std::shared_ptr<char> AllocMem(ui64 size)
{
    return {static_cast<char*>(std::aligned_alloc(BlockSize, size)), std::free};
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    static constexpr ui32 ServicesCount = 2;

    TFileHandle FileData;
    TVector<IFileIOServicePtr> Services;

    void SetUp(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        const TFsPath filePath = TryGetRamDrivePath() / "test";

        FileData =
            TFileHandle(filePath, OpenAlways | RdWr | DirectAligned | Sync);
        FileData.Resize(BlockCount * BlockSize);

        auto factory = CreateIoUringServiceFactory({
            .SubmissionQueueEntries = SubmissionQueueSize,
            .MaxKernelWorkersCount = 1,
            .ShareKernelWorkers = true,
            .ForceAsyncIO = true,
            .ForceSingleBuffer = true,
        });

        Services.reserve(ServicesCount);
        for (ui32 i = 0; i != ServicesCount; ++i) {
            Services.push_back(factory->CreateFileIOService());
        }

        for (const auto& service: Services) {
            service->Start();
        }
    }

    void TearDown(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        for (const auto& service: Services) {
            service->Stop();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIoUringTest)
{

    Y_UNIT_TEST_F(ShouldReadWrite, TFixture)
    {
        const ui64 requestStartIndex = 20;
        const ui64 requestBlockCount = 200;
        const ui64 length = requestBlockCount * BlockSize;
        const i64 offset = requestStartIndex * BlockSize;

        std::shared_ptr<char> memory = AllocMem(length);

        TArrayRef<char> buffer {memory.get(), length};

        for (int i = 0; i != ServicesCount; ++i) {
            auto& service = *Services[i];

            const int expectedData = 'A' + i;
            std::memset(buffer.data(), expectedData, buffer.size());

            {
                auto result = service.AsyncWrite(FileData, offset, buffer);
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValueSync());
            }

            std::memset(buffer.data(), 0, buffer.size());

            {
                auto result = service.AsyncRead(FileData, offset, buffer);

                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValueSync());
            }

            for (char val: buffer) {
                UNIT_ASSERT_VALUES_EQUAL(expectedData, val);
            }
        }
    }

    Y_UNIT_TEST_F(ShouldReadWriteV, TFixture)
    {
        const ui64 requestStartIndex = 15;
        const ui64 requestBlockCount = 200;
        const ui64 length = requestBlockCount * BlockSize;
        const i64 offset = requestStartIndex * BlockSize;

        std::shared_ptr<char> memory = AllocMem(length);

        TVector<TArrayRef<char>> buffers{
            {memory.get(), 20 * BlockSize},
            {memory.get() + 20 * BlockSize, 80 * BlockSize},
            {memory.get() + 100 * BlockSize, 40 * BlockSize},
            {memory.get() + 140 * BlockSize, 60 * BlockSize}};

        TVector<TArrayRef<const char>> constBuffers;
        for (auto& buffer: buffers) {
            constBuffers.emplace_back(buffer.data(), buffer.size());
        }

        for (int i = 0; i != ServicesCount; ++i) {
            auto& service = *Services[i];

            const int expectedData = 'A' + i;

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), expectedData, buffer.size());
            }

            {
                auto result =
                    service.AsyncWriteV(FileData, offset, constBuffers);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValueSync());
            }

            for (auto& buffer: buffers) {
                std::memset(buffer.data(), 0, buffer.size());
            }

            {
                auto result = service.AsyncReadV(FileData, offset, buffers);
                UNIT_ASSERT_VALUES_EQUAL(length, result.GetValueSync());
            }

            for (auto& buffer: buffers) {
                for (char val: buffer) {
                    UNIT_ASSERT_VALUES_EQUAL(expectedData, val);
                }
            }
        }
    }

    Y_UNIT_TEST_F(ShouldStop, TFixture)
    {
        for (const auto& service: Services) {
            service->Stop();
        }
    }
}

}   // namespace NCloud::NFileStore
