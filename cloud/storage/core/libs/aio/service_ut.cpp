#include "service.h"

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
#include <util/thread/factory.h>

namespace NCloud {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TFsPath TryGetRamDrivePath()
{
    auto p = GetRamDrivePath();
    return !p
        ? GetSystemTempDir()
        : p;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAioTest)
{
    Y_UNIT_TEST(ShouldReadWrite)
    {
        auto service = CreateAIOService();
        service->Start();
        Y_DEFER { service->Stop(); };

        const ui32 blockSize = 4_KB;
        const ui64 blockCount = 1024;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFileHandle fileData(filePath, OpenAlways | RdWr | DirectAligned | Sync);
        fileData.Resize(blockCount * blockSize);

        const ui64 requestStartIndex = 20;
        const ui64 requestBlockCount = 200;
        const ui64 length = requestBlockCount * blockSize;
        const i64 offset = requestStartIndex * blockSize;

        std::shared_ptr<char> memory {
            static_cast<char*>(std::aligned_alloc(blockSize, length)),
            std::free
        };

        TArrayRef<char> buffer {memory.get(), length};

        std::memset(buffer.data(), 'X', buffer.size());

        {
            auto result = service->AsyncWrite(fileData, offset, buffer);

            UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValueSync());
        }

        std::memset(buffer.data(), '.', buffer.size());

        {
            auto result = service->AsyncRead(fileData, offset, buffer);

            UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValueSync());
        }

        for (char val: buffer) {
            UNIT_ASSERT_VALUES_EQUAL('X', val);
        }
    }

    Y_UNIT_TEST(ShouldReadWriteV)
    {
        auto service = CreateAIOService();
        service->Start();
        Y_DEFER { service->Stop(); };

        const ui32 blockSize = 4_KB;
        const ui64 blockCount = 1024;
        const auto filePath = TryGetRamDrivePath() / "test";

        TFileHandle fileData(filePath, OpenAlways | RdWr | DirectAligned | Sync);
        fileData.Resize(blockCount * blockSize);

        const ui64 requestStartIndex = 20;
        const ui64 requestBlockCount = 200;
        const ui64 length = requestBlockCount * blockSize;
        const i64 offset = requestStartIndex * blockSize;

        std::shared_ptr<char> memory {
            static_cast<char*>(std::aligned_alloc(blockSize, length)),
            std::free
        };

        TVector<TArrayRef<char>> buffers;
        buffers.emplace_back(memory.get(), 20 * blockSize);
        buffers.emplace_back(memory.get() + 20 * blockSize, 80 * blockSize);
        buffers.emplace_back(memory.get() + 100 * blockSize, 40 * blockSize);
        buffers.emplace_back(memory.get() + 140 * blockSize, 60 * blockSize);

        TVector<TArrayRef<const char>> constBuffers;
        for (auto& buffer: buffers) {
            constBuffers.emplace_back(buffer.data(), buffer.size());
        }

        for (auto& buffer: buffers) {
            std::memset(buffer.data(), 'X', buffer.size());
        }

        {
            auto result = service->AsyncWriteV(fileData, offset, constBuffers);
            UNIT_ASSERT_VALUES_EQUAL(length, result.GetValueSync());
        }

        for (auto& buffer: buffers) {
            std::memset(buffer.data(), '.', buffer.size());
        }

        {
            auto result = service->AsyncReadV(fileData, offset, buffers);
            UNIT_ASSERT_VALUES_EQUAL(length, result.GetValueSync());
        }

        for (auto& buffer: buffers) {
            for (char val: buffer) {
                UNIT_ASSERT_VALUES_EQUAL('X', val);
            }
        }
    }

    Y_UNIT_TEST(ShouldRetryIoSetupErrors)
    {
        const ui32 eventCountLimit =
            FromString<ui32>(TIFStream("/proc/sys/fs/aio-max-nr").ReadLine());
        const ui32 service1EventCount = eventCountLimit / 2;
        auto service1 = CreateAIOService({.MaxEvents = service1EventCount});
        auto promise1 = NThreading::NewPromise<void>();
        auto promise2 = NThreading::NewPromise<void>();
        SystemThreadFactory()->Run([=] () mutable {
            promise1.SetValue();

            const auto service2EventCount =
                eventCountLimit - service1EventCount + 1;
            // should cause EAGAIN from io_setup until service1 is destroyed
            auto service2 = CreateAIOService({.MaxEvents = service2EventCount});
            Y_UNUSED(service2);
            promise2.SetValue();
        });

        promise1.GetFuture().GetValueSync();
        Sleep(TDuration::Seconds(1));
        service1.reset();
        promise2.GetFuture().GetValueSync();
    }
}

}   // namespace NCloud
