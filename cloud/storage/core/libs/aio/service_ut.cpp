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

    Y_UNIT_TEST(ShouldRetryIoSetupErrors)
    {
        const auto eventCountLimit =
            FromString<size_t>(TIFStream("/proc/sys/fs/aio-max-nr").ReadLine());
        const auto service1EventCount = eventCountLimit / 2;
        auto service1 = CreateAIOService(service1EventCount);
        auto promise1 = NThreading::NewPromise<void>();
        auto promise2 = NThreading::NewPromise<void>();
        SystemThreadFactory()->Run([&] () {
            promise1.SetValue();

            const auto service2EventCount =
                eventCountLimit - service1EventCount + 1;
            // should cause EAGAIN from io_setup until service1 is destroyed
            auto service2 = CreateAIOService(service2EventCount);
            Y_UNUSED(service2);
            promise2.SetValue();
        });

        promise1.GetFuture().GetValueSync();
        Sleep(TDuration::Seconds(1));
        service1.reset();
        promise2.GetFuture().GetValue(TDuration::Seconds(5));
    }
}

}   // namespace NCloud
