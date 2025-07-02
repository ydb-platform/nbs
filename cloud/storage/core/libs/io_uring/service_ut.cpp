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

#include <atomic>
#include <chrono>

namespace NCloud {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4_KB;
constexpr const ui64 BlockCount = 1024;

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
    static const ui32 SubmissionQueueSize = 32;

    TFileHandle FileData;
    IFileIOServicePtr IoUring;

    void SetUp(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        const TFsPath filePath = TryGetRamDrivePath() / "test";

        FileData = TFileHandle(
            filePath,
            OpenAlways | RdWr | DirectAligned | Sync);
        FileData.Resize(BlockCount * BlockSize);

        IoUring = CreateIoUringService("CQ", SubmissionQueueSize);
        IoUring->Start();
    }

    void TearDown(NUnitTest::TTestContext& context) final
    {
        Y_UNUSED(context);

        IoUring->Stop();
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

        std::memset(buffer.data(), 'X', buffer.size());

        {
            auto result = IoUring->AsyncWrite(FileData, offset, buffer);

            UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValueSync());
        }

        std::memset(buffer.data(), '.', buffer.size());

        {
            auto result = IoUring->AsyncRead(FileData, offset, buffer);

            UNIT_ASSERT_VALUES_EQUAL(buffer.size(), result.GetValueSync());
        }

        for (char val: buffer) {
            UNIT_ASSERT_VALUES_EQUAL('X', val);
        }
    }

    Y_UNIT_TEST_F(ShouldReadWriteV, TFixture)
    {
        const ui64 requestStartIndex = 20;
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

        for (auto& buffer: buffers) {
            std::memset(buffer.data(), 'X', buffer.size());
        }

        {
            auto result = IoUring->AsyncWriteV(FileData, offset, constBuffers);
            UNIT_ASSERT_VALUES_EQUAL(length, result.GetValueSync());
        }

        for (auto& buffer: buffers) {
            std::memset(buffer.data(), '.', buffer.size());
        }

        {
            auto result = IoUring->AsyncReadV(FileData, offset, buffers);
            UNIT_ASSERT_VALUES_EQUAL(length, result.GetValueSync());
        }

        for (auto& buffer: buffers) {
            for (char val: buffer) {
                UNIT_ASSERT_VALUES_EQUAL('X', val);
            }
        }
    }
}

}   // namespace NCloud
