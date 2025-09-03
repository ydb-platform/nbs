#include "safe_deallocator.h"

#include <cloud/blockstore/libs/nvme/nvme.h>

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace NNvme;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestNvmeManager final: NNvme::INvmeManager
{
    struct TDeallocateRequest
    {
        TString Path;
        ui64 OffsetBytes = 0;
        ui64 SizeBytes = 0;
    };

    ITaskQueuePtr TaskQueue;
    TVector<TDeallocateRequest> DeallocateRequests;

    std::function<NProto::TError ()> DeallocateImpl = [] {
        return NProto::TError{};
    };

    explicit TTestNvmeManager(ITaskQueuePtr taskQueue)
        : TaskQueue(std::move(taskQueue))
    {}

    TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) final
    {
        Y_UNUSED(path, ses);

        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) final
    {
        DeallocateRequests.emplace_back(path, offsetBytes, sizeBytes);

        return TaskQueue->Execute([error = DeallocateImpl()] { return error; });
    }

    TResultOrError<bool> IsSsd(const TString& path) final
    {
        Y_UNUSED(path);
        return true;
    }

    TResultOrError<TString> GetSerialNumber(const TString& path) final
    {
        Y_UNUSED(path);
        return TString("serial");
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestFileIO final: public IFileIOService
{
    struct TReadRequest
    {
        i64 Offset = 0;
        ui64 Size = 0;
    };

    ITaskQueuePtr TaskQueue;
    TVector<TReadRequest> ReadRequests;

    std::function<TResultOrError<ui32>(i64, TArrayRef<char>)> ReadImpl =
        [](i64 offset, TArrayRef<char> buffer)
    {
        Y_UNUSED(offset);

        std::memset(buffer.data(), 0, buffer.size());

        return buffer.size();
    };

    explicit TTestFileIO(ITaskQueuePtr taskQueue)
        : TaskQueue(std::move(taskQueue))
    {}

    void Start() final
    {}

    void Stop() final
    {}

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        ReadRequests.emplace_back(offset, buffer.size());

        auto [len, error] = ReadImpl(offset, buffer);

        TaskQueue->ExecuteSimple([=] {
            completion->Func(completion, error, len);
        });
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        completion->Func(completion, MakeError(E_NOT_IMPLEMENTED), 0);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        completion->Func(completion, MakeError(E_NOT_IMPLEMENTED), 0);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        completion->Func(completion, MakeError(E_NOT_IMPLEMENTED), 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    static constexpr ui32 BlockSize = 4_KB;
    static constexpr ui64 StorageSize = 93_GB;
    static constexpr ui64 StartIndex = 0;
    static constexpr ui64 BlocksCount = StorageSize / BlockSize;
    const ui32 ExpectedDeallocations = 93;
    const ui32 ExpectedReads = BlocksCount / 1000;

    const TString Filename = "filename";

    ITaskQueuePtr TaskQueue;
    std::shared_ptr<TTestNvmeManager> NvmeManager;
    std::shared_ptr<TTestFileIO> FileIO;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) final
    {
        TaskQueue = CreateThreadPool("Exec", 1);
        NvmeManager = std::make_shared<TTestNvmeManager>(TaskQueue);
        FileIO = std::make_shared<TTestFileIO>(TaskQueue);

        TaskQueue->Start();
    }

    void TearDown(NUnitTest::TTestContext& /* context */) final
    {
        TaskQueue->Stop();
    }

    void ShouldDeallocateDeviceImpl()
    {
        auto future = SafeDeallocateDevice(
            Filename,
            TFileHandle{},
            FileIO,
            StartIndex,
            BlocksCount,
            BlockSize,
            NvmeManager);

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(
            ExpectedDeallocations,
            NvmeManager->DeallocateRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(ExpectedReads, FileIO->ReadRequests.size());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSafeDeallocateDeviceTest)
{
    Y_UNIT_TEST_F(ShouldDeallocateDevice, TFixture)
    {
        ShouldDeallocateDeviceImpl();
    }

    Y_UNIT_TEST_F(ShouldDeallocateDeviceFF, TFixture)
    {
        FileIO->ReadImpl = [&](i64, auto buffer) -> TResultOrError<ui32>
        {
            std::memset(buffer.data(), 0xFF, buffer.size());

            return buffer.size();
        };

        ShouldDeallocateDeviceImpl();
    }

    Y_UNIT_TEST_F(ShouldHandleReadError, TFixture)
    {
        const NProto::TError expectedError =
            MakeError(MAKE_SYSTEM_ERROR(42), "Read error");

        const ui64 brokenReadIndex = RandomNumber<ui64>(ExpectedReads);
        ui64 readIndex = 0;

        FileIO->ReadImpl = [&](i64, auto buffer) -> TResultOrError<ui32>
        {
            if (readIndex++ == brokenReadIndex) {
                return expectedError;
            }

            std::memset(buffer.data(), 0, buffer.size());

            return buffer.size();
        };

        auto future = SafeDeallocateDevice(
            Filename,
            TFileHandle{},
            FileIO,
            StartIndex,
            BlocksCount,
            BlockSize,
            NvmeManager);

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedError.GetCode(),
            error.GetCode(),
            FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedError.GetMessage(),
            error.GetMessage(),
            FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(readIndex, FileIO->ReadRequests.size());
    }

    Y_UNIT_TEST_F(ShouldDetectDummyRead, TFixture)
    {
        const ui64 brokenReadIndex = RandomNumber<ui64>(ExpectedReads);
        ui64 readIndex = 0;

        FileIO->ReadImpl = [&](i64, auto buffer) -> TResultOrError<ui32>
        {
            if (readIndex++ != brokenReadIndex) {
                std::memset(buffer.data(), 0, buffer.size());
            }

            return buffer.size();
        };

        auto future = SafeDeallocateDevice(
            Filename,
            TFileHandle{},
            FileIO,
            StartIndex,
            BlocksCount,
            BlockSize,
            NvmeManager);

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(E_IO, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(readIndex, FileIO->ReadRequests.size());
    }

    Y_UNIT_TEST_F(ShouldDetectDirtyBuffer, TFixture)
    {
        const ui64 brokenReadIndex = RandomNumber<ui64>(ExpectedReads);
        ui64 readIndex = 0;

        FileIO->ReadImpl = [&](i64, auto buffer) -> TResultOrError<ui32>
        {
            std::memset(buffer.data(), 0, buffer.size());

            if (readIndex++ == brokenReadIndex) {
                buffer[buffer.size() / 2] = 0x42;
            }

            return buffer.size();
        };

        auto future = SafeDeallocateDevice(
            Filename,
            TFileHandle{},
            FileIO,
            StartIndex,
            BlocksCount,
            BlockSize,
            NvmeManager);

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(E_IO, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(readIndex, FileIO->ReadRequests.size());
    }

    Y_UNIT_TEST_F(ShouldHandleShortRead, TFixture)
    {
        const ui64 brokenReadIndex = RandomNumber<ui64>(ExpectedReads);
        ui64 readIndex = 0;

        FileIO->ReadImpl = [&](i64, auto buffer) -> TResultOrError<ui32>
        {
            std::memset(buffer.data(), 0, buffer.size());

            if (readIndex++ == brokenReadIndex) {
                return buffer.size() / 2;
            }

            return buffer.size();
        };

        auto future = SafeDeallocateDevice(
            Filename,
            TFileHandle{},
            FileIO,
            StartIndex,
            BlocksCount,
            BlockSize,
            NvmeManager);

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(E_IO, error.GetCode(), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(readIndex, FileIO->ReadRequests.size());
    }

    Y_UNIT_TEST_F(ShouldHandleDeallocateError, TFixture)
    {
        const NProto::TError expectedError =
            MakeError(MAKE_SYSTEM_ERROR(42), "Deallocate error");

        const ui64 brokenDeallocateIndex =
            RandomNumber<ui64>(ExpectedDeallocations);
        ui64 deallocateIndex = 0;

        NvmeManager->DeallocateImpl = [&]
        {
            if (deallocateIndex++ == brokenDeallocateIndex) {
                return expectedError;
            }

            return NProto::TError{};
        };

        auto future = SafeDeallocateDevice(
            Filename,
            TFileHandle{},
            FileIO,
            StartIndex,
            BlocksCount,
            BlockSize,
            NvmeManager);

        const auto& error = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedError.GetCode(),
            error.GetCode(),
            FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL_C(
            expectedError.GetMessage(),
            error.GetMessage(),
            FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(
            deallocateIndex,
            NvmeManager->DeallocateRequests.size());
    }
}

}   // namespace NCloud::NBlockStore::NServer
