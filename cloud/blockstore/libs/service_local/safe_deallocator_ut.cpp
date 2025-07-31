#include "safe_deallocator.h"

#include <cloud/blockstore/libs/nvme/nvme.h>

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <library/cpp/testing/unittest/registar.h>

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

        return TaskQueue->Execute([] { return MakeError(S_OK); });
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

        TaskQueue->ExecuteSimple([completion, len = buffer.size()]
                                 { completion->Func(completion, {}, len); });
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSafeDeallocateDeviceTest)
{
    Y_UNIT_TEST_F(ShouldDeallocateDevice, TFixture)
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
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        const ui32 expectedDeallocations = 93;
        const ui32 expectedReads = BlocksCount / 1000;

        UNIT_ASSERT_VALUES_EQUAL(
            expectedDeallocations,
            NvmeManager->DeallocateRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(expectedReads, FileIO->ReadRequests.size());
    }
}

}   // namespace NCloud::NBlockStore::NServer
