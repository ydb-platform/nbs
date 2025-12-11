#include "storage_with_stats.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 8;
constexpr ui64 DefaultBlockCount = 100;
constexpr ui64 DefaultBytesCount = DefaultBlockSize * DefaultBlockCount;

////////////////////////////////////////////////////////////////////////////////

struct TTestStorage final: public IStorage
{
    NProto::TError Error;

    TTestStorage() = default;

    explicit TTestStorage(NProto::TError error)
        : Error(std::move(error))
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return CreateResponse<NProto::TZeroBlocksResponse>();
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return CreateResponse<NProto::TReadBlocksLocalResponse>();
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return CreateResponse<NProto::TWriteBlocksLocalResponse>();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture(NProto::TError());
    }

    void ReportIOError() override
    {}

private:
    template <typename T>
    TFuture<T> CreateResponse()
    {
        T response;
        *response.MutableError() = Error;

        return MakeFuture(response);
    }
};

////////////////////////////////////////////////////////////////////////////////

void ReadBlocksLocal(IStoragePtr storage)
{
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetBlocksCount(DefaultBlockCount);
    request->BlockSize = DefaultBlockSize;

    TVector<TString> blocks;
    request->Sglist.SetSgList(ResizeBlocks(
        blocks,
        DefaultBlockCount,
        TString(DefaultBlockSize, 'A')));

    storage->ReadBlocksLocal(MakeIntrusive<TCallContext>(), std::move(request))
        .GetValueSync();
}

void WriteBlocksLocal(IStoragePtr storage)
{
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->BlocksCount = DefaultBlockCount;
    request->BlockSize = DefaultBlockSize;

    TVector<TString> blocks;
    request->Sglist.SetSgList(ResizeBlocks(
        blocks,
        DefaultBlockCount,
        TString(DefaultBlockSize, 'A')));

    storage->WriteBlocksLocal(MakeIntrusive<TCallContext>(), std::move(request))
        .GetValueSync();
}

void ZeroBlocksLocal(IStoragePtr storage)
{
    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetBlocksCount(DefaultBlockCount);

    storage->ZeroBlocks(MakeIntrusive<TCallContext>(), std::move(request))
        .GetValueSync();
}

IStoragePtr CreateTestStorage(TStorageIoStatsPtr stats)
{
    return CreateStorageWithIoStats(
        std::make_shared<TTestStorage>(),
        std::move(stats),
        DefaultBlockSize);
}

IStoragePtr CreateErrorStorage(TStorageIoStatsPtr stats, i32 code)
{
    NProto::TError error;
    error.SetCode(code);

    return CreateStorageWithIoStats(
        std::make_shared<TTestStorage>(error),
        std::move(stats),
        DefaultBlockSize);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageWithIoStatsTest)
{
    Y_UNIT_TEST(ShouldCountLocalReads)
    {
        auto stats = std::make_shared<TStorageIoStats>();
        auto storage = CreateTestStorage(stats);

        const int requestCount = 100;

        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->NumReadOps));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->BytesRead));

        for (int i = 0; i != requestCount; ++i) {
            ReadBlocksLocal(storage);

            UNIT_ASSERT_VALUES_EQUAL(i + 1, AtomicGet(stats->NumReadOps));
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBytesCount * (i + 1),
                AtomicGet(stats->BytesRead));
        }
    }

    Y_UNIT_TEST(ShouldCountLocalWrites)
    {
        auto stats = std::make_shared<TStorageIoStats>();
        auto storage = CreateTestStorage(stats);

        const int requestCount = 100;

        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->NumWriteOps));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->BytesWritten));

        for (int i = 0; i != requestCount; ++i) {
            WriteBlocksLocal(storage);

            UNIT_ASSERT_VALUES_EQUAL(i + 1, AtomicGet(stats->NumWriteOps));
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBytesCount * (i + 1),
                AtomicGet(stats->BytesWritten));
        }
    }

    Y_UNIT_TEST(ShouldCountLocalZero)
    {
        auto stats = std::make_shared<TStorageIoStats>();
        auto storage = CreateTestStorage(stats);

        const int requestCount = 100;

        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->NumZeroOps));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->BytesZeroed));

        for (int i = 0; i != requestCount; ++i) {
            ZeroBlocksLocal(storage);

            UNIT_ASSERT_VALUES_EQUAL(i + 1, AtomicGet(stats->NumZeroOps));
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBytesCount * (i + 1),
                AtomicGet(stats->BytesZeroed));
        }
    }

    void CheckErrorsCount(bool isRetryableError)
    {
        auto stats = std::make_shared<TStorageIoStats>();
        auto storage =
            CreateErrorStorage(stats, isRetryableError ? E_REJECTED : E_IO);

        const int requestCount = 100;

        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->NumWriteOps));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->BytesWritten));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->NumReadOps));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->BytesRead));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->Errors));

        for (int i = 0; i != requestCount; ++i) {
            WriteBlocksLocal(storage);
            ReadBlocksLocal(storage);
            ZeroBlocksLocal(storage);
        }

        UNIT_ASSERT_VALUES_EQUAL(requestCount, AtomicGet(stats->NumWriteOps));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->BytesWritten));
        UNIT_ASSERT_VALUES_EQUAL(requestCount, AtomicGet(stats->NumReadOps));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(stats->BytesRead));
        UNIT_ASSERT_VALUES_EQUAL(
            isRetryableError ? 0 : requestCount * 3,
            AtomicGet(stats->Errors));
    }

    Y_UNIT_TEST(ShouldCountErrors)
    {
        CheckErrorsCount(false);
    }

    Y_UNIT_TEST(ShouldNotCountRetryableErrors)
    {
        CheckErrorsCount(true);
    }

    Y_UNIT_TEST(ShouldCountExplicitErrors)
    {
        auto stats = std::make_shared<TStorageIoStats>();
        auto storage = CreateErrorStorage(stats, E_IO);

        storage->ReportIOError();
        storage->ReportIOError();

        UNIT_ASSERT_VALUES_EQUAL(2, AtomicGet(stats->Errors));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
