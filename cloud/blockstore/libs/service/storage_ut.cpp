#include "storage.h"
#include "storage_test.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageTest)
{
    void ShouldHandleNonNormalizedRequests(ui32 requestBlockSize)
    {
        auto storage = std::make_shared<TTestStorage>();
        storage->WriteBlocksLocalHandler = [] (auto ctx, auto request) {
            Y_UNUSED(ctx);

            UNIT_ASSERT_VALUES_EQUAL(1_MB / 4_KB, request->BlocksCount);
            UNIT_ASSERT_VALUES_EQUAL(4_KB, request->BlockSize);

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);

            const auto& sgList = guard.Get();
            UNIT_ASSERT_VALUES_EQUAL(1, sgList.size());
            UNIT_ASSERT_VALUES_EQUAL(1_MB, sgList[0].Size());
            UNIT_ASSERT_VALUES_EQUAL(1_MB, Count(sgList[0].AsStringBuf(), 'X'));

            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };

        auto adapter = std::make_shared<TStorageAdapter>(
            storage,
            4_KB,   // storageBlockSize
            false,  // normalize
            32_MB); // maxRequestSize

        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetStartIndex(1000);

        auto& iov = *request->MutableBlocks();
        auto& buffers = *iov.MutableBuffers();
        auto& buffer = *buffers.Add();
        buffer.ReserveAndResize(1_MB);
        memset(const_cast<char*>(buffer.data()), 'X', buffer.size());

        auto future = adapter->WriteBlocks(
            Now(),
            MakeIntrusive<TCallContext>(),
            std::move(request),
            requestBlockSize);

        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));
    }

    Y_UNIT_TEST(ShouldHandleNonNormalizedRequests4K)
    {
        ShouldHandleNonNormalizedRequests(4_KB);
    }

    Y_UNIT_TEST(ShouldHandleNonNormalizedRequests8K)
    {
        ShouldHandleNonNormalizedRequests(8_KB);
    }

    void ShouldNormalizeRequests(ui32 requestBlockSize)
    {
        auto storage = std::make_shared<TTestStorage>();
        storage->WriteBlocksLocalHandler = [] (auto ctx, auto request) {
            Y_UNUSED(ctx);

            UNIT_ASSERT_VALUES_EQUAL(1_MB / 4_KB, request->BlocksCount);
            UNIT_ASSERT_VALUES_EQUAL(4_KB, request->BlockSize);

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);

            const auto& sgList = guard.Get();
            UNIT_ASSERT_VALUES_EQUAL(1_MB / 4_KB, sgList.size());
            for (auto b: sgList) {
                UNIT_ASSERT_VALUES_EQUAL(4_KB, b.Size());
                UNIT_ASSERT_VALUES_EQUAL(4_KB, Count(b.AsStringBuf(), 'X'));
            }

            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };

        auto adapter = std::make_shared<TStorageAdapter>(
            storage,
            4_KB,   // storageBlockSize
            true,   // normalize
            32_MB); // maxRequestSize

        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetStartIndex(1000);

        auto& iov = *request->MutableBlocks();
        auto& buffers = *iov.MutableBuffers();
        auto& buffer = *buffers.Add();
        buffer.ReserveAndResize(1_MB);
        memset(const_cast<char*>(buffer.data()), 'X', buffer.size());

        auto future = adapter->WriteBlocks(
            Now(),
            MakeIntrusive<TCallContext>(),
            std::move(request),
            requestBlockSize);

        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));
    }

    Y_UNIT_TEST(ShouldNormalizeRequests4K)
    {
        ShouldNormalizeRequests(4_KB);
    }

    Y_UNIT_TEST(ShouldNormalizeRequests8K)
    {
        ShouldNormalizeRequests(8_KB);
    }

    template <typename F>
    void DoShouldHandleTimedOutRequests(F runRequest)
    {
        constexpr TDuration WaitTimeout = TDuration::Seconds(2);

        auto storage = std::make_shared<TTestStorage>();
        storage->ReadBlocksLocalHandler = [&](auto ctx, auto request) {
            auto promise = NewPromise<NProto::TReadBlocksLocalResponse>();
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return promise;
        };
        storage->WriteBlocksLocalHandler = [&](auto ctx, auto request) {
            auto promise = NewPromise<NProto::TWriteBlocksLocalResponse>();
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return promise;
        };
        storage->ZeroBlocksHandler = [&](auto ctx, auto request) {
            auto promise = NewPromise<NProto::TZeroBlocksResponse>();
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return promise;
        };

        auto adapter = std::make_shared<TStorageAdapter>(
            storage,
            4_KB,                   // storageBlockSize
            false,                  // normalize
            32_MB,                  // maxRequestSize
            TDuration::Seconds(1)   // timeout
        );

        auto now = TInstant::Seconds(1);

        // Run request.
        auto future = runRequest(adapter, now);

        // Assert request is not handled.
        bool responseReady = future.Wait(WaitTimeout);
        UNIT_ASSERT(!responseReady);
        UNIT_ASSERT_VALUES_EQUAL(0, storage->ErrorCount);

        // Skip more time than IO timeout.
        now += TDuration::Seconds(5);

        // Assert request finished with IO error.
        adapter->CheckIOTimeouts(now);
        UNIT_ASSERT_VALUES_EQUAL(1, storage->ErrorCount);
        responseReady = future.Wait(TDuration());
        UNIT_ASSERT(responseReady);
        auto response = future.GetValue(TDuration());
        UNIT_ASSERT(HasError(response));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldHandleTimedOutReadRequests)
    {
        auto runRequest =
            [](std::shared_ptr<TStorageAdapter> adapter, TInstant now)
        {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetStartIndex(1000);
            request->SetBlocksCount(1);

            return adapter->ReadBlocks(
                now,
                MakeIntrusive<TCallContext>(),
                std::move(request),
                4096);
        };
        DoShouldHandleTimedOutRequests(runRequest);
    }

    Y_UNIT_TEST(ShouldHandleTimedOutWriteRequests)
    {
        auto runRequest =
            [](std::shared_ptr<TStorageAdapter> adapter, TInstant now)
        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetStartIndex(1000);
            auto& iov = *request->MutableBlocks();
            auto& buffers = *iov.MutableBuffers();
            auto& buffer = *buffers.Add();
            buffer.ReserveAndResize(1_MB);
            memset(const_cast<char*>(buffer.data()), 'X', buffer.size());

            return adapter->WriteBlocks(
                now,
                MakeIntrusive<TCallContext>(),
                std::move(request),
                4096);
        };
        DoShouldHandleTimedOutRequests(runRequest);
    }

    Y_UNIT_TEST(ShouldHandleTimeedOutZeroRequests)
    {
        auto runRequest =
            [](std::shared_ptr<TStorageAdapter> adapter, TInstant now)
        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(1000);
            request->SetBlocksCount(1);

            return adapter->ZeroBlocks(
                now,
                MakeIntrusive<TCallContext>(),
                std::move(request),
                4096);
        };
        DoShouldHandleTimedOutRequests(runRequest);
    }
}

}   // namespace NCloud::NBlockStore
