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
}

}   // namespace NCloud::NBlockStore
