#include "storage.h"
#include "storage_test.h"

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/list.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////
namespace {

class TTickOnGetNowTimer: public ITimer
{
public:
    using TCallback = std::function<void()>;

private:
    TInstant CurrentTime;
    const TDuration Step;
    TList<TCallback> CallbackList;

public:
    explicit TTickOnGetNowTimer(TDuration step)
        : Step(step)
    {}

    TInstant Now() override
    {
        if (!CallbackList.empty()) {
            auto callback = std::move(CallbackList.front());
            callback();
            CallbackList.pop_front();
        }
        CurrentTime += Step;
        return CurrentTime;
    }

    void AddOnTickCallback(TCallback callback)
    {
        CallbackList.push_back(std::move(callback));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageTest)
{
    void ShouldHandleNonNormalizedRequests(ui32 requestBlockSize, bool useDataBuffer)
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
            4_KB,                // storageBlockSize
            false,               // normalize,
            TDuration::Zero(),   // maxRequestDuration
            TDuration::Zero()    // shutdownTimeout
        );

        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetStartIndex(1000);

        TString data;
        TStringBuf dataBuffer;

        if (useDataBuffer) {
            data = TString(1_MB, 'X');
            dataBuffer = TStringBuf(data);
        } else {
            auto& iov = *request->MutableBlocks();
            auto& buffers = *iov.MutableBuffers();
            auto& buffer = *buffers.Add();
            buffer.ReserveAndResize(1_MB);
            memset(const_cast<char*>(buffer.data()), 'X', buffer.size());
        }

        auto future = adapter->WriteBlocks(
            Now(),
            MakeIntrusive<TCallContext>(),
            std::move(request),
            requestBlockSize,
            dataBuffer
        );

        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));
    }

    Y_UNIT_TEST(ShouldHandleNonNormalizedRequests4K)
    {
        ShouldHandleNonNormalizedRequests(4_KB, false);
        ShouldHandleNonNormalizedRequests(4_KB, true);
    }

    Y_UNIT_TEST(ShouldHandleNonNormalizedRequests8K)
    {
        ShouldHandleNonNormalizedRequests(8_KB, false);
        ShouldHandleNonNormalizedRequests(8_KB, true);
    }

    void ShouldNormalizeRequests(ui32 requestBlockSize, bool useDataBuffer)
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
            4_KB,                // storageBlockSize
            true,                // normalize,
            TDuration::Zero(),   // maxRequestDuration
            TDuration::Zero()    // shutdownTimeout
        );

        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetStartIndex(1000);

        TString data;
        TStringBuf dataBuffer;

        if (!useDataBuffer) {
            auto& iov = *request->MutableBlocks();
            auto& buffers = *iov.MutableBuffers();
            auto& buffer = *buffers.Add();
            buffer.ReserveAndResize(1_MB);
            memset(const_cast<char*>(buffer.data()), 'X', buffer.size());
        } else {
            data = TString(1_MB, 'X');
            dataBuffer = TStringBuf(data);
        }

        auto future = adapter->WriteBlocks(
            Now(),
            MakeIntrusive<TCallContext>(),
            std::move(request),
            requestBlockSize,
            dataBuffer
        );

        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));
    }

    Y_UNIT_TEST(ShouldNormalizeRequests4K)
    {
        ShouldNormalizeRequests(4_KB, false);
        ShouldNormalizeRequests(4_KB, true);
    }

    Y_UNIT_TEST(ShouldNormalizeRequests8K)
    {
        ShouldNormalizeRequests(8_KB, false);
        ShouldNormalizeRequests(8_KB, true);
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
            4_KB,                    // storageBlockSize
            false,                   // normalize
            TDuration::Seconds(1),   // maxRequestDuration
            TDuration::Zero()        // shutdownTimeout
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
                4096,
                {}   // no data buffer
            );
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
                4096,
                {}   // no data buffer
            );
        };
        DoShouldHandleTimedOutRequests(runRequest);
    }

    Y_UNIT_TEST(ShouldHandleTimedOutZeroRequests)
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

    Y_UNIT_TEST(ShouldWaitForRequestsToCompleteOnShutdown)
    {
        constexpr TDuration waitTimeout = TDuration::Seconds(1);
        constexpr TDuration shutdownTimeout = TDuration::Seconds(10);
        auto fastTimer =
            std::make_shared<TTickOnGetNowTimer>(TDuration::Seconds(1));

        auto storage = std::make_shared<TTestStorage>();
        auto readPromise = NewPromise<NProto::TReadBlocksLocalResponse>();
        auto writePromise = NewPromise<NProto::TWriteBlocksLocalResponse>();
        auto zeroPromise = NewPromise<NProto::TZeroBlocksResponse>();
        storage->ReadBlocksLocalHandler = [&](auto ctx, auto request) {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return readPromise;
        };
        storage->WriteBlocksLocalHandler = [&](auto ctx, auto request) {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return writePromise;
        };
        storage->ZeroBlocksHandler = [&](auto ctx, auto request) {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return zeroPromise;
        };

        auto adapter = std::make_shared<TStorageAdapter>(
            storage,
            4_KB,                    // storageBlockSize
            false,                   // normalize
            TDuration::Seconds(1),   // request timeout
            shutdownTimeout          // shutdown timeout
        );

        auto now = fastTimer->Now();

        {   // Run Read request.
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetStartIndex(1000);
            request->SetBlocksCount(1);

            auto response = adapter->ReadBlocks(
                now,
                MakeIntrusive<TCallContext>(),
                std::move(request),
                4096,
                {}   // no data buffer
            );

            // Assert request is not handled.
            bool responseReady = response.Wait(waitTimeout);
            UNIT_ASSERT(!responseReady);
        }
        {   // Run Write request.
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetStartIndex(1000);
            auto& iov = *request->MutableBlocks();
            auto& buffers = *iov.MutableBuffers();
            auto& buffer = *buffers.Add();
            buffer.ReserveAndResize(1_MB);
            memset(const_cast<char*>(buffer.data()), 'X', buffer.size());

            auto response = adapter->WriteBlocks(
                now,
                MakeIntrusive<TCallContext>(),
                std::move(request),
                4096,
                {}   // no data buffer
            );

            // Assert request is not handled.
            bool responseReady = response.Wait(waitTimeout);
            UNIT_ASSERT(!responseReady);
        }
        {
            // Run Zero request.
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(1000);
            request->SetBlocksCount(1);
            auto response = adapter->ZeroBlocks(
                now,
                MakeIntrusive<TCallContext>(),
                std::move(request),
                4096);

            // Assert request is not handled.
            bool responseReady = response.Wait(waitTimeout);
            UNIT_ASSERT(!responseReady);
        }

        // Assert that there are 3 incomplete operations left on shutdown.
        UNIT_ASSERT_VALUES_EQUAL(3, adapter->Shutdown(fastTimer));
        // Assert that the time has passed more than the shutdown timeout.
        UNIT_ASSERT_LT(now + shutdownTimeout, fastTimer->Now());

        now = fastTimer->Now();
        // We will complete one request for every fastTimer->Now() call. Since
        // these calls will occur in the Shutdown(), we will simulate the
        // successful waiting for requests to be completed during Shutdown().
        fastTimer->AddOnTickCallback(
            [&]() { readPromise.SetValue(NProto::TReadBlocksLocalResponse{}); });
        fastTimer->AddOnTickCallback(
            [&]() { writePromise.SetValue(NProto::TWriteBlocksResponse{}); });
        fastTimer->AddOnTickCallback(
            [&]() { zeroPromise.SetValue(NProto::TZeroBlocksResponse{}); });

        // Assert that all requests completed during shutdown.
        UNIT_ASSERT_VALUES_EQUAL(0, adapter->Shutdown(fastTimer));

        // Assert that the time has passed less than the shutdown timeout.
        UNIT_ASSERT_GT(now + shutdownTimeout, fastTimer->Now());
    }

    void DoShouldOptimizeVoidBlocks(
        bool storageUseOwnAllocator,
        ui32 blockSize,
        bool normalize,
        bool enableOptimization)
    {
        auto storage = std::make_shared<TTestStorage>();
        storage->DoAllocations = storageUseOwnAllocator;

        //  The first block will be filled with zeros and the second with ones
        //  and so on.
        storage->ReadBlocksLocalHandler =
            [blockSize](
                auto ctx,
                std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);

            const ui64 requestByteCount =
                static_cast<ui64>(request->GetBlocksCount()) *
                request->BlockSize;

            TString data;
            data.resize(requestByteCount, 0);
            for (ui64 offset = 0; offset < data.size(); offset += blockSize) {
                std::memset(
                    const_cast<char*>(data.data() + offset),
                    offset / blockSize,
                    blockSize);
            }

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            SgListCopy(TBlockDataRef{data.data(), data.size()}, guard.Get());

            NProto::TReadBlocksLocalResponse r;
            *r.MutableError() = MakeError(S_OK);
            return MakeFuture<>(std::move(r));
        };

        auto adapter = std::make_shared<TStorageAdapter>(
            storage,
            4_KB,   // storageBlockSize
            normalize,
            TDuration::Zero(),   // request timeout
            TDuration::Zero()    // shutdown timeout
        );

        {   // Requesting the reading of two blocks. Since the first block will
            // be filled with zeros, it should be optimized and will return an
            // empty buffer.
            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->MutableHeaders()->SetOptimizeNetworkTransfer(
                enableOptimization
                    ? NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS
                    : NProto::EOptimizeNetworkTransfer::NO_OPTIMIZATION);
            request->SetBlocksCount(2);

            auto response = adapter->ReadBlocks(
                Now(),
                MakeIntrusive<TCallContext>(),
                std::move(request),
                blockSize,
                {}   // no data buffer
            );
            response.Wait();
            const auto& value = response.GetValue();
            UNIT_ASSERT_EQUAL(S_OK, value.GetError().GetCode());

            const auto& data = value.GetBlocks().GetBuffers();
            UNIT_ASSERT_EQUAL(
                enableOptimization ? 0 : blockSize,
                data[0].size());   // First block should be optimized
            UNIT_ASSERT_EQUAL(blockSize, data[1].size());
        }
    }

    Y_UNIT_TEST(ShouldOptimizeZeroBlocks)
    {
        const ui32 blockSizes[] = {4_KB, 8_KB, 16_KB, 32_KB, 64_KB, 128_KB};
        for (auto blockSize: blockSizes) {
            DoShouldOptimizeVoidBlocks(false, blockSize, false, false);
            DoShouldOptimizeVoidBlocks(false, blockSize, false, true);
            DoShouldOptimizeVoidBlocks(false, blockSize, true, false);
            DoShouldOptimizeVoidBlocks(false, blockSize, true, true);

            DoShouldOptimizeVoidBlocks(true, blockSize, false, false);
            DoShouldOptimizeVoidBlocks(true, blockSize, false, true);
            DoShouldOptimizeVoidBlocks(true, blockSize, true, false);
            DoShouldOptimizeVoidBlocks(true, blockSize, true, true);
        }
    }

    void DoShouldReadCorrectData(bool normalize, bool useDataBuffer)
    {
        auto storage = std::make_shared<TTestStorage>();

        auto firstBlock = TString(4096, 'A');
        auto secondBlock = TString(4096, 'B');

        storage->ReadBlocksLocalHandler =
            [&firstBlock, &secondBlock](
                auto ctx,
                std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);

            UNIT_ASSERT_EQUAL(request->GetBlocksCount(), 2);

            TString blocks = firstBlock + secondBlock;
            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            SgListCopy(
                TBlockDataRef{blocks.data(), blocks.size()},
                guard.Get());

            NProto::TReadBlocksLocalResponse r;
            *r.MutableError() = MakeError(S_OK);
            return MakeFuture<>(std::move(r));
        };

        auto adapter = std::make_shared<TStorageAdapter>(
            storage,
            4_KB,   // storageBlockSize
            normalize,
            TDuration::Zero(),   // request timeout
            TDuration::Zero()    // shutdown timeout
        );

        {
            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            request->SetBlocksCount(2);

            TString data(4096 * 2 , '\0');
            TStringBuf dataBuf;

            if (useDataBuffer) {
                dataBuf = TStringBuf(data);
            }

            auto response = adapter->ReadBlocks(
                Now(),
                MakeIntrusive<TCallContext>(),
                std::move(request),
                4096, // block size
                dataBuf
            );

            response.Wait();
            const auto& value = response.GetValue();
            UNIT_ASSERT_EQUAL(S_OK, value.GetError().GetCode());

            if (useDataBuffer) {
                UNIT_ASSERT_EQUAL(dataBuf.SubString(0, 4096), firstBlock);
                UNIT_ASSERT_EQUAL(dataBuf.SubString(4096, 4096), secondBlock);
            } else {
                const auto& blockBuffers = value.GetBlocks().GetBuffers();
                UNIT_ASSERT_EQUAL(blockBuffers.size(), 2);
                UNIT_ASSERT_EQUAL(firstBlock, blockBuffers[0]);
                UNIT_ASSERT_EQUAL(secondBlock, blockBuffers[1]);
            }

        }
    }

    Y_UNIT_TEST(ShouldReadCorrectData)
    {
        DoShouldReadCorrectData(false, true);
        DoShouldReadCorrectData(false, false);
        DoShouldReadCorrectData(true, true);
        DoShouldReadCorrectData(true, false);
    }

}

}   // namespace NCloud::NBlockStore
