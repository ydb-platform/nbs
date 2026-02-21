#include "overlapped_requests_guard_wrapper.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvironment
{
    std::shared_ptr<TTestStorage> Storage;
    std::shared_ptr<IStorage> Wrapper;

    TMap<ui64, TPromise<NProto::TReadBlocksLocalResponse>> ReadPromises;
    TMap<ui64, TPromise<NProto::TWriteBlocksLocalResponse>> WritePromises;
    TMap<ui64, TPromise<NProto::TZeroBlocksResponse>> ZeroPromises;

    TString TestData;
    TSgList Sglist10;
    TSgList Sglist5;

    TTestEnvironment()
    {
        Storage = std::make_shared<TTestStorage>();
        Wrapper = CreateOverlappingRequestsGuardStorageWrapper(Storage);

        Storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
            -> TFuture<NProto::TReadBlocksLocalResponse>
        {
            Y_UNUSED(request);

            return ReadPromises[callContext->RequestId] =
                       NewPromise<NProto::TReadBlocksLocalResponse>();
        };

        Storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
            -> TFuture<NProto::TWriteBlocksLocalResponse>
        {
            Y_UNUSED(request);

            return WritePromises[callContext->RequestId] =
                       NewPromise<NProto::TWriteBlocksLocalResponse>();
        };

        Storage->ZeroBlocksHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<NProto::TZeroBlocksRequest> request)
            -> TFuture<NProto::TZeroBlocksResponse>
        {
            Y_UNUSED(request);

            return ZeroPromises[callContext->RequestId] =
                       NewPromise<NProto::TZeroBlocksResponse>();
        };

        TestData.resize(10 * DefaultBlockSize, 0);
        Sglist10.push_back(
            {TBlockDataRef(TestData.data(), 10 * DefaultBlockSize)});
        Sglist5.push_back(
            {TBlockDataRef(TestData.data(), 5 * DefaultBlockSize)});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOverlappingRequestsGuardStorageWrapperTest)
{
    Y_UNIT_TEST(OverlappingReadsShouldNotBlockEachOther)
    {
        // [-----1-----]
        // [-----2-----]
        //
        // Read requests are executed independently and do not delay each
        // other's execution.

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run first request
        auto request1 = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request1->SetStartIndex(range.Start);
        request1->SetBlocksCount(range.Size());
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run second request
        auto request2 = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request2->SetStartIndex(range.Start);
        request2->SetBlocksCount(range.Size());
        request2->Sglist = TGuardedSgList(env.Sglist10);
        auto future2 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(2, env.ReadPromises.size());

        // Complete second request
        env.ReadPromises[2].SetValue(NProto::TReadBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete first request
        env.ReadPromises[1].SetValue(NProto::TReadBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.GetError().code(),
            FormatError(result1.GetError()));
    }

    Y_UNIT_TEST(WritesSholdNotDelayOverlappingReads)
    {
        // [-----1-----]
        // [-----2-----]
        //
        // The execution of read request #2 is not delayed by the execution of
        // write request #1

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run first write request
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request1->SetStartIndex(range.Start);
        request1->BlocksCount = range.Size();
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run read request #2
        auto request2 = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request2->SetStartIndex(range.Start);
        request2->SetBlocksCount(range.Size());
        request2->Sglist = TGuardedSgList(env.Sglist10);
        auto future2 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(1, env.ReadPromises.size());

        // Complete second request
        env.ReadPromises[2].SetValue(NProto::TReadBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete first request
        env.WritePromises[1].SetValue(NProto::TWriteBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.GetError().code(),
            FormatError(result1.GetError()));
    }

    Y_UNIT_TEST(ShouldNotDelayNonOverlappingWrites)
    {
        // [-----1-----]
        //              [-----2-----]
        //
        // The execution of independent write request #2 is not delayed by the
        // execution of write request #1

        TTestEnvironment env;

        auto range1 = TBlockRange64::WithLength(1, 10);
        auto range2 = TBlockRange64::WithLength(12, 5);

        // Run first write request
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request1->SetStartIndex(range1.Start);
        request1->BlocksCount = range1.Size();
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run second write request #2
        auto request2 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request2->SetStartIndex(range2.Start);
        request2->BlocksCount = range2.Size();
        request2->Sglist = TGuardedSgList(env.Sglist5);
        auto future2 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(2, env.WritePromises.size());

        // Complete second request
        env.WritePromises[2].SetValue(NProto::TWriteBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete first request
        env.WritePromises[1].SetValue(NProto::TWriteBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.GetError().code(),
            FormatError(result1.GetError()));
    }

    Y_UNIT_TEST(ShouldNotDelaySequentialWrites)
    {
        // [-----1-----]
        // [-----2-----]
        //
        // The execution of write request #2 is not delayed when write request
        // #1 already completed

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run write request #1
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request1->SetStartIndex(range.Start);
        request1->BlocksCount = range.Size();
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Complete request #1
        env.WritePromises[1].SetValue(NProto::TWriteBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.GetError().code(),
            FormatError(result1.GetError()));

        // Run write request #2
        auto request2 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request2->SetStartIndex(range.Start);
        request2->BlocksCount = range.Size();
        request2->Sglist = TGuardedSgList(env.Sglist5);
        auto future2 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Complete request #2
        env.WritePromises[2].SetValue(NProto::TWriteBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));
    }

    Y_UNIT_TEST(CoveredWritesShouldNotBeExecuted)
    {
        // [------1-----]
        // [--2--]
        //        [--3--]
        //
        // Request #1 should delay the execution of requests #2 and #3. When
        // request #1 completes, it will result in requests #2 and #3 being
        // completed without real execution.

        TTestEnvironment env;

        // Run write request [1, 10]
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request1->SetStartIndex(1);
        request1->BlocksCount = 10;
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request covered by first write request
        auto request2 = std::make_shared<NProto::TZeroBlocksRequest>();
        request2->SetStartIndex(1);
        request2->SetBlocksCount(5);
        auto future2 = env.Wrapper->ZeroBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request covered by first write request
        auto request3 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request3->SetStartIndex(5);
        request3->BlocksCount = 5;
        request3->Sglist = TGuardedSgList(env.Sglist5);

        auto future3 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(3ul),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete first write request
        auto response = NProto::TWriteBlocksLocalResponse();
        *response.MutableError() = MakeError(E_REJECTED);
        env.WritePromises[1].SetValue(std::move(response));
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.GetError().code(),
            FormatError(result1.GetError()));

        // The zeroing should be completed too with same error.
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // The second write should be completed too with same error.
        const auto& result3 = future3.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result3.GetError().code(),
            FormatError(result3.GetError()));

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());
    }

    Y_UNIT_TEST(OverlappedWritesShouldBeDelayed)
    {
        //      [-------1------]
        //   [--2--]
        //                   [--3--]
        //
        // Request #1 delays the execution of requests 2 and 3.

        TTestEnvironment env;

        TString data;
        data.resize(10 * DefaultBlockSize, 0);
        TSgList sglist{TBlockDataRef(data.data(), data.size())};
        TSgList sglist5{TBlockDataRef(data.data(), 5 * DefaultBlockSize)};

        // Run write request [1, 10]
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request1->SetStartIndex(1);
        request1->BlocksCount = 10;
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request overlapped with first write request
        auto request2 = std::make_shared<NProto::TZeroBlocksRequest>();
        request2->SetStartIndex(0);
        request2->SetBlocksCount(5);
        auto future2 = env.Wrapper->ZeroBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request overlapped with first write request
        auto request3 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request3->SetStartIndex(9);
        request3->BlocksCount = 5;
        request3->Sglist = TGuardedSgList(env.Sglist5);

        auto future3 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(3ul),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete first write request with error
        auto response = NProto::TWriteBlocksLocalResponse();
        *response.MutableError() = MakeError(E_REJECTED);
        env.WritePromises[1].SetValue(std::move(response));
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.GetError().code(),
            FormatError(result1.GetError()));

        // Zero and second write requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(2, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.ZeroPromises.contains(2));

        // Complete zero request
        env.ZeroPromises[2].SetValue(NProto::TZeroBlocksResponse{});
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete write request
        env.WritePromises[3].SetValue(NProto::TWriteBlocksLocalResponse{});
        const auto& result3 = future3.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result3.GetError().code(),
            FormatError(result3.GetError()));
    }

    Y_UNIT_TEST(OverlappedRunningWritesShouldDelayOtherWrites)
    {
        // [---1---]
        //     [---2---]
        //          [---3---]
        // Request #1 should delay the execution of request #2. But request # 2
        // should not delay the execution of request #3 while it is itself
        // deferred.

        TTestEnvironment env;

        // Run write request [1, 10]
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request1->SetStartIndex(1);
        request1->BlocksCount = 10;
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request overlapped with first write request
        auto request2 = std::make_shared<NProto::TZeroBlocksRequest>();
        request2->SetStartIndex(9);
        request2->SetBlocksCount(5);
        auto future2 = env.Wrapper->ZeroBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request overlapped with zero request
        auto request3 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request3->SetStartIndex(12);
        request3->BlocksCount = 5;
        request3->Sglist = TGuardedSgList(env.Sglist5);

        auto future3 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(3ul),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Both write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(2, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete first write request with error
        auto response = NProto::TWriteBlocksLocalResponse();
        *response.MutableError() = MakeError(E_REJECTED);
        env.WritePromises[1].SetValue(std::move(response));
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.GetError().code(),
            FormatError(result1.GetError()));

        // Zero request should delayed since it overlapped with second write
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete write request with S_OK
        env.WritePromises[3].SetValue(NProto::TWriteBlocksLocalResponse{});
        const auto& result3 = future3.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result3.GetError().code(),
            FormatError(result3.GetError()));

        // Zero request should running
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.ZeroPromises.contains(2));

        // Complete zero request
        env.ZeroPromises[2].SetValue(NProto::TZeroBlocksResponse{});
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));
    }
}

}   // namespace NCloud::NBlockStore
