#include "overlapping_requests_guard_service.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlockSize = 4096;
const char* const DefaultDiskId = "vol1";

struct TTestBlockStore: public TBlockStoreImpl<TTestBlockStore, IBlockStore>
{
    TMap<ui64, TPromise<NProto::TReadBlocksResponse>> ReadBlocksPromises;
    TMap<ui64, TPromise<NProto::TReadBlocksLocalResponse>>
        ReadBlocksLocalPromises;
    TMap<ui64, TPromise<NProto::TWriteBlocksResponse>> WriteBlocksPromises;
    TMap<ui64, TPromise<NProto::TWriteBlocksLocalResponse>>
        WriteBlocksLocalPromises;
    TMap<ui64, TPromise<NProto::TZeroBlocksResponse>> ZeroBlocksPromises;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void Start() override
    {}

    void Stop() override
    {}

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request) override
    {
        Y_UNUSED(request);

        return ReadBlocksPromises[callContext->RequestId] =
                   NewPromise<NProto::TReadBlocksResponse>();
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(request);

        return ReadBlocksLocalPromises[callContext->RequestId] =
                   NewPromise<NProto::TReadBlocksLocalResponse>();
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        Y_UNUSED(request);

        return WriteBlocksPromises[callContext->RequestId] =
                   NewPromise<NProto::TWriteBlocksResponse>();
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(request);

        return WriteBlocksLocalPromises[callContext->RequestId] =
                   NewPromise<NProto::TWriteBlocksLocalResponse>();
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(request);

        return ZeroBlocksPromises[callContext->RequestId] =
                   NewPromise<NProto::TZeroBlocksResponse>();
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TMountVolumeResponse response;
        response.MutableVolume()->SetDiskId(request->GetDiskId());
        response.MutableVolume()->SetBlockSize(DefaultBlockSize);

        return NThreading::MakeFuture(std::move(response));
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<typename TMethod::TResponse>();
    }
};

struct TTestEnvironment
{
    std::shared_ptr<TTestBlockStore> Storage;
    std::shared_ptr<IBlockStore> OverlappingGuard;

    TString TestData;
    TSgList Sglist10;
    TSgList Sglist5;

    TTestEnvironment()
    {
        Storage = std::make_shared<TTestBlockStore>();
        OverlappingGuard = CreateOverlappingRequestsGuardsService(Storage);

        TestData.resize(10 * DefaultBlockSize, 0);
        Sglist10.push_back(
            {TBlockDataRef(TestData.data(), 10 * DefaultBlockSize)});
        Sglist5.push_back(
            {TBlockDataRef(TestData.data(), 5 * DefaultBlockSize)});
    }

    void MountVolume(const TString& diskId = DefaultDiskId)
    {
        auto request = std::make_shared<NProto::TMountVolumeRequest>();
        request->SetDiskId(diskId);

        auto future = OverlappingGuard->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto response = future.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    void SetupRequest(
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        TBlockRange64 range)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        request->SetBlocksCount(range.Size());
    }

    void SetupRequest(
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        TBlockRange64 range)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        request->SetBlocksCount(range.Size());
        TSgList sglist;
        sglist.push_back(
            {TBlockDataRef(TestData.data(), range.Size() * DefaultBlockSize)});
        request->Sglist = TGuardedSgList(std::move(sglist));
    }

    void SetupRequest(
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        TBlockRange64 range)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        for (size_t i = 0; i < range.Size(); ++i) {
            request->MutableBlocks()->AddBuffers(
                TString(DefaultBlockSize, 'a'));
        }
    }

    void SetupRequest(
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        TBlockRange64 range)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);

        request->BlocksCount = range.Size();

        TSgList sglist;
        sglist.push_back(
            {TBlockDataRef(TestData.data(), range.Size() * DefaultBlockSize)});
        request->Sglist = TGuardedSgList(std::move(sglist));
    }

    void SetupRequest(
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        TBlockRange64 range)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        request->SetBlocksCount(range.Size());
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
        env.SetupRequest(request1, range);
        auto future1 = env.OverlappingGuard->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run second request
        auto request2 = std::make_shared<NProto::TReadBlocksRequest>();
        env.SetupRequest(request2, range);
        auto future2 = env.OverlappingGuard->ReadBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Storage->ReadBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Storage->ReadBlocksPromises.size());

        // Complete second request
        env.Storage->ReadBlocksPromises[2].SetValue(
            NProto::TReadBlocksResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete first request
        env.Storage->ReadBlocksLocalPromises[1].SetValue(
            NProto::TReadBlocksLocalResponse());
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
        env.MountVolume();

        auto range = TBlockRange64::WithLength(1, 10);

        // Run first write request
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request1, range);
        auto future1 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run read request #2
        auto request2 = std::make_shared<NProto::TReadBlocksLocalRequest>();
        env.SetupRequest(request2, range);
        auto future2 = env.OverlappingGuard->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Storage->WriteBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Storage->ReadBlocksLocalPromises.size());

        // Complete second request
        env.Storage->ReadBlocksLocalPromises[2].SetValue(
            NProto::TReadBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete first request
        env.Storage->WriteBlocksLocalPromises[1].SetValue(
            NProto::TWriteBlocksLocalResponse());
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
        env.MountVolume();

        auto range1 = TBlockRange64::WithLength(1, 10);
        auto range2 = TBlockRange64::WithLength(12, 5);

        // Run first write request
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request1, range1);
        auto future1 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run second write request #2
        auto request2 = std::make_shared<NProto::TWriteBlocksRequest>();
        env.SetupRequest(request2, range2);
        auto future2 = env.OverlappingGuard->WriteBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Storage->WriteBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(1, env.Storage->WriteBlocksPromises.size());

        // Complete second request
        env.Storage->WriteBlocksPromises[2].SetValue(
            NProto::TWriteBlocksResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete first request
        env.Storage->WriteBlocksLocalPromises[1].SetValue(
            NProto::TWriteBlocksLocalResponse());
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
        env.MountVolume();

        auto range = TBlockRange64::WithLength(1, 10);

        // Run write request #1
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request1, range);
        auto future1 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Complete request #1
        env.Storage->WriteBlocksLocalPromises[1].SetValue(
            NProto::TWriteBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.GetError().code(),
            FormatError(result1.GetError()));

        // Run write request #2
        auto request2 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request2, range);
        auto future2 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Complete request #2
        env.Storage->WriteBlocksLocalPromises[2].SetValue(
            NProto::TWriteBlocksLocalResponse());
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
        env.MountVolume();

        // Run write request [1, 10]
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request1, TBlockRange64::WithLength(1, 10));
        auto future1 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request covered by first write request
        auto request2 = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request2, TBlockRange64::WithLength(1, 5));
        auto future2 = env.OverlappingGuard->ZeroBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request covered by first write request
        auto request3 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request3, TBlockRange64::WithLength(5, 5));
        auto future3 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(3ul),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Storage->WriteBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->WriteBlocksLocalPromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(0, env.Storage->ZeroBlocksPromises.size());

        // Complete first write request
        auto response = NProto::TWriteBlocksLocalResponse();
        *response.MutableError() = MakeError(E_REJECTED);
        env.Storage->WriteBlocksLocalPromises[1].SetValue(std::move(response));
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
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Storage->WriteBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(0, env.Storage->ZeroBlocksPromises.size());
    }

    Y_UNIT_TEST(OverlappedWritesShouldBeDelayed)
    {
        //      [-------1------]
        //   [--2--]
        //                   [--3--]
        //
        // Request #1 delays the execution of requests 2 and 3.

        TTestEnvironment env;
        env.MountVolume();

        // Run write request [1, 10]
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request1, TBlockRange64::WithLength(1, 10));
        auto future1 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request overlapped with first write request
        auto request2 = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request2, TBlockRange64::WithLength(0, 5));
        auto future2 = env.OverlappingGuard->ZeroBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request overlapped with first write request
        auto request3 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request3, TBlockRange64::WithLength(9, 5));
        auto future3 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(3ul),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.Storage->WriteBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->WriteBlocksLocalPromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(0, env.Storage->ZeroBlocksPromises.size());

        // Complete first write request with error
        auto response = NProto::TWriteBlocksLocalResponse();
        *response.MutableError() = MakeError(E_REJECTED);
        env.Storage->WriteBlocksLocalPromises[1].SetValue(std::move(response));
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.GetError().code(),
            FormatError(result1.GetError()));

        // Zero and second write requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.Storage->WriteBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->WriteBlocksLocalPromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->WriteBlocksLocalPromises.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(1, env.Storage->ZeroBlocksPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->ZeroBlocksPromises.contains(2));

        // Complete zero request
        env.Storage->ZeroBlocksPromises[2].SetValue(
            NProto::TZeroBlocksResponse{});
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete write request
        env.Storage->WriteBlocksLocalPromises[3].SetValue(
            NProto::TWriteBlocksLocalResponse{});
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
        env.MountVolume();

        // Run write request [1, 10]
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request1, TBlockRange64::WithLength(1, 10));
        auto future1 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request overlapped with first write request
        auto request2 = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request2, TBlockRange64::WithLength(9, 5));
        auto future2 = env.OverlappingGuard->ZeroBlocks(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request overlapped with zero request
        auto request3 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request3, TBlockRange64::WithLength(12, 5));
        request3->Sglist = TGuardedSgList(env.Sglist5);

        auto future3 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(3ul),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Both write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.Storage->WriteBlocksLocalPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->WriteBlocksLocalPromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->WriteBlocksLocalPromises.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(0, env.Storage->ZeroBlocksPromises.size());

        // Complete first write request with error
        auto response = NProto::TWriteBlocksLocalResponse();
        *response.MutableError() = MakeError(E_REJECTED);
        env.Storage->WriteBlocksLocalPromises[1].SetValue(std::move(response));
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.GetError().code(),
            FormatError(result1.GetError()));

        // Zero request should delayed since it overlapped with second write
        UNIT_ASSERT_VALUES_EQUAL(0, env.Storage->ZeroBlocksPromises.size());

        // Complete write request with S_OK
        env.Storage->WriteBlocksLocalPromises[3].SetValue(
            NProto::TWriteBlocksLocalResponse{});
        const auto& result3 = future3.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result3.GetError().code(),
            FormatError(result3.GetError()));

        // Zero request should running
        UNIT_ASSERT_VALUES_EQUAL(1, env.Storage->ZeroBlocksPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            env.Storage->ZeroBlocksPromises.contains(2));

        // Complete zero request
        env.Storage->ZeroBlocksPromises[2].SetValue(
            NProto::TZeroBlocksResponse{});
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));
    }

    Y_UNIT_TEST(ShouldReplyErrorWithoutMount)
    {
        TTestEnvironment env;
        auto range = TBlockRange64::WithLength(1, 10);

        // Run first write request
        auto request1 = std::make_shared<NProto::TWriteBlocksRequest>();
        env.SetupRequest(request1, range);
        auto future1 = env.OverlappingGuard->WriteBlocks(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(true, future1.HasValue());

        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.GetError().code(),
            FormatError(result1.GetError()));
    }

    Y_UNIT_TEST(DisksShouldHandledIndependently)
    {
        // [-----1-----]
        // [-----2-----]
        //
        // The execution of write request #2 volume#2 is not delayed by the
        // execution of write request #1 in volume #1

        TTestEnvironment env;
        env.MountVolume();
        env.MountVolume("vol2");

        auto range = TBlockRange64::WithLength(1, 10);

        // Run first write request
        auto request1 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request1, range);
        auto future1 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(1ul),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run second request #2
        auto request2 = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request2, range);
        request2->SetDiskId("vol2");
        auto future2 = env.OverlappingGuard->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(2ul),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.Storage->WriteBlocksLocalPromises.size());

        // Complete second request
        env.Storage->WriteBlocksLocalPromises[2].SetValue(
            NProto::TWriteBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.GetError().code(),
            FormatError(result2.GetError()));

        // Complete first request
        env.Storage->WriteBlocksLocalPromises[1].SetValue(
            NProto::TWriteBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.GetError().code(),
            FormatError(result1.GetError()));
    }
}

}   // namespace NCloud::NBlockStore
