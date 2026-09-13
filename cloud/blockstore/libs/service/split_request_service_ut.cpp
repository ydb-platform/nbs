#include "split_request_service.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/json/json_reader.h>
#include <util/datetime/cputimer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <exception>
#include <functional>
#include <future>
#include <thread>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlockSize = 2;
constexpr ui64 StripeSize = 6;
const char* const DefaultDiskId = "vol1";

////////////////////////////////////////////////////////////////////////////////

TString GetDataFromResponse(const NProto::TReadBlocksResponse& response)
{
    TString result;
    for (const auto& buffer: response.GetBlocks().GetBuffers()) {
        result += buffer;
    }
    return result;
}

TString GetDataFromRequest(std::shared_ptr<NProto::TWriteBlocksRequest> request)
{
    TString result;
    for (const auto& buffer: request->GetBlocks().GetBuffers()) {
        result += buffer;
    }
    return result;
}

TString GetDataFromRequest(
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return {};
    }

    TString result;
    for (const auto& block: guard.Get()) {
        result += block.AsStringBuf();
    }
    return result;
}

void CopyToGuardedSgList(const TString& data, TGuardedSgList& sglist)
{
    auto guard = sglist.Acquire();
    if (!guard) {
        return;
    }
    SgListCopy(TBlockDataRef{data.data(), data.size()}, guard.Get());
}

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore: public TBlockStoreImpl<TTestBlockStore, IBlockStore>
{
    template <typename TMethod>
    struct TRequestInfo
    {
        using TRequest = TMethod::TRequest;
        using TResponse = TMethod::TResponse;

        TPromise<TResponse> Promise = NewPromise<TResponse>();
        std::shared_ptr<TRequest> Request;
        TCallContextPtr CallContext;
    };

    TMap<
        TBlockRange64,
        TRequestInfo<TBlockStoreReadBlocksMethod>,
        TBlockRangeComparator>
        ReadBlocksPromises;
    TMap<
        TBlockRange64,
        TRequestInfo<TBlockStoreReadBlocksLocalMethod>,
        TBlockRangeComparator>
        ReadBlocksLocalPromises;
    TMap<
        TBlockRange64,
        TRequestInfo<TBlockStoreWriteBlocksMethod>,
        TBlockRangeComparator>
        WriteBlocksPromises;
    TMap<
        TBlockRange64,
        TRequestInfo<TBlockStoreWriteBlocksLocalMethod>,
        TBlockRangeComparator>
        WriteBlocksLocalPromises;
    TMap<
        TBlockRange64,
        TRequestInfo<TBlockStoreZeroBlocksMethod>,
        TBlockRangeComparator>
        ZeroBlocksPromises;

    std::optional<NProto::TError> SyncZeroBlocksError;
    std::function<void(TCallContextPtr)> OnReadBlocks;

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
        Y_UNUSED(callContext);

        if (OnReadBlocks) {
            OnReadBlocks(callContext);
        }

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            ReadBlocksPromises[range] = {.Request = std::move(request),
                                        .CallContext = std::move(callContext)};
        return info.Promise;
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            ReadBlocksLocalPromises[range] = {.Request = std::move(request),
                                        .CallContext = std::move(callContext)};
        return info.Promise;
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        Y_UNUSED(callContext);

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            WriteBlocksPromises[range] = {.Request = std::move(request),
                                        .CallContext = std::move(callContext)};
        return info.Promise;
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            WriteBlocksLocalPromises[range] = {.Request = std::move(request),
                                        .CallContext = std::move(callContext)};
        return info.Promise;
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);

        if (SyncZeroBlocksError) {
            NProto::TZeroBlocksResponse response;
            *response.MutableError() = *SyncZeroBlocksError;
            return MakeFuture(std::move(response));
        }

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            ZeroBlocksPromises[range] = {.Request = std::move(request),
                                        .CallContext = std::move(callContext)};
        return info.Promise;
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TMountVolumeResponse response;
        auto& volume = *response.MutableVolume();
        volume.SetDiskId(request->GetDiskId());
        volume.SetBlockSize(DefaultBlockSize);
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        auto& device = *response.MutableVolume()->MutableDevices()->Add();
        device.SetBlockCount(StripeSize);

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
    std::shared_ptr<IBlockStore> SplitRequestService;

    TString TestData;
    TSgList Sglist10;
    TSgList Sglist5;

    TTestEnvironment()
    {
        Storage = std::make_shared<TTestBlockStore>();
        SplitRequestService = CreateSplitRequestService(Storage);

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

        auto future = SplitRequestService->MountVolume(
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
        TBlockRange64 range,
        TString* data)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        request->SetBlocksCount(range.Size());
        request->SetBlockSize(DefaultBlockSize);
        request->Sglist = TGuardedSgList(
            {TBlockDataRef(data->data(), range.Size() * DefaultBlockSize)});
        UNIT_ASSERT_VALUES_EQUAL(range.Size() * DefaultBlockSize, data->size());
    }

    void SetupRequest(
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        TBlockRange64 range,
        const TString& data)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        for (size_t i = 0; i < range.Size(); ++i) {
            request->MutableBlocks()->AddBuffers(
                data.substr(i * DefaultBlockSize, DefaultBlockSize));
        }
        UNIT_ASSERT_VALUES_EQUAL(range.Size() * DefaultBlockSize, data.size());
    }

    void SetupRequest(
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        TBlockRange64 range,
        const TString& data)
    {
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        request->SetBlockSize(DefaultBlockSize);
        request->BlocksCount = range.Size();

        TSgList sglist;
        sglist.push_back(
            {TBlockDataRef(data.data(), range.Size() * DefaultBlockSize)});
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

Y_UNIT_TEST_SUITE(TSplitRequestServiceTest)
{
    Y_UNIT_TEST(ShouldSplitReadBlocks)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run ReadBlocks request
        auto request = std::make_shared<NProto::TReadBlocksRequest>();
        env.SetupRequest(request, range);
        auto future1 = env.SplitRequestService->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two reads after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, testBlockStore.ReadBlocksPromises.size());
        auto* firstRead = testBlockStore.ReadBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondRead = testBlockStore.ReadBlocksPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstRead != nullptr);
        UNIT_ASSERT(secondRead != nullptr);

        // Complete first read
        {
            NProto::TReadBlocksResponse response;
            response.MutableBlocks()->AddBuffers("aabbccddee");
            firstRead->Promise.SetValue(std::move(response));
        }

        // Complete second read
        {
            NProto::TReadBlocksResponse response;
            response.MutableBlocks()->AddBuffers("ffgghhjjkk");
            secondRead->Promise.SetValue(std::move(response));
        }

        // Should get response for user read
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            "aabbccddeeffgghhjjkk",
            GetDataFromResponse(result));
    }

    Y_UNIT_TEST(ShouldSplitReadBlocksLocal)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        TString data = "xxxxxxxxxxxxxxxxxxxx";
        auto range = TBlockRange64::WithLength(1, 10);

        // Run ReadBlocks request
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        env.SetupRequest(request, range, &data);
        auto future1 = env.SplitRequestService->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two reads after splitting
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            testBlockStore.ReadBlocksLocalPromises.size());
        auto* firstRead = testBlockStore.ReadBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondRead = testBlockStore.ReadBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstRead != nullptr);
        UNIT_ASSERT(secondRead != nullptr);

        // Complete first read
        {
            NProto::TReadBlocksLocalResponse response;
            CopyToGuardedSgList("aabbccddee", firstRead->Request->Sglist);
            firstRead->Promise.SetValue(std::move(response));
        }

        // Complete second read
        {
            NProto::TReadBlocksLocalResponse response;
            CopyToGuardedSgList("ffgghhjjkk", secondRead->Request->Sglist);
            secondRead->Promise.SetValue(std::move(response));
        }

        // Should get response for user read
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL("aabbccddeeffgghhjjkk", data);
    }

    Y_UNIT_TEST(ShouldSplitWriteBlocks)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        const TString data = "aabbccddeeffgghhjjkk";
        auto range =
            TBlockRange64::WithLength(1, data.size() / DefaultBlockSize);

        // Run WriteBlocks request
        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        env.SetupRequest(request, range, data);
        auto future1 = env.SplitRequestService->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two writes after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, testBlockStore.WriteBlocksPromises.size());
        auto* firstWrite = testBlockStore.WriteBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondWrite = testBlockStore.WriteBlocksPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstWrite != nullptr);
        UNIT_ASSERT(secondWrite != nullptr);

        // Complete first write
        {
            UNIT_ASSERT_VALUES_EQUAL(
                "aabbccddee",
                GetDataFromRequest(firstWrite->Request));
            firstWrite->Promise.SetValue(NProto::TWriteBlocksResponse());
        }

        // Complete second write
        {
            UNIT_ASSERT_VALUES_EQUAL(
                "ffgghhjjkk",
                GetDataFromRequest(secondWrite->Request));
            secondWrite->Promise.SetValue(NProto::TWriteBlocksResponse());
        }

        // Should get response for user write
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldSplitWriteBlocksLocal)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        const TString data = "aabbccddeeffgghhjjkk";
        auto range =
            TBlockRange64::WithLength(1, data.size() / DefaultBlockSize);

        // Run WriteBlocksLocal request
        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request, range, data);
        auto future1 = env.SplitRequestService->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two writes after splitting
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            testBlockStore.WriteBlocksLocalPromises.size());
        auto* firstWrite = testBlockStore.WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondWrite = testBlockStore.WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstWrite != nullptr);
        UNIT_ASSERT(secondWrite != nullptr);

        // Complete first write
        {
            UNIT_ASSERT_VALUES_EQUAL(
                "aabbccddee",
                GetDataFromRequest(firstWrite->Request));
            firstWrite->Promise.SetValue(NProto::TWriteBlocksLocalResponse());
        }

        // Complete second write
        {
            UNIT_ASSERT_VALUES_EQUAL(
                "ffgghhjjkk",
                GetDataFromRequest(secondWrite->Request));
            secondWrite->Promise.SetValue(NProto::TWriteBlocksLocalResponse());
        }

        // Should get response for user write
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldSplitZeroBlocks)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run zero request
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request, range);
        auto future1 = env.SplitRequestService->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two zero-blocks requests after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, testBlockStore.ZeroBlocksPromises.size());
        auto* firstZero = testBlockStore.ZeroBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondZero = testBlockStore.ZeroBlocksPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstZero != nullptr);
        UNIT_ASSERT(secondZero != nullptr);

        // Complete first sub request
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(1, 5),
                TBlockRangeHelper::GetRange(
                    *firstZero->Request,
                    DefaultBlockSize));
            firstZero->Promise.SetValue(NProto::TZeroBlocksResponse());
        }

        // Complete second sub request
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(6, 5),
                TBlockRangeHelper::GetRange(
                    *secondZero->Request,
                    DefaultBlockSize));
            secondZero->Promise.SetValue(NProto::TZeroBlocksResponse());
        }

        // Should get response for user ZeroBlocks
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldHandleErrorInFirstSubRequest)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run zero request
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request, range);
        auto future1 = env.SplitRequestService->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two zero-blocks requests after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, testBlockStore.ZeroBlocksPromises.size());
        auto* firstZero = testBlockStore.ZeroBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondZero = testBlockStore.ZeroBlocksPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstZero != nullptr);
        UNIT_ASSERT(secondZero != nullptr);

        // Complete first request with error
        {
            NProto::TZeroBlocksResponse response;
            *response.MutableError() = MakeError(E_REJECTED);
            firstZero->Promise.SetValue(std::move(response));
        }

        // Complete second request with success
        {
            secondZero->Promise.SetValue(NProto::TZeroBlocksResponse());
        }

        // Should get response with error for user ZeroBlocks
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldHandleErrorInSecondSubRequest)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run zero request
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request, range);
        auto future1 = env.SplitRequestService->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two zero-blocks requests after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, testBlockStore.ZeroBlocksPromises.size());
        auto* firstZero = testBlockStore.ZeroBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondZero = testBlockStore.ZeroBlocksPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstZero != nullptr);
        UNIT_ASSERT(secondZero != nullptr);

        // Complete first request with success
        {
            firstZero->Promise.SetValue(NProto::TZeroBlocksResponse());
        }

        // Complete second request  with error
        {
            NProto::TZeroBlocksResponse response;
            *response.MutableError() = MakeError(E_ABORTED);
            secondZero->Promise.SetValue(std::move(response));
        }

        // Should get response with error for user ZeroBlocks
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ABORTED,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRecalculateChecksumOnWriteBlocksSplit)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        const TString data = "aabbccddeeffgghhjjkk";
        auto range =
            TBlockRange64::WithLength(1, data.size() / DefaultBlockSize);

        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        env.SetupRequest(request, range, data);
        *request->MutableChecksums()->Add() =
            CalculateChecksum(request->GetBlocks(), DefaultBlockSize);

        auto future = env.SplitRequestService->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(2, testBlockStore.WriteBlocksPromises.size());
        auto* firstWrite = testBlockStore.WriteBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondWrite = testBlockStore.WriteBlocksPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstWrite != nullptr);
        UNIT_ASSERT(secondWrite != nullptr);

        UNIT_ASSERT_VALUES_EQUAL(1, firstWrite->Request->ChecksumsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, secondWrite->Request->ChecksumsSize());

        NProto::TIOVector firstIov;
        for (size_t i = 0; i < 5; ++i) {
            firstIov.AddBuffers(
                data.substr(i * DefaultBlockSize, DefaultBlockSize));
        }
        NProto::TIOVector secondIov;
        for (size_t i = 5; i < 10; ++i) {
            secondIov.AddBuffers(
                data.substr(i * DefaultBlockSize, DefaultBlockSize));
        }
        const auto expectedFirst =
            CalculateChecksum(firstIov, DefaultBlockSize);
        const auto expectedSecond =
            CalculateChecksum(secondIov, DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(
            expectedFirst.GetChecksum(),
            firstWrite->Request->GetChecksums(0).GetChecksum());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedSecond.GetChecksum(),
            secondWrite->Request->GetChecksums(0).GetChecksum());

        UNIT_ASSERT_VALUES_UNEQUAL(
            firstWrite->Request->GetChecksums(0).GetChecksum(),
            secondWrite->Request->GetChecksums(0).GetChecksum());

        firstWrite->Promise.SetValue(NProto::TWriteBlocksResponse());
        secondWrite->Promise.SetValue(NProto::TWriteBlocksResponse());

        const auto& result = future.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRecalculateChecksumOnWriteBlocksLocalSplit)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        const TString data = "aabbccddeeffgghhjjkk";
        auto range =
            TBlockRange64::WithLength(1, data.size() / DefaultBlockSize);

        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request, range, data);

        {
            auto guard = request->Sglist.Acquire();
            *request->MutableChecksums()->Add() =
                CalculateChecksum(guard.Get());
        }

        auto future = env.SplitRequestService->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            testBlockStore.WriteBlocksLocalPromises.size());
        auto* firstWrite = testBlockStore.WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondWrite = testBlockStore.WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstWrite != nullptr);
        UNIT_ASSERT(secondWrite != nullptr);

        UNIT_ASSERT_VALUES_EQUAL(1, firstWrite->Request->ChecksumsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, secondWrite->Request->ChecksumsSize());

        TSgList firstSgList = {{data.data(), 5 * DefaultBlockSize}};
        TSgList secondSgList = {
            {data.data() + 5 * DefaultBlockSize, 5 * DefaultBlockSize}};
        const auto expectedFirst = CalculateChecksum(firstSgList);
        const auto expectedSecond = CalculateChecksum(secondSgList);

        UNIT_ASSERT_VALUES_EQUAL(
            expectedFirst.GetChecksum(),
            firstWrite->Request->GetChecksums(0).GetChecksum());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedSecond.GetChecksum(),
            secondWrite->Request->GetChecksums(0).GetChecksum());

        UNIT_ASSERT_VALUES_EQUAL(
            5 * DefaultBlockSize,
            firstWrite->Request->GetChecksums(0).GetByteCount());
        UNIT_ASSERT_VALUES_EQUAL(
            5 * DefaultBlockSize,
            secondWrite->Request->GetChecksums(0).GetByteCount());

        UNIT_ASSERT_VALUES_UNEQUAL(
            firstWrite->Request->GetChecksums(0).GetChecksum(),
            secondWrite->Request->GetChecksums(0).GetChecksum());

        firstWrite->Promise.SetValue(NProto::TWriteBlocksLocalResponse());
        secondWrite->Promise.SetValue(NProto::TWriteBlocksLocalResponse());

        const auto& result = future.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldNotFailWhenSubRequestCompletesSynchronously)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        // Make every ZeroBlocks sub-request complete synchronously with an
        // error.
        testBlockStore.SyncZeroBlocksError = MakeError(E_REJECTED, "sync fail");

        // Request that crosses a stripe boundary -> will be split.
        auto range = TBlockRange64::WithLength(1, 10);
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request, range);

        TFuture<NProto::TZeroBlocksResponse> future;
        UNIT_ASSERT_NO_EXCEPTION(
            future = env.SplitRequestService->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request)));

        // Future must be valid and already carry the propagated error.
        UNIT_ASSERT(future.Initialized());
        UNIT_ASSERT(future.HasValue());
        const auto& result = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldForwardRequestIfSplittingIsNotRequired)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        auto range = TBlockRange64::WithLength(1, 5);

        // Run zero request
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request, range);
        auto future1 = env.SplitRequestService->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get same zero-blocks request
        UNIT_ASSERT_VALUES_EQUAL(1, testBlockStore.ZeroBlocksPromises.size());
        auto* firstZero = testBlockStore.ZeroBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        UNIT_ASSERT(firstZero != nullptr);

        // Complete first request with success
        {
            firstZero->Promise.SetValue(NProto::TZeroBlocksResponse());
        }

        // Should get response for user ZeroBlocks
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldCancelSubRequestSglistsOnWriteBlocksLocalError)
    {
        TTestEnvironment env;
        env.MountVolume();
        TTestBlockStore& testBlockStore = *env.Storage;

        const TString data = "aabbccddeeffgghhjjkk";
        auto range =
            TBlockRange64::WithLength(1, data.size() / DefaultBlockSize);

        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        env.SetupRequest(request, range, data);
        auto future = env.SplitRequestService->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            testBlockStore.WriteBlocksLocalPromises.size());
        auto* firstWrite = testBlockStore.WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondWrite = testBlockStore.WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstWrite != nullptr);
        UNIT_ASSERT(secondWrite != nullptr);

        // Second sub-request sglist is valid before first error
        {
            auto guard = secondWrite->Request->Sglist.Acquire();
            UNIT_ASSERT(guard);
        }

        // Complete first sub-request with error
        {
            NProto::TWriteBlocksLocalResponse response;
            *response.MutableError() = MakeError(E_REJECTED);
            firstWrite->Promise.SetValue(std::move(response));
        }

        const auto& result = future.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result.GetError().GetCode(),
            FormatError(result.GetError()));

        // Second sub-request sglist must be closed after parent sglist is
        // closed
        {
            auto guard = secondWrite->Request->Sglist.Acquire();
            UNIT_ASSERT(!guard);
        }
    }

    Y_UNIT_TEST(ShouldReplyErrorWithoutMount)
    {
        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, 10);

        {
            // Run ZeroBlocks request
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            env.SetupRequest(request, range);
            auto future = env.SplitRequestService->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& result = future.GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                result.GetError().GetCode(),
                FormatError(result.GetError()));
        }

        {
            // Run ReadBlocks request

            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            env.SetupRequest(request, range);
            auto future = env.SplitRequestService->ReadBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& result = future.GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                result.GetError().GetCode(),
                FormatError(result.GetError()));

        }

        {
            // Run WriteBlocks request
            const TString data = "abcdefghij";
            auto range =
                TBlockRange64::WithLength(1, data.size() / DefaultBlockSize);
            auto request = std::make_shared<NProto::TWriteBlocksRequest>();
            env.SetupRequest(request, range, data);
            auto future = env.SplitRequestService->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& result = future.GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_BS_INVALID_SESSION,
                result.GetError().GetCode(),
                FormatError(result.GetError()));

        }
    }
    Y_UNIT_TEST(ShouldExposeCompleteSplitReadAndEarlyError)
    {
        for (bool fail: {false, true}) {
            TTestEnvironment env;
            env.MountVolume();
            auto root = CreateCallContext(7772);
            const ui64 start = GetCycleCount();
            root->SetRequestStartedCycles(start);
            root->EnableRequestTiming();
            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            env.SetupRequest(request, TBlockRange64::WithLength(1, 10));
            auto future = env.SplitRequestService->ReadBlocks(root, request);
            auto& a = *env.Storage->ReadBlocksPromises.FindPtr(
                TBlockRange64::WithLength(1, 5));
            auto& b = *env.Storage->ReadBlocksPromises.FindPtr(
                TBlockRange64::WithLength(6, 5));
            UNIT_ASSERT(a.CallContext.Get() != b.CallContext.Get());
            UNIT_ASSERT_VALUES_EQUAL(&a.CallContext->LWOrbit, &root->LWOrbit);
            NProto::TReadBlocksResponse response;
            response.MutableBlocks()->AddBuffers("aabbccddee");
            if (fail) {
                *response.MutableError() = MakeError(E_REJECTED, "test error");
                // An unlocated wait in a noncausal sibling must not suppress
                // the complete diagnostic result for the early error.
                b.CallContext->AddTime(
                    EProcessingStage::Postponed, TDuration::MicroSeconds(5));
            }
            a.Promise.SetValue(std::move(response));
            if (!fail) {
                UNIT_ASSERT(!future.HasValue());
                NProto::TReadBlocksResponse other;
                other.MutableBlocks()->AddBuffers("ffgghhjjkk");
                b.Promise.SetValue(std::move(other));
            }
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                future.GetValue().GetError().GetCode(),
                fail ? E_REJECTED : S_OK);
            if (!fail) {
                UNIT_ASSERT_VALUES_EQUAL(
                    GetDataFromResponse(future.GetValue()),
                    "aabbccddeeffgghhjjkk");
            }
            const auto total = CyclesToDurationSafe(GetCycleCount() - start);
            const auto snapshot = root->CompleteRequestTiming(total);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(snapshot, &json, true));
            UNIT_ASSERT_C(
                json["complete"].GetBoolean(), json["reason"].GetString());
            UNIT_ASSERT_VALUES_EQUAL(
                json["without_waits_us"].GetUInteger(), total.MicroSeconds());
            if (fail) {
                // The response is already visible while the sibling is pending.
                UNIT_ASSERT(!b.Promise.GetFuture().HasValue());
                b.Promise.SetValue(NProto::TReadBlocksResponse());
                UNIT_ASSERT_VALUES_EQUAL(
                    root->CompleteRequestTiming(total), snapshot);
            }
        }
    }

    Y_UNIT_TEST(ShouldExposeMissingSplitWriteTimingWithoutChangingData)
    {
        TTestEnvironment env;
        env.MountVolume();
        auto root = CreateCallContext(7772);
        const ui64 start = GetCycleCount();
        root->SetRequestStartedCycles(start);
        root->EnableRequestTiming();
        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        env.SetupRequest(
            request, TBlockRange64::WithLength(1, 10), "aabbccddeeffgghhjjkk");
        auto future = env.SplitRequestService->WriteBlocks(root, request);
        auto& a = *env.Storage->WriteBlocksPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto& b = *env.Storage->WriteBlocksPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT_VALUES_EQUAL(GetDataFromRequest(a.Request), "aabbccddee");
        UNIT_ASSERT_VALUES_EQUAL(GetDataFromRequest(b.Request), "ffgghhjjkk");
        a.CallContext->AddTime(
            EProcessingStage::Shaping, TDuration::MicroSeconds(5));
        a.Promise.SetValue(NProto::TWriteBlocksResponse());
        UNIT_ASSERT(!future.HasValue());
        b.Promise.SetValue(NProto::TWriteBlocksResponse());
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(future.GetValue().GetError().GetCode(), S_OK);
        const auto total = CyclesToDurationSafe(GetCycleCount() - start);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(
            root->CompleteRequestTiming(total), &json, true));
        UNIT_ASSERT(!json["complete"].GetBoolean());
        UNIT_ASSERT(json["without_waits_us"].IsNull());
        UNIT_ASSERT(json["wait_impact_us"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(json["total_us"].GetUInteger(),
                                 total.MicroSeconds());
        UNIT_ASSERT_VALUES_EQUAL(root->Time(EProcessingStage::Shaping),
                                 TDuration::MicroSeconds(5));
    }


    Y_UNIT_TEST(ShouldMarkWaitInsidePendingDispatchIncomplete)
    {
        for (bool unlocated: {false, true}) {
            for (bool earlyError: {false, true}) {
                TTestEnvironment env;
                env.MountVolume();
                auto root = CreateCallContext(7772);
                const auto start = GetCycleCount();
                root->SetRequestStartedCycles(start);
                root->EnableRequestTiming();
                env.Storage->OnReadBlocks = [&](TCallContextPtr context) {
                    if (env.Storage->ReadBlocksPromises.empty()) {
                        if (unlocated) {
                            context->AddTime(
                                EProcessingStage::Shaping,
                                TDuration::MicroSeconds(5));
                        } else {
                            // Equal endpoints make this structural test
                            // independent of wall time and CPU calibration.
                            const auto now = GetCycleCount();
                            context->AddTimedWait(
                                EProcessingStage::Shaping, now, now);
                        }
                    }
                };
                auto request = std::make_shared<NProto::TReadBlocksRequest>();
                env.SetupRequest(request, TBlockRange64::WithLength(1, 10));
                auto future = env.SplitRequestService->ReadBlocks(root, request);
                UNIT_ASSERT(!future.HasValue());
                auto& a = *env.Storage->ReadBlocksPromises.FindPtr(
                    TBlockRange64::WithLength(1, 5));
                auto& b = *env.Storage->ReadBlocksPromises.FindPtr(
                    TBlockRange64::WithLength(6, 5));
                NProto::TReadBlocksResponse second;
                second.MutableBlocks()->AddBuffers("ffgghhjjkk");
                if (earlyError) {
                    *second.MutableError() = MakeError(E_REJECTED, "later part");
                }
                b.Promise.SetValue(std::move(second));
                UNIT_ASSERT_VALUES_EQUAL(future.HasValue(), earlyError);
                if (!earlyError) {
                    NProto::TReadBlocksResponse first;
                    first.MutableBlocks()->AddBuffers("aabbccddee");
                    a.Promise.SetValue(std::move(first));
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetDataFromResponse(future.GetValue()),
                        "aabbccddeeffgghhjjkk");
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    future.GetValue().GetError().GetCode(),
                    earlyError ? E_REJECTED : S_OK);
                const auto total = CyclesToDurationSafe(GetCycleCount() - start);
                const auto snapshot = root->CompleteRequestTiming(total);
                NJson::TJsonValue json;
                UNIT_ASSERT(NJson::ReadJsonTree(snapshot, &json, true));
                UNIT_ASSERT(!json["complete"].GetBoolean());
                UNIT_ASSERT(json["without_waits_us"].IsNull());
                UNIT_ASSERT(json["wait_impact_us"].IsNull());
                UNIT_ASSERT_VALUES_EQUAL(
                    json["reason"].GetString(),
                    "synchronous_dispatch_dependency_not_recorded");
                UNIT_ASSERT_VALUES_EQUAL(
                    root->Time(EProcessingStage::Shaping),
                    TDuration::MicroSeconds(unlocated ? 5 : 0));
                if (earlyError) {
                    a.Promise.SetValue(NProto::TReadBlocksResponse());
                    UNIT_ASSERT_VALUES_EQUAL(
                        root->CompleteRequestTiming(total), snapshot);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepTimingEventSequenceLocalToDispatchThread)
    {
        const auto before = TCallContextBase::GetThreadTimingEventSequence();
        std::thread worker([] {
            auto context = CreateCallContext();
            const auto now = GetCycleCount();
            context->SetRequestStartedCycles(now);
            context->EnableRequestTiming();
            context->AddTimedWait(EProcessingStage::Backoff, now, now);
            context->AddTime(
                EProcessingStage::Shaping, TDuration::MicroSeconds(1));
        });
        worker.join();
        UNIT_ASSERT_VALUES_EQUAL(
            TCallContextBase::GetThreadTimingEventSequence(), before);
    }


    Y_UNIT_TEST(ShouldFreezeTimingForConcurrentReadResponses)
    {
        struct TCase
        {
            ui32 FirstError;
            ui32 SecondError;
        };
        const TCase cases[] = {
            {S_OK, S_OK},
            {E_REJECTED, S_OK},
            {E_REJECTED, E_CANCELLED},
        };
        for (const auto& test: cases) {
            TTestEnvironment env;
            env.MountVolume();
            auto root = CreateCallContext(7772);
            const auto start = GetCycleCount();
            root->SetRequestStartedCycles(start);
            root->EnableRequestTiming();
            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            env.SetupRequest(request, TBlockRange64::WithLength(1, 10));
            auto future = env.SplitRequestService->ReadBlocks(root, request);
            auto& a = *env.Storage->ReadBlocksPromises.FindPtr(
                TBlockRange64::WithLength(1, 5));
            auto& b = *env.Storage->ReadBlocksPromises.FindPtr(
                TBlockRange64::WithLength(6, 5));
            NProto::TReadBlocksResponse first;
            first.MutableBlocks()->AddBuffers("aabbccddee");
            *first.MutableError() = MakeError(test.FirstError, "first");
            NProto::TReadBlocksResponse second;
            second.MutableBlocks()->AddBuffers("ffgghhjjkk");
            *second.MutableError() = MakeError(test.SecondError, "second");

            std::atomic<ui32> completions{0};
            TDuration frozenTotal;
            ui32 frozenError = S_OK;
            TString snapshot;
            std::exception_ptr callbackError;
            future.Subscribe(
                [&](const TFuture<NProto::TReadBlocksResponse>& completed) {
                    completions.fetch_add(1, std::memory_order_relaxed);
                    try {
                        frozenTotal = CyclesToDurationSafe(GetCycleCount() - start);
                        frozenError = completed.GetValue().GetError().GetCode();
                        snapshot = root->CompleteRequestTiming(
                            frozenTotal, frozenError);
                    } catch (...) {
                        callbackError = std::current_exception();
                    }
                });

            std::promise<void> startPromise;
            auto startFuture = startPromise.get_future().share();
            std::promise<void> readyPromise;
            auto readyFuture = readyPromise.get_future();
            std::atomic<ui32> ready{0};
            std::exception_ptr workerErrors[2];
            auto release = [&](ui32 index, auto& promise, auto& response) {
                try {
                    if (ready.fetch_add(1, std::memory_order_acq_rel) == 1) {
                        readyPromise.set_value();
                    }
                    startFuture.get();
                    promise.SetValue(std::move(response));
                } catch (...) {
                    workerErrors[index] = std::current_exception();
                }
            };
            std::thread firstWorker([&] { release(0, a.Promise, first); });
            std::thread secondWorker([&] { release(1, b.Promise, second); });
            readyFuture.get();
            startPromise.set_value();
            firstWorker.join();
            secondWorker.join();
            for (const auto& error: workerErrors) {
                if (error) {
                    std::rethrow_exception(error);
                }
            }
            if (callbackError) {
                std::rethrow_exception(callbackError);
            }
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(completions.load(), 1);
            const auto& response = future.GetValue();
            if (!test.FirstError && !test.SecondError) {
                UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetCode(), S_OK);
                UNIT_ASSERT_VALUES_EQUAL(
                    GetDataFromResponse(response), "aabbccddeeffgghhjjkk");
            } else if (!test.SecondError) {
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetCode(), test.FirstError);
            } else {
                UNIT_ASSERT(
                    response.GetError().GetCode() == test.FirstError ||
                    response.GetError().GetCode() == test.SecondError);
            }
            UNIT_ASSERT_VALUES_EQUAL(frozenError, response.GetError().GetCode());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(snapshot, &json, true));
            UNIT_ASSERT_C(json["complete"].GetBoolean(), json["reason"].GetString());
            UNIT_ASSERT_VALUES_EQUAL(
                json["without_waits_us"].GetUInteger(),
                frozenTotal.MicroSeconds());

            TString lateSnapshot;
            std::exception_ptr lateError;
            std::thread lateWorker([&] {
                try {
                    const auto now = GetCycleCount();
                    a.CallContext->AddTimedWait(
                        EProcessingStage::Postponed, now, now);
                    a.CallContext->AddTime(
                        EProcessingStage::Postponed,
                        TDuration::MicroSeconds(5));
                    a.CallContext->FinishRequestTiming(now);
                    root->MarkRequestTimingIncomplete("late_after_freeze");
                    lateSnapshot = root->CompleteRequestTiming(
                        frozenTotal + TDuration::MilliSeconds(1),
                        frozenError ? S_OK : E_REJECTED);
                } catch (...) {
                    lateError = std::current_exception();
                }
            });
            lateWorker.join();
            if (lateError) {
                std::rethrow_exception(lateError);
            }
            UNIT_ASSERT_VALUES_EQUAL(lateSnapshot, snapshot);
            UNIT_ASSERT_VALUES_EQUAL(
                root->CompleteRequestTiming(frozenTotal, frozenError), snapshot);
        }
    }


    Y_UNIT_TEST(ShouldMarkFinalDispatchDelayingEarlyErrorIncomplete)
    {
        for (bool unlocated: {false, true}) {
            TTestEnvironment env;
            env.MountVolume();
            auto root = CreateCallContext(7772);
            const auto start = GetCycleCount();
            root->SetRequestStartedCycles(start);
            root->EnableRequestTiming();
            env.Storage->OnReadBlocks = [&](TCallContextPtr context) {
                if (!env.Storage->ReadBlocksPromises.empty()) {
                    auto& first = *env.Storage->ReadBlocksPromises.FindPtr(
                        TBlockRange64::WithLength(1, 5));
                    NProto::TReadBlocksResponse response;
                    *response.MutableError() = MakeError(E_REJECTED, "early");
                    // The first callback cancels observation of this part
                    // while the final Execute has not returned to its caller.
                    first.Promise.SetValue(std::move(response));
                    if (unlocated) {
                        context->AddTime(
                            EProcessingStage::Shaping,
                            TDuration::MicroSeconds(5));
                    } else {
                        const auto now = GetCycleCount();
                        context->AddTimedWait(EProcessingStage::Shaping, now, now);
                    }
                }
            };
            auto request = std::make_shared<NProto::TReadBlocksRequest>();
            env.SetupRequest(request, TBlockRange64::WithLength(1, 10));
            auto future = env.SplitRequestService->ReadBlocks(root, request);
            UNIT_ASSERT(future.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                future.GetValue().GetError().GetCode(), E_REJECTED);
            const auto total = CyclesToDurationSafe(GetCycleCount() - start);
            const auto snapshot = root->CompleteRequestTiming(total, E_REJECTED);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(snapshot, &json, true));
            UNIT_ASSERT(!json["complete"].GetBoolean());
            UNIT_ASSERT(json["without_waits_us"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(
                json["reason"].GetString(),
                "synchronous_dispatch_dependency_not_recorded");
            auto& last = *env.Storage->ReadBlocksPromises.FindPtr(
                TBlockRange64::WithLength(6, 5));
            UNIT_ASSERT(!last.Promise.GetFuture().HasValue());
            last.Promise.SetValue(NProto::TReadBlocksResponse());
            UNIT_ASSERT_VALUES_EQUAL(
                root->CompleteRequestTiming(total, E_REJECTED), snapshot);
        }
    }

    Y_UNIT_TEST(ShouldMarkUnknownSynchronousDispatchDependency)
    {
        TTestEnvironment env;
        env.MountVolume();
        env.Storage->SyncZeroBlocksError = MakeError(E_REJECTED, "sync fail");
        auto root = CreateCallContext();
        const auto start = GetCycleCount();
        root->SetRequestStartedCycles(start);
        root->EnableRequestTiming();
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        env.SetupRequest(request, TBlockRange64::WithLength(1, 10));
        auto future = env.SplitRequestService->ZeroBlocks(root, request);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            future.GetValue().GetError().GetCode(), E_REJECTED);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(
            root->CompleteRequestTiming(
                CyclesToDurationSafe(GetCycleCount() - start)),
            &json, true));
        UNIT_ASSERT(!json["complete"].GetBoolean());
        UNIT_ASSERT(json["without_waits_us"].IsNull());
    }

}

}   // namespace NCloud::NBlockStore
