#include "split_request_service.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
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

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            ReadBlocksPromises[range] = {.Request = std::move(request)};
        return info.Promise;
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            ReadBlocksLocalPromises[range] = {.Request = std::move(request)};
        return info.Promise;
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        Y_UNUSED(callContext);

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            WriteBlocksPromises[range] = {.Request = std::move(request)};
        return info.Promise;
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            WriteBlocksLocalPromises[range] = {.Request = std::move(request)};
        return info.Promise;
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);

        auto range = TBlockRangeHelper::GetRange(*request, DefaultBlockSize);
        auto& info =
            ZeroBlocksPromises[range] = {.Request = std::move(request)};
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
        request->BlockSize = DefaultBlockSize;
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
        request->BlockSize = DefaultBlockSize;
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
}

}   // namespace NCloud::NBlockStore
