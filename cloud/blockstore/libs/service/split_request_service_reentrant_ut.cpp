#include "split_request_service.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlockSize = 2;
constexpr ui64 StripeSize = 6;
const char* const DefaultDiskId = "vol1";

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore: public TBlockStoreImpl<TTestBlockStore, IBlockStore>
{
    struct TRequestInfo
    {
        TPromise<NProto::TWriteBlocksLocalResponse> Promise =
            NewPromise<NProto::TWriteBlocksLocalResponse>();
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> Request;
    };

    TMap<TBlockRange64, TRequestInfo, TBlockRangeComparator>
        WriteBlocksLocalPromises;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void Start() override
    {}

    void Stop() override
    {}

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

        return MakeFuture(std::move(response));
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSplitRequestServiceReentrantTest)
{
    Y_UNIT_TEST(ShouldCloseLocalSglistBeforeCompletingOnSubRequestError)
    {
        auto storage = std::make_shared<TTestBlockStore>();
        auto service = CreateSplitRequestService(storage);

        {
            auto request = std::make_shared<NProto::TMountVolumeRequest>();
            request->SetDiskId(DefaultDiskId);

            auto future = service->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));
            const auto& response = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }

        const TString data = "aabbccddeeffgghhjjkk";
        const auto range =
            TBlockRange64::WithLength(1, data.size() / DefaultBlockSize);

        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->SetDiskId(DefaultDiskId);
        request->SetStartIndex(range.Start);
        request->SetBlockSize(DefaultBlockSize);
        request->BlocksCount = range.Size();
        request->Sglist = TGuardedSgList({
            TBlockDataRef(data.data(), data.size())});

        auto requestRef = request;
        auto future = service->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(2, storage->WriteBlocksLocalPromises.size());
        auto* firstWrite = storage->WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(1, 5));
        auto* secondWrite = storage->WriteBlocksLocalPromises.FindPtr(
            TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstWrite != nullptr);
        UNIT_ASSERT(secondWrite != nullptr);

        bool callbackCalled = false;
        bool secondSglistClosedBeforeCallback = false;
        future.Subscribe(
            [requestRef,
             secondWrite,
             &callbackCalled,
             &secondSglistClosedBeforeCallback](
                const TFuture<NProto::TWriteBlocksLocalResponse>&) mutable
            {
                callbackCalled = true;
                secondSglistClosedBeforeCallback =
                    !secondWrite->Request->Sglist.Acquire();

                // Reproduce a continuation that moves the original request
                // while OnSubResponse() is still on the stack.
                auto movedRequest = std::move(*requestRef);
                Y_UNUSED(movedRequest);
            });

        {
            auto guard = secondWrite->Request->Sglist.Acquire();
            UNIT_ASSERT(guard);
        }

        NProto::TWriteBlocksLocalResponse response;
        *response.MutableError() = MakeError(E_REJECTED);
        firstWrite->Promise.SetValue(std::move(response));

        UNIT_ASSERT(callbackCalled);
        UNIT_ASSERT(secondSglistClosedBeforeCallback);

        const auto& result = future.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result.GetError().GetCode(),
            FormatError(result.GetError()));
    }
}

}   // namespace NCloud::NBlockStore
