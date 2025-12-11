#include "validation.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/function.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool HasError(const TFuture<T>& future)
{
    if (!future.HasException()) {
        const auto& response = future.GetValue();
        return response.HasError() && FAILED(response.GetError().GetCode());
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

struct TValidationCallback final: public IValidationCallback
{
    size_t ErrorsCount = 0;

    void ReportError(const TString& message) override
    {
        Y_UNUSED(message);
        ++ErrorsCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestService final: public IBlockStore
{
    void Start() override
    {}
    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    using T##name##Handler = std::function<TFuture<NProto::T##name##Response>( \
        TCallContextPtr ctx,                                                   \
        const NProto::T##name##Request&)>;                                     \
    T##name##Handler name##Handler;                                            \
                                                                               \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return name##Handler(ctx, *request);                                   \
    }                                                                          \
    // BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NProto::TMountVolumeRequest> CreateMountVolumeRequest(
    const TString& diskId)
{
    auto request = std::make_shared<NProto::TMountVolumeRequest>();
    request->SetDiskId(diskId);
    return request;
}

std::shared_ptr<NProto::TMountVolumeResponse> CreateMountVolumeResponse(
    ui32 blockSize = 1)
{
    auto response = std::make_shared<NProto::TMountVolumeResponse>();
    NProto::TVolume volumeInfo;
    volumeInfo.SetBlockSize(blockSize);
    auto& volume = *response->MutableVolume();
    volume = volumeInfo;
    return response;
}

std::shared_ptr<NProto::TWriteBlocksRequest> CreateWriteBlocksRequest(
    const TString& diskId,
    ui64 startIndex,
    ui32 blocksCount,
    const TString& data = "1")
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    for (ui32 i = 0; i < blocksCount; ++i) {
        request->MutableBlocks()->AddBuffers(data);
    }
    return request;
}

std::shared_ptr<NProto::TReadBlocksRequest> CreateReadBlocksRequest(
    const TString& diskId,
    ui64 startIndex,
    ui32 blocksCount)
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blocksCount);
    return request;
}

NProto::TReadBlocksResponse CreateReadBlocksResponse(
    ui32 blocksCount,
    const TString& data = "1")
{
    NProto::TReadBlocksResponse response;
    for (ui32 i = 0; i < blocksCount; ++i) {
        response.MutableBlocks()->AddBuffers(data);
    }
    return response;
}

////////////////////////////////////////////////////////////////////////////////

using BlocksHolder = std::shared_ptr<TVector<TString>>;

std::shared_ptr<NProto::TWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
    TVector<BlocksHolder>& blocksHolderList,
    const TString& diskId,
    ui64 startIndex,
    ui32 blocksCount,
    const TString& data = "1")
{
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    request->BlocksCount = blocksCount;
    request->BlockSize = data.size();

    auto blocksHolder = std::make_shared<TVector<TString>>();
    blocksHolderList.push_back(blocksHolder);

    auto sglist = ResizeBlocks(*blocksHolder, blocksCount, data);
    request->Sglist = TGuardedSgList(std::move(sglist));
    return request;
}

std::shared_ptr<NProto::TReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
    TVector<BlocksHolder>& blocksHolderList,
    const TString& diskId,
    ui64 startIndex,
    ui32 blocksCount,
    const TString& data = "1")
{
    auto blocksHolder = std::make_shared<TVector<TString>>();
    blocksHolderList.push_back(blocksHolder);

    auto sglist = ResizeBlocks(*blocksHolder, blocksCount, data);

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blocksCount);
    request->BlockSize = data.size();
    request->Sglist = TGuardedSgList(std::move(sglist));
    return request;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TValidationServiceTest)
{
    Y_UNIT_TEST(ShouldNotWarnOnNonOverlappingRequests)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest1 = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse1;
        };

        auto writeResult1 =
            validator->WriteBlocks(ctx, std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 = CreateWriteBlocksRequest("test", 1, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse2;
        };

        auto writeResult2 =
            validator->WriteBlocks(ctx, std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldDetectWriteWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest1 = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse1;
        };

        auto writeResult1 =
            validator->WriteBlocks(ctx, std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse2;
        };

        auto writeResult2 =
            validator->WriteBlocks(ctx, std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldDetectReadWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse;
        };

        auto writeResult = validator->WriteBlocks(ctx, std::move(writeRequest));

        // ReadBlocks
        auto readRequest = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksResponse>();

        service->ReadBlocksHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksRequest&)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocks(ctx, std::move(readRequest));

        writeResponse.SetValue({});
        readResponse.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);

        // ReadBlocks
        auto readRequest2 = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksResponse>();

        service->ReadBlocksHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksRequest&)
        {
            return readResponse2;
        };

        auto readResult2 = validator->ReadBlocks(ctx, std::move(readRequest2));
        readResponse2.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldDetectInconsistentRead)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest = CreateWriteBlocksRequest("test", 0, 1, "t");
        auto writeResponse = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse;
        };

        auto writeResult = validator->WriteBlocks(ctx, std::move(writeRequest));
        writeResponse.SetValue({});

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // ReadBlocks
        auto readRequest = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksResponse>();

        service->ReadBlocksHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksRequest&)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocks(ctx, std::move(readRequest));
        readResponse.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldNotDetectReadReadRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // ReadBlocks
        auto readRequest1 = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse1 = NewPromise<NProto::TReadBlocksResponse>();

        service->ReadBlocksHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksRequest&)
        {
            return readResponse1;
        };

        auto readResult1 = validator->ReadBlocks(ctx, std::move(readRequest1));

        // ReadBlocks
        auto readRequest2 = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksResponse>();

        service->ReadBlocksHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksRequest&)
        {
            return readResponse2;
        };

        auto readResult2 = validator->ReadBlocks(ctx, std::move(readRequest2));

        readResponse1.SetValue(CreateReadBlocksResponse(1));
        readResponse2.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(readResult1));
        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);
    }

    // Local requests

    Y_UNIT_TEST(ShouldNotWarnOnNonOverlappingLocalRequests)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest1 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        service->WriteBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksLocalRequest&)
        {
            return writeResponse1;
        };

        auto writeResult1 =
            validator->WriteBlocksLocal(ctx, std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 1, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        service->WriteBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksLocalRequest&)
        {
            return writeResponse2;
        };

        auto writeResult2 =
            validator->WriteBlocksLocal(ctx, std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldDetectLocalWriteWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest1 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        service->WriteBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksLocalRequest&)
        {
            return writeResponse1;
        };

        auto writeResult1 =
            validator->WriteBlocksLocal(ctx, std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        service->WriteBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksLocalRequest&)
        {
            return writeResponse2;
        };

        auto writeResult2 =
            validator->WriteBlocksLocal(ctx, std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldDetectLocalReadWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse = NewPromise<NProto::TWriteBlocksLocalResponse>();

        service->WriteBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksLocalRequest&)
        {
            return writeResponse;
        };

        auto writeResult =
            validator->WriteBlocksLocal(ctx, std::move(writeRequest));

        // ReadBlocks
        auto readRequest =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksLocalResponse>();

        service->ReadBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksLocalRequest&)
        {
            return readResponse;
        };

        auto readResult =
            validator->ReadBlocksLocal(ctx, std::move(readRequest));

        writeResponse.SetValue({});
        readResponse.SetValue({});

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);

        // ReadBlocks
        auto readRequest2 =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksLocalResponse>();

        service->ReadBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksLocalRequest&)
        {
            return readResponse2;
        };

        auto readResult2 =
            validator->ReadBlocksLocal(ctx, std::move(readRequest2));
        readResponse2.SetValue({});

        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldDetectLocalInconsistentRead)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1, "t");
        auto writeResponse = NewPromise<NProto::TWriteBlocksLocalResponse>();

        service->WriteBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksLocalRequest&)
        {
            return writeResponse;
        };

        auto writeResult =
            validator->WriteBlocksLocal(ctx, std::move(writeRequest));
        writeResponse.SetValue({});

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // ReadBlocks
        auto readRequest =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksLocalResponse>();

        service->ReadBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksLocalRequest&)
        {
            return readResponse;
        };

        auto readResult =
            validator->ReadBlocksLocal(ctx, std::move(readRequest));
        readResponse.SetValue({});

        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldNotDetectLocalReadReadRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // ReadBlocks
        auto readRequest1 =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse1 = NewPromise<NProto::TReadBlocksLocalResponse>();

        service->ReadBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksLocalRequest&)
        {
            return readResponse1;
        };

        auto readResult1 =
            validator->ReadBlocksLocal(ctx, std::move(readRequest1));

        // ReadBlocks
        auto readRequest2 =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksLocalResponse>();

        service->ReadBlocksLocalHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksLocalRequest&)
        {
            return readResponse2;
        };

        auto readResult2 =
            validator->ReadBlocksLocal(ctx, std::move(readRequest2));

        readResponse1.SetValue({});
        readResponse2.SetValue({});

        UNIT_ASSERT(!HasError(readResult1));
        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldNotDetectInconsistencyAfterAWriteWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest1 = CreateWriteBlocksRequest("test", 0, 1, "t");
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse1;
        };

        auto writeResult1 =
            validator->WriteBlocks(ctx, std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 = CreateWriteBlocksRequest("test", 0, 1, "t");
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse2;
        };

        auto writeResult2 =
            validator->WriteBlocks(ctx, std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);

        // ReadBlocks
        auto readRequest = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksResponse>();

        service->ReadBlocksHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksRequest&)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocks(ctx, std::move(readRequest));
        readResponse.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(readResult));
        // error count should not increase
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }

    Y_UNIT_TEST(ShouldNotDetectInconsistencyUponAReadWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto service = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationService(
            logging,
            monitoring,
            service,
            CreateCrcDigestCalculator(),
            TDuration::Days(1),
            callback);

        auto ctx = MakeIntrusive<TCallContext>();

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        service->MountVolumeHandler =
            [&](TCallContextPtr, const NProto::TMountVolumeRequest&)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(ctx, std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse());

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT_VALUES_EQUAL(0, callback->ErrorsCount);

        // WriteBlocks
        auto writeRequest = CreateWriteBlocksRequest("test", 0, 1, "t");
        auto writeResponse = NewPromise<NProto::TWriteBlocksResponse>();

        service->WriteBlocksHandler =
            [&](TCallContextPtr, const NProto::TWriteBlocksRequest&)
        {
            return writeResponse;
        };

        auto writeResult = validator->WriteBlocks(ctx, std::move(writeRequest));

        // ReadBlocks
        auto readRequest = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksResponse>();

        service->ReadBlocksHandler =
            [&](TCallContextPtr, const NProto::TReadBlocksRequest&)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocks(ctx, std::move(readRequest));

        // race detected
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);

        writeResponse.SetValue({});
        readResponse.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT(!HasError(readResult));

        // error count should not increase
        UNIT_ASSERT_VALUES_EQUAL(1, callback->ErrorsCount);
    }
}

}   // namespace NCloud::NBlockStore::NServer
