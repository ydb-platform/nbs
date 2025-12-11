#include "validation.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/function.h>

namespace NCloud::NBlockStore::NClient {

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

std::shared_ptr<NProto::TMountVolumeRequest> CreateMountVolumeRequest(
    const TString& diskId)
{
    auto request = std::make_shared<NProto::TMountVolumeRequest>();
    request->SetDiskId(diskId);
    return request;
}

std::shared_ptr<NProto::TMountVolumeResponse> CreateMountVolumeResponse(
    const TString& diskId,
    ui32 blockSize = 1)
{
    auto response = std::make_shared<NProto::TMountVolumeResponse>();
    NProto::TVolume volumeInfo;
    volumeInfo.SetDiskId(diskId);
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

Y_UNIT_TEST_SUITE(TValidationClientTest)
{
    Y_UNIT_TEST(ShouldNotWarnOnNonOverlappingRequests)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest1 = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksResponse>();

        client->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest>)
        {
            return writeResponse1;
        };

        auto writeResult1 = validator->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 = CreateWriteBlocksRequest("test", 1, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksResponse>();

        client->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest>)
        {
            return writeResponse2;
        };

        auto writeResult2 = validator->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT(callback->ErrorsCount == 0);
    }

    Y_UNIT_TEST(ShouldDetectWriteWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest1 = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksResponse>();

        client->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest>)
        {
            return writeResponse1;
        };

        auto writeResult1 = validator->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksResponse>();

        client->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest>)
        {
            return writeResponse2;
        };

        auto writeResult2 = validator->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT(callback->ErrorsCount == 1);
    }

    Y_UNIT_TEST(ShouldDetectReadWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest = CreateWriteBlocksRequest("test", 0, 1);
        auto writeResponse = NewPromise<NProto::TWriteBlocksResponse>();

        client->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest>)
        {
            return writeResponse;
        };

        auto writeResult = validator->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest));

        // ReadBlocks
        auto readRequest = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksResponse>();

        client->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest>)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest));

        writeResponse.SetValue({});
        readResponse.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT(callback->ErrorsCount == 1);

        // ReadBlocks
        auto readRequest2 = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksResponse>();

        client->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest>)
        {
            return readResponse2;
        };

        auto readResult2 = validator->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest2));
        readResponse2.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT(callback->ErrorsCount == 1);
    }

    Y_UNIT_TEST(ShouldDetectInconsistentRead)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest = CreateWriteBlocksRequest("test", 0, 1, "t");
        auto writeResponse = NewPromise<NProto::TWriteBlocksResponse>();

        client->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest>)
        {
            return writeResponse;
        };

        auto writeResult = validator->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest));
        writeResponse.SetValue({});

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // ReadBlocks
        auto readRequest = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksResponse>();

        client->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest>)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest));
        readResponse.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT(callback->ErrorsCount == 1);
    }

    Y_UNIT_TEST(ShouldNotDetectReadReadRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // ReadBlocks
        auto readRequest1 = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse1 = NewPromise<NProto::TReadBlocksResponse>();

        client->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest>)
        {
            return readResponse1;
        };

        auto readResult1 = validator->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest1));

        // ReadBlocks
        auto readRequest2 = CreateReadBlocksRequest("test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksResponse>();

        client->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest>)
        {
            return readResponse2;
        };

        auto readResult2 = validator->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest2));

        readResponse1.SetValue(CreateReadBlocksResponse(1));
        readResponse2.SetValue(CreateReadBlocksResponse(1));

        UNIT_ASSERT(!HasError(readResult1));
        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT(callback->ErrorsCount == 0);
    }

    // Local requests

    Y_UNIT_TEST(ShouldNotWarnOnNonOverlappingLocalRequests)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest1 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
        {
            return writeResponse1;
        };

        auto writeResult1 = validator->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 1, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
        {
            return writeResponse2;
        };

        auto writeResult2 = validator->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT(callback->ErrorsCount == 0);
    }

    Y_UNIT_TEST(ShouldDetectLocalWriteWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest1 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse1 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
        {
            return writeResponse1;
        };

        auto writeResult1 = validator->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest1));

        // WriteBlocks
        auto writeRequest2 =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse2 = NewPromise<NProto::TWriteBlocksLocalResponse>();

        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
        {
            return writeResponse2;
        };

        auto writeResult2 = validator->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest2));

        writeResponse1.SetValue({});
        writeResponse2.SetValue({});

        UNIT_ASSERT(!HasError(writeResult1));
        UNIT_ASSERT(!HasError(writeResult2));
        UNIT_ASSERT(callback->ErrorsCount == 1);
    }

    Y_UNIT_TEST(ShouldDetectLocalReadWriteRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto writeResponse = NewPromise<NProto::TWriteBlocksLocalResponse>();

        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
        {
            return writeResponse;
        };

        auto writeResult = validator->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest));

        // ReadBlocks
        auto readRequest =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksLocalResponse>();

        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest>)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest));

        writeResponse.SetValue({});
        readResponse.SetValue({});

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT(callback->ErrorsCount == 1);

        // ReadBlocks
        auto readRequest2 =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksLocalResponse>();

        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest>)
        {
            return readResponse2;
        };

        auto readResult2 = validator->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest2));
        readResponse2.SetValue({});

        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT(callback->ErrorsCount == 1);
    }

    Y_UNIT_TEST(ShouldDetectLocalInconsistentRead)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // WriteBlocks
        auto writeRequest =
            CreateWriteBlocksLocalRequest(blocksHolderList, "test", 0, 1, "t");
        auto writeResponse = NewPromise<NProto::TWriteBlocksLocalResponse>();

        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
        {
            return writeResponse;
        };

        auto writeResult = validator->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(writeRequest));
        writeResponse.SetValue({});

        UNIT_ASSERT(!HasError(writeResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // ReadBlocks
        auto readRequest =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse = NewPromise<NProto::TReadBlocksLocalResponse>();

        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest>)
        {
            return readResponse;
        };

        auto readResult = validator->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest));
        readResponse.SetValue({});

        UNIT_ASSERT(!HasError(readResult));
        UNIT_ASSERT(callback->ErrorsCount == 1);
    }

    Y_UNIT_TEST(ShouldNotDetectLocalReadReadRace)
    {
        auto logging = CreateLoggingService("console");
        auto monitoring = CreateMonitoringServiceStub();
        TVector<BlocksHolder> blocksHolderList;

        auto client = std::make_shared<TTestService>();

        auto callback = std::make_shared<TValidationCallback>();
        auto validator = CreateValidationClient(
            logging,
            monitoring,
            client,
            CreateCrcDigestCalculator(),
            callback);

        // MountVolume
        auto mountRequest = CreateMountVolumeRequest("test");
        auto mountResponse = NewPromise<NProto::TMountVolumeResponse>();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest>)
        {
            return mountResponse;
        };

        auto mountResult = validator->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::move(mountRequest));
        mountResponse.SetValue(*CreateMountVolumeResponse("test"));

        UNIT_ASSERT(!HasError(mountResult));
        UNIT_ASSERT(callback->ErrorsCount == 0);

        // ReadBlocks
        auto readRequest1 =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse1 = NewPromise<NProto::TReadBlocksLocalResponse>();

        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest>)
        {
            return readResponse1;
        };

        auto readResult1 = validator->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest1));

        // ReadBlocks
        auto readRequest2 =
            CreateReadBlocksLocalRequest(blocksHolderList, "test", 0, 1);
        auto readResponse2 = NewPromise<NProto::TReadBlocksLocalResponse>();

        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest>)
        {
            return readResponse2;
        };

        auto readResult2 = validator->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(readRequest2));

        readResponse1.SetValue({});
        readResponse2.SetValue({});

        UNIT_ASSERT(!HasError(readResult1));
        UNIT_ASSERT(!HasError(readResult2));
        UNIT_ASSERT(callback->ErrorsCount == 0);
    }
}

}   // namespace NCloud::NBlockStore::NClient
