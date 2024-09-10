#include "encryption_service.h"

#include "encryption_client.h"
#include "encryption_key.h"

#include <cloud/blockstore/public/api/protos/io.pb.h>
#include <cloud/blockstore/public/api/protos/encryption.pb.h>

#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestCountService
    : public TTestService
{
    ui32 IOCounter = 0;

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture(NProto::TMountVolumeResponse());
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture(NProto::TUnmountVolumeResponse());
    }

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        ++IOCounter;
        return MakeFuture(NProto::TReadBlocksResponse());
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        ++IOCounter;
        return MakeFuture(NProto::TWriteBlocksResponse());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEncryptionClientFactory
    : public IEncryptionClientFactory
{
    const IBlockStorePtr Service;

    NProto::TEncryptionSpec ExpectedSpec;
    TString ExpectedDiskId;

    TVector<std::shared_ptr<TTestCountService>> Sessions;

    explicit TEncryptionClientFactory(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    TFuture<TResultOrError<IBlockStorePtr>> CreateEncryptionClient(
        IBlockStorePtr client,
        const NProto::TEncryptionSpec& encryptionSpec,
        const TString& diskId) override
    {
        UNIT_ASSERT_VALUES_EQUAL(Service.get(), client.get());

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(comparator.Equals(ExpectedSpec, encryptionSpec));
        UNIT_ASSERT_VALUES_EQUAL(ExpectedDiskId, diskId);

        auto session = std::make_shared<TTestCountService>();
        Sessions.push_back(session);
        return MakeFuture(TResultOrError<IBlockStorePtr>(session));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeEncryptionClientFactory
    : public IVolumeEncryptionClientFactory
{
    TFuture<TResultOrError<IBlockStorePtr>> CreateEncryptionClient(
        IBlockStorePtr client,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(client);
        Y_UNUSED(volume);

        return MakeFuture(TResultOrError<IBlockStorePtr>(
            std::make_shared<TTestCountService>()));
    }
};

////////////////////////////////////////////////////////////////////////////////

void MountVolume(
    std::shared_ptr<TEncryptionClientFactory> clientFactory,
    IBlockStorePtr service,
    const TString& clientId,
    bool encrypted)
{
    auto request = std::make_shared<NProto::TMountVolumeRequest>();
    request->MutableHeaders()->SetClientId(clientId);
    request->SetDiskId(clientId + "_disk");

    if (encrypted) {
        auto& encryptionSpec = *request->MutableEncryptionSpec();
        encryptionSpec.SetMode(NProto::ENCRYPTION_AES_XTS);
    }

    clientFactory->ExpectedSpec = request->GetEncryptionSpec();
    clientFactory->ExpectedDiskId = request->GetDiskId();;

    auto future = service->MountVolume(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_C(!HasError(response), response.GetError());
}

void UnmountVolume(IBlockStorePtr service, const TString& clientId)
{
    auto request = std::make_shared<NProto::TUnmountVolumeRequest>();
    request->MutableHeaders()->SetClientId(clientId);
    request->SetDiskId(clientId + "_disk");

    auto future = service->UnmountVolume(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_C(!HasError(response), response.GetError());
}

void ReadBlocks(IBlockStorePtr service, const TString& clientId)
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();
    request->MutableHeaders()->SetClientId(clientId);
    request->SetDiskId(clientId + "_disk");

    auto future = service->ReadBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_C(!HasError(response), response.GetError());
}

void WriteBlocks(IBlockStorePtr service, const TString& clientId)
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();
    request->MutableHeaders()->SetClientId(clientId);
    request->SetDiskId(clientId + "_disk");

    auto future = service->WriteBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    const auto& response = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_C(!HasError(response), response.GetError());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMultipleEncryptionServiceTest)
{
    Y_UNIT_TEST(ShouldSupportMultipleSessions)
    {
        auto service = std::make_shared<TTestCountService>();
        auto clientFactory = std::make_shared<TEncryptionClientFactory>(
            service);

        auto clientId1 = "testClientId1";
        auto clientId2 = "testClientId2";
        auto clientId3 = "testClientId3";

        auto multipleService = CreateMultipleEncryptionService(
            service,
            CreateLoggingService("console"),
            clientFactory);

        MountVolume(clientFactory, multipleService, clientId1, false);
        UNIT_ASSERT_VALUES_EQUAL(1, clientFactory->Sessions.size());

        MountVolume(clientFactory, multipleService, clientId2, true);
        UNIT_ASSERT_VALUES_EQUAL(2, clientFactory->Sessions.size());

        MountVolume(clientFactory, multipleService, clientId3, true);
        UNIT_ASSERT_VALUES_EQUAL(3, clientFactory->Sessions.size());

        auto& client1 = clientFactory->Sessions[0];
        ReadBlocks(multipleService, clientId1);
        UNIT_ASSERT_VALUES_EQUAL(1, client1->IOCounter);

        WriteBlocks(multipleService, clientId1);
        UNIT_ASSERT_VALUES_EQUAL(2, client1->IOCounter);

        auto& client2 = clientFactory->Sessions[1];
        ReadBlocks(multipleService, clientId2);
        UNIT_ASSERT_VALUES_EQUAL(1, client2->IOCounter);
        WriteBlocks(multipleService, clientId2);
        UNIT_ASSERT_VALUES_EQUAL(2, client2->IOCounter);

        auto& client3 = clientFactory->Sessions[2];
        ReadBlocks(multipleService, clientId3);
        UNIT_ASSERT_VALUES_EQUAL(1, client3->IOCounter);
        WriteBlocks(multipleService, clientId3);
        UNIT_ASSERT_VALUES_EQUAL(2, client3->IOCounter);

        UNIT_ASSERT_VALUES_EQUAL(0, service->IOCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, client1->IOCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, client2->IOCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, client3->IOCounter);

        {
            std::weak_ptr<TTestCountService> weak = client3;
            client3.reset();
            UNIT_ASSERT(nullptr != weak.lock());
            UnmountVolume(multipleService, clientId3);
            UNIT_ASSERT(nullptr == weak.lock());
        }

        {
            std::weak_ptr<TTestCountService> weak = client2;
            client2.reset();
            UNIT_ASSERT(nullptr != weak.lock());
            UnmountVolume(multipleService, clientId2);
            UNIT_ASSERT(nullptr == weak.lock());
        }

        UnmountVolume(multipleService, clientId1);
    }

    Y_UNIT_TEST(ShouldCreateEncryptedDisk)
    {
        auto logging = CreateLoggingService("console");

        auto clientFactory = CreateEncryptionClientFactory(
            logging,
            CreateEncryptionKeyProvider(CreateKmsKeyProviderStub()),
            std::make_shared<TVolumeEncryptionClientFactory>());

        auto service = std::make_shared<TTestService>();
        service->CreateVolumeHandler =
            [&] (std::shared_ptr<NProto::TCreateVolumeRequest> request) {
                auto encryptionSpec = request->GetEncryptionSpec();
                UNIT_ASSERT(NProto::ENCRYPTION_AES_XTS == encryptionSpec.GetMode());
                UNIT_ASSERT_VALUES_EQUAL("", encryptionSpec.GetKeyHash());
                UNIT_ASSERT_C(!encryptionSpec.HasKeyPath(), encryptionSpec.GetKeyPath());

                return MakeFuture(NProto::TCreateVolumeResponse());
            };

        auto multipleService = CreateMultipleEncryptionService(
            service,
            logging,
            clientFactory);

        auto request = std::make_shared<NProto::TCreateVolumeRequest>();
        auto& encryptionSpec = *request->MutableEncryptionSpec();
        encryptionSpec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *encryptionSpec.MutableKeyPath();
        auto& kmsKey = *keyPath.MutableKmsKey();
        kmsKey.SetKekId("kek-id");
        kmsKey.SetEncryptedDEK("encrypted-dek");
        kmsKey.SetTaskId("task-id");

        auto future = multipleService->CreateVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }
}

}   // namespace NCloud::NBlockStore
