#include "client.h"

#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryption_service.h>
#include <cloud/blockstore/libs/root_kms/iface/client.h>
#include <cloud/blockstore/libs/root_kms/iface/key_provider.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/system/env.h>
#include <util/string/builder.h>

#include <chrono>

namespace NCloud::NBlockStore {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : NUnitTest::TBaseFixture
{
    const TString KekId = "nbs";
    const ILoggingServicePtr Logging =
        CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});

    IRootKmsClientPtr Client;

    void SetUp(NUnitTest::TTestContext&) override
    {
        Client = CreateClient();
        Client->Start();

        while (!IsRootKmsAlive()) {
            Sleep(1s);
        }
    }

    IRootKmsClientPtr CreateClient(
        const TString& address = {},
        TDuration requestTimeout = TDuration::Minutes(5),
        const TString& sslTargetNameOverride = {}) const
    {
        return CreateRootKmsClient(
            Logging,
            {.Address = address
                 ? address
                 : "localhost:" + GetEnv("FAKE_ROOT_KMS_PORT"),
             .RootCertsFile = GetEnv("FAKE_ROOT_KMS_CA"),
             .CertChainFile = GetEnv("FAKE_ROOT_KMS_CLIENT_CRT"),
             .PrivateKeyFile = GetEnv("FAKE_ROOT_KMS_CLIENT_KEY"),
             .RequestTimeout = requestTimeout,
             .SslTargetNameOverride = sslTargetNameOverride});
    }

    void TearDown(NUnitTest::TTestContext&) override
    {
        Client->Stop();
    }

    bool IsRootKmsAlive() const
    {
        const auto future = Client->Decrypt(TString(), TString());
        const auto& [_, error] = future.GetValueSync();

        return error.GetCode() == E_GRPC_NOT_FOUND;
    }
};

NProto::TMountVolumeResponse MakeRootKmsEncryptedMountResponse(
    const TString& diskId,
    const TString& kekId,
    const TString& encryptedDEK)
{
    NProto::TMountVolumeResponse response;

    auto& volume = *response.MutableVolume();
    volume.SetDiskId(diskId);
    volume.SetBlockSize(4096);
    volume.SetBlocksCount(1000);

    auto& encryptionDesc = *volume.MutableEncryptionDesc();
    encryptionDesc.SetMode(
        NProto::ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY);

    auto& key = *encryptionDesc.MutableEncryptionKey();
    key.SetKekId(kekId);
    key.SetEncryptedDEK(Base64Encode(encryptedDEK));

    return response;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////


Y_UNIT_TEST_SUITE(TRootKmsClientTest)
{
    Y_UNIT_TEST_F(ShouldGenerateAndDecryptDEK, TFixture)
    {
        {
            auto gen = Client->GenerateDataEncryptionKey(KekId);

            auto [key, genError] = gen.GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, genError.GetCode(), genError);
            UNIT_ASSERT_VALUES_EQUAL(KekId, key.GetKekId());
            UNIT_ASSERT_VALUES_UNEQUAL("", key.GetEncryptedDEK());

            auto decrypt = Client->Decrypt(KekId, key.GetEncryptedDEK());

            const auto& [decryptedDEK, decryptError] = decrypt.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                decryptError.GetCode(),
                decryptError);
            UNIT_ASSERT_VALUES_UNEQUAL("", decryptedDEK.GetKey());
        }

        {
            auto future = Client->GenerateDataEncryptionKey("unknown");

            auto [key, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_GRPC_NOT_FOUND,
                error.GetCode(),
                error);
        }

        {
            auto future = Client->Decrypt("unknown", "ciphertext");

            const auto& [key, error] = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_GRPC_NOT_FOUND,
                error.GetCode(),
                error);
        }
    }

    Y_UNIT_TEST_F(
        ShouldReproduceMountDelayWithNonResponsiveRootKmsBackend,
        TFixture)
    {
        auto generated = Client->GenerateDataEncryptionKey(KekId);
        const auto& [kmsKey, generateError] =
            generated.GetValue(TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            generateError.GetCode(),
            generateError);

        const auto hangingPort = GetEnv("FAKE_ROOT_KMS_HANGING_PORT");
        const auto healthyPort = GetEnv("FAKE_ROOT_KMS_PORT");
        const TString address = TStringBuilder()
            << "ipv4:///127.0.0.1:" << hangingPort
            << ",127.0.0.1:" << hangingPort
            << ",127.0.0.1:" << healthyPort;

        auto faultClient = CreateClient(
            address,
            TDuration::Seconds(1),
            "localhost");
        faultClient->Start();

        // The hanging endpoint deliberately lacks this probe key, while the
        // healthy endpoint has it. Once a NOT_FOUND is followed by success,
        // both subchannels are READY and the round-robin picker has just used
        // the healthy slot. The next two slots are the duplicated hanging
        // endpoint and the third is healthy again.
        bool sawHangingEndpoint = false;
        bool pickerAligned = false;
        for (ui32 i = 0; i < 30; ++i) {
            auto probe =
                faultClient->GenerateDataEncryptionKey("healthy-probe");
            const auto& [_, error] =
                probe.GetValue(TDuration::Seconds(5));

            if (error.GetCode() == S_OK) {
                if (sawHangingEndpoint) {
                    pickerAligned = true;
                    break;
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_GRPC_NOT_FOUND,
                    error.GetCode(),
                    error);
                sawHangingEndpoint = true;
            }
        }
        UNIT_ASSERT_C(
            pickerAligned,
            "Failed to observe both Root KMS backends");

        auto keyProvider = CreateEncryptionKeyProvider(
            CreateKmsKeyProviderStub(),
            CreateRootKmsKeyProvider(faultClient, KekId));
        auto clientFactory = CreateEncryptionClientFactory(
            Logging,
            std::move(keyProvider),
            NProto::EZP_WRITE_ENCRYPTED_ZEROS);

        auto service = std::make_shared<TTestService>();
        service->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                return MakeFuture(MakeRootKmsEncryptedMountResponse(
                    request->GetDiskId(),
                    kmsKey.GetKekId(),
                    kmsKey.GetEncryptedDEK()));
            };

        auto multipleService = CreateMultipleEncryptionService(
            service,
            Logging,
            clientFactory);

        auto mount = [&] (const TString& diskId, const TString& clientId) {
            auto request =
                std::make_shared<NProto::TMountVolumeRequest>();
            request->SetDiskId(diskId);
            request->MutableHeaders()->SetClientId(clientId);
            return multipleService->MountVolume(
                MakeIntrusive<TCallContext>(),
                std::move(request));
        };

        auto firstMount = mount("disk-0", "client-0");
        auto secondMount = mount("disk-1", "client-1");

        UNIT_ASSERT(!firstMount.HasValue());
        UNIT_ASSERT(!secondMount.HasValue());

        // The next round-robin slot is healthy, so a later MountVolume
        // completes while the first two are still waiting for their Root KMS
        // request deadline.
        auto thirdMount = mount("disk-2", "client-2");
        const auto& thirdMountResponse =
            thirdMount.GetValue(TDuration::Seconds(2));

        UNIT_ASSERT_C(
            !HasError(thirdMountResponse),
            thirdMountResponse.GetError());
        UNIT_ASSERT_VALUES_EQUAL(
            "disk-2",
            thirdMountResponse.GetVolume().GetDiskId());

        Sleep(TDuration::MilliSeconds(200));
        UNIT_ASSERT(!firstMount.HasValue());
        UNIT_ASSERT(!secondMount.HasValue());

        const auto& firstMountResponse =
            firstMount.GetValue(TDuration::Seconds(3));
        const auto& secondMountResponse =
            secondMount.GetValue(TDuration::Seconds(3));

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_GRPC_DEADLINE_EXCEEDED,
            firstMountResponse.GetError().GetCode(),
            firstMountResponse.GetError());
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_GRPC_DEADLINE_EXCEEDED,
            secondMountResponse.GetError().GetCode(),
            secondMountResponse.GetError());

        faultClient->Stop();
    }
}

}   // namespace NCloud::NBlockStore
