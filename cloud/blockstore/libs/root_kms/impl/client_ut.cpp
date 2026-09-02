#include "client.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/root_kms/iface/client.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/system/env.h>

#include <chrono>

namespace NCloud::NBlockStore {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture: NUnitTest::TBaseFixture
{
    const TString KekId = "nbs";

    ILoggingServicePtr Logging;
    IRootKmsClientPtr Client;

    void SetUp(NUnitTest::TTestContext&) override
    {
        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Logging->Start();
    }

    void TearDown(NUnitTest::TTestContext&) override
    {
        UNIT_ASSERT(Client);

        Client->Stop();
        Logging->Stop();
    }

    void StartClient(TDuration requestTimeout = {})
    {
        Client = CreateRootKmsClient(
            Logging,
            {.Address = "localhost:" + GetEnv("FAKE_ROOT_KMS_PORT"),
             .RootCertsFile = GetEnv("FAKE_ROOT_KMS_CA"),
             .CertChainFile = GetEnv("FAKE_ROOT_KMS_CLIENT_CRT"),
             .PrivateKeyFile = GetEnv("FAKE_ROOT_KMS_CLIENT_KEY"),
             .RequestTimeout = requestTimeout});
        Client->Start();
    }

    void WaitReady() const
    {
        while (!IsRootKmsAlive()) {
            Sleep(1s);
        }
    }

    bool IsRootKmsAlive() const
    {
        const auto future = Client->Decrypt(TString(), TString());
        const auto& [_, error] = future.GetValueSync();

        return error.GetCode() == E_GRPC_NOT_FOUND;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRootKmsClientTest)
{
    Y_UNIT_TEST_F(ShouldGenerateAndDecryptDEK, TFixture)
    {
        StartClient();
        WaitReady();

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

    Y_UNIT_TEST_F(ShouldReturnDeadlineExceededOnRequestTimeout, TFixture)
    {
        constexpr TDuration requestTimeout = 1ms;

        StartClient(requestTimeout);

        const auto future = Client->Decrypt(TString(), TString());
        const auto& [_, error] = future.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_GRPC_DEADLINE_EXCEEDED,
            error.GetCode(),
            error);
    }
}

}   // namespace NCloud::NBlockStore
