#include "kms_client.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString TestDecrypt(
    TString address,
    ui32 port,
    TString keyId,
    TString ciphertext,
    TString token,
    TString targetName)
{
    NProto::TGrpcClientConfig config;
    config.SetAddress(address);
    config.SetPort(port);
    config.SetInsecure(false);
    config.SetSslTargetNameOverride(targetName);

    auto logging = CreateLoggingService("console");
    auto kmsClient = CreateKmsClient(logging, config);
    kmsClient->Start();
    Y_DEFER{
        kmsClient->Stop();
    };

    auto future = kmsClient->Decrypt(keyId, Base64Decode(ciphertext), token);
    auto response = future.GetValueSync();
    UNIT_ASSERT_C(!HasError(response), response.GetError());

    return Base64Encode(response.GetResult());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKmsClientTest)
{
    Y_UNIT_TEST(ShouldDecryptUsingRootKmsOnHwNbsStableLab)
    {
        auto iamToken = "XXXXX";

        auto plaintext = TestDecrypt(
            "sas09-ct7-17.cloud.yandex.net",
            4301,
            "rootKmsNbsTestsKeyId1",
            "AAAAAAAAAB5yb290S21zTmJzVGVzdHNLZXlJZDFfdmVyc2lvbjEAAAAMiK3GO9FosLi8lxgYAAAAA/a9px3l7hQXj/hRecGD2outvpE=",
            iamToken,
            "kms.private-api.hw-nbs-stable.cloud-lab.yandex.net");

        UNIT_ASSERT_VALUES_EQUAL("ping", plaintext);
    }

    Y_UNIT_TEST(ShouldDecryptUsingKmsOnTesting)
    {
        auto iamToken = "XXXXX";

        auto plaintext = TestDecrypt(
            "kms.cloud-testing.yandex.net",
            8443,
            "dq8f9tcfdei7un2opn0l",
            "AAAAAQAAABRkcThyb2JxZDRudmZsbm1hN3Y2ZgAAABD3QIcovaoWB+bsf+5OJHrZAAAADC0+Z37NMr+sbta7e1dzyJkoqkzZqAUfNHbM2xstdG7EeVs=",
            iamToken,
            {});

        UNIT_ASSERT_VALUES_EQUAL("pingpong", plaintext);
    }
}

}   // namespace NCloud::NBlockStore
