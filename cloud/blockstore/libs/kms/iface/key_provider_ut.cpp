#include "key_provider.h"

#include "compute_client.h"
#include "kms_client.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/public/api/protos/encryption.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore {

using namespace NIamClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestIamTokenClient: public IIamTokenClient
{
    const TResultOrError<TTokenInfo> Response;

    bool Executed = false;

    TTestIamTokenClient(TResultOrError<TTokenInfo> response)
        : Response(std::move(response))
    {}

    void Start() override
    {}
    void Stop() override
    {}

    TResponse GetToken() override
    {
        UNIT_FAIL("Don't use sync method");
        return GetTokenAsync().GetValueSync();
    }

    NThreading::TFuture<TResponse> GetTokenAsync() override
    {
        UNIT_ASSERT(!Executed);
        Executed = true;

        return MakeFuture(Response);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestComputeClient: public IComputeClient
{
    const TResultOrError<TString> Response;

    bool Executed = false;
    TString ExpectedDiskId;
    TString ExpectedTaskId;
    TString ExpectedToken;

    TTestComputeClient(TResultOrError<TString> response)
        : Response(std::move(response))
    {}

    void Start() override
    {}
    void Stop() override
    {}

    TFuture<TResponse> CreateTokenForDEK(
        const TString& diskId,
        const TString& taskId,
        const TString& authToken) override
    {
        UNIT_ASSERT(!Executed);
        Executed = true;

        UNIT_ASSERT_VALUES_EQUAL(ExpectedDiskId, diskId);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedTaskId, taskId);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedToken, authToken);
        return MakeFuture(Response);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestKmsClient: public IKmsClient
{
    const TResultOrError<TString> Response;

    bool Executed = false;
    TString ExpectedKeyId;
    TString ExpectedCiphertext;
    TString ExpectedToken;

    TTestKmsClient(TResultOrError<TString> response)
        : Response(std::move(response))
    {}

    void Start() override
    {}
    void Stop() override
    {}

    TFuture<TResponse> Decrypt(
        const TString& keyId,
        const TString& ciphertext,
        const TString& authToken) override
    {
        UNIT_ASSERT(!Executed);
        Executed = true;

        UNIT_ASSERT_VALUES_EQUAL(ExpectedKeyId, keyId);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedCiphertext, ciphertext);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedToken, authToken);
        return MakeFuture(Response);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKmsKeyProviderTest)
{
    Y_UNIT_TEST(ShouldProvideEncryptionKeyFromKMS)
    {
        auto encryptedDEK = "testEncryptedDEK";

        NProto::TKmsKey kmsKey;
        kmsKey.SetKekId("testKekId");
        kmsKey.SetEncryptedDEK(Base64Encode(encryptedDEK));
        kmsKey.SetTaskId("testTaskId");
        TString diskId = "testDiskId";
        TString nbsToken = "testNbsToken";
        TString computeToken = "testComputeToken";
        TString dek = "testDEK";

        auto iamTokenClient = std::make_shared<TTestIamTokenClient>(
            TTokenInfo(nbsToken, TInstant::Now()));

        auto computeClient = std::make_shared<TTestComputeClient>(computeToken);
        computeClient->ExpectedDiskId = diskId;
        computeClient->ExpectedTaskId = kmsKey.GetTaskId();
        computeClient->ExpectedToken = nbsToken;

        auto kmsClient = std::make_shared<TTestKmsClient>(dek);
        kmsClient->ExpectedKeyId = kmsKey.GetKekId();
        kmsClient->ExpectedCiphertext = encryptedDEK;
        kmsClient->ExpectedToken = computeToken;

        auto executor = TExecutor::Create("TestService");
        auto kmsKeyProvider = CreateKmsKeyProvider(
            executor,
            iamTokenClient,
            computeClient,
            kmsClient);

        executor->Start();
        Y_DEFER
        {
            executor->Stop();
        }

        auto future = kmsKeyProvider->GetKey(kmsKey, diskId);

        auto response = future.ExtractValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());

        auto key = response.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(dek, key.GetKey());
    }
}

}   // namespace NCloud::NBlockStore
