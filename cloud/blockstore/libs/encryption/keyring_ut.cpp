#include "encryptor.h"

#include "encryption_test.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/endpoints/keyring/keyring_endpoints_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>

#include <openssl/err.h>
#include <openssl/evp.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey = "01234567890123456789012345678901";

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    IEncryptionKeyProviderPtr KeyProvider;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        KeyProvider = CreateEncryptionKeyProvider(CreateKmsKeyProviderStub());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKeyringEncryptorTest)
{
    Y_UNIT_TEST_F(ShouldGetTheSameEncryptionKeyHashesFromFileAndKeyring, TFixture)
    {
        TString fileKeyHash;
        {
            TEncryptionKeyFile keyFile(DefaultEncryptionKey);

            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::ENCRYPTION_AES_XTS);
            auto& keyPath = *spec.MutableKeyPath();
            keyPath.SetFilePath(keyFile.GetPath());

            auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
            UNIT_ASSERT_C(!HasError(keyOrError), keyOrError.GetError());
            fileKeyHash = keyOrError.GetResult().GetHash();
            UNIT_ASSERT(!fileKeyHash.empty());
        }

        TString keyringKeyHash;
        {
            const TString guid = CreateGuidAsString();
            const TString nbsDesc = "nbs_" + guid;
            const TString endpointsDesc = "nbs_endpoints_" + guid;
            const TString keyName = "key_" + guid;

            auto mutableStorage = CreateKeyringMutableEndpointStorage(
                nbsDesc,
                endpointsDesc);

            auto initError = mutableStorage->Init();
            UNIT_ASSERT_C(!HasError(initError), initError);

            Y_DEFER {
                auto error = mutableStorage->Remove();
                UNIT_ASSERT_C(!HasError(error), error);
            };

            auto keyringOrError = mutableStorage->AddEndpoint(
                keyName,
                DefaultEncryptionKey);
            UNIT_ASSERT_C(!HasError(keyringOrError), keyringOrError.GetError());

            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::ENCRYPTION_AES_XTS);
            auto& keyPath = *spec.MutableKeyPath();
            keyPath.SetKeyringId(FromString<ui32>(keyringOrError.GetResult()));

            auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
            UNIT_ASSERT_C(!HasError(keyOrError), keyOrError.GetError());
            keyringKeyHash = keyOrError.GetResult().GetHash();
            UNIT_ASSERT(!keyringKeyHash.empty());
        }

        UNIT_ASSERT_VALUES_EQUAL(fileKeyHash, keyringKeyHash);
    }

    Y_UNIT_TEST_F(ShouldFailEncryptorCreationIfKeyringKeyLengthIsInvalid, TFixture)
    {
        const TString guid = CreateGuidAsString();
        const TString nbsDesc = "nbs_" + guid;
        const TString endpointsDesc = "nbs_endpoints_" + guid;
        const TString keyName = "key_" + guid;

        auto mutableStorage = CreateKeyringMutableEndpointStorage(
            nbsDesc,
            endpointsDesc);

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        auto keyringOrError = mutableStorage->AddEndpoint(
            keyName,
            "key_with_invalid_length");
        UNIT_ASSERT_C(!HasError(keyringOrError), keyringOrError.GetError());

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetKeyringId(FromString<ui32>(keyringOrError.GetResult()));

        auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore
