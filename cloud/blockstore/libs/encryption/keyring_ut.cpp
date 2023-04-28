#include "encryptor.h"

#include "encryption_test.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/keyring/endpoints_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>

#include <openssl/err.h>
#include <openssl/evp.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey = "01234567890123456789012345678901";

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKeyringEncryptorTest)
{
    Y_UNIT_TEST(ShouldGetTheSameEncryptionKeyHashesFromFileAndKeyring)
    {
        TString fileKeyHash;
        {
            TEncryptionKeyFile keyFile(DefaultEncryptionKey);

            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::ENCRYPTION_AES_XTS);
            auto& keyPath = *spec.MutableKeyPath();
            keyPath.SetFilePath(keyFile.GetPath());

            auto hashOrError = ComputeEncryptionKeyHash(spec);
            UNIT_ASSERT_C(!HasError(hashOrError), hashOrError.GetError());
            fileKeyHash = hashOrError.GetResult();
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

            auto keyOrError = mutableStorage->AddEndpoint(
                keyName,
                DefaultEncryptionKey);
            UNIT_ASSERT_C(!HasError(keyOrError), keyOrError.GetError());

            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::ENCRYPTION_AES_XTS);
            auto& keyPath = *spec.MutableKeyPath();
            keyPath.SetKeyringId(keyOrError.GetResult());

            auto hashOrError = ComputeEncryptionKeyHash(spec);
            UNIT_ASSERT_C(!HasError(hashOrError), hashOrError.GetError());
            keyringKeyHash = hashOrError.GetResult();
            UNIT_ASSERT(!keyringKeyHash.empty());
        }

        UNIT_ASSERT_VALUES_EQUAL(fileKeyHash, keyringKeyHash);
    }

    Y_UNIT_TEST(ShouldFailEncryptorCreationIfKeyringKeyLengthIsInvalid)
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

        auto keyOrError = mutableStorage->AddEndpoint(
            keyName,
            "key_with_invalid_length");
        UNIT_ASSERT_C(!HasError(keyOrError), keyOrError.GetError());

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetKeyringId(keyOrError.GetResult());

        {
            TString keyHash;
            auto resultOrError = CreateAesXtsEncryptor(keyPath, keyHash);
            UNIT_ASSERT(HasError(resultOrError));
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                resultOrError.GetError().GetCode());
        }

        {
            auto resultOrError = ComputeEncryptionKeyHash(spec);
            UNIT_ASSERT(HasError(resultOrError));
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                resultOrError.GetError().GetCode());
        }
    }
}

}   // namespace NCloud::NBlockStore
