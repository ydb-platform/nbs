#include "encryption_key.h"

#include "encryption_test.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

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

Y_UNIT_TEST_SUITE(TEncryptionKeyTest)
{
    Y_UNIT_TEST_F(ShouldProvideEncryptionKey, TFixture)
    {
        TString encryptionKey = "01234567890123456789012345678901";

        TEncryptionKeyFile keyFile(encryptionKey);

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath(keyFile.GetPath());

        auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(!HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            encryptionKey,
            keyOrError.GetResult().GetKey());
    }

    Y_UNIT_TEST_F(ShouldFailToProvideKeyIfKeyFileNotExist, TFixture)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath("nonexistent_file");

        auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
    }

    Y_UNIT_TEST_F(ShouldFailToProvideKeyIfKeyLengthIsInvalid, TFixture)
    {
        TEncryptionKeyFile keyFile("key_with_invalid_length");

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath(keyFile.GetPath());

        auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToProvideKeyIfKeyRingNotExist, TFixture)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetKeyringId(-1);

        auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }

    Y_UNIT_TEST_F(ShouldFailToProvideKeyIfKeyPathIsEmpty, TFixture)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);

        auto keyOrError = KeyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore
