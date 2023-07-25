#include "encryption_key.h"

#include "encryption_test.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEncryptionKeyTest)
{
    Y_UNIT_TEST(ShouldProvideEncryptionKey)
    {
        TString encryptionKey = "01234567890123456789012345678901";

        TEncryptionKeyFile keyFile(encryptionKey);

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath(keyFile.GetPath());

        auto keyProvider = CreateDefaultEncryptionKeyProvider();

        auto keyOrError = keyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(!HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            encryptionKey,
            keyOrError.GetResult().GetKey());
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyFileNotExist)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath("nonexistent_file");

        auto keyProvider = CreateDefaultEncryptionKeyProvider();

        auto keyOrError = keyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyLengthIsInvalid)
    {
        TEncryptionKeyFile keyFile("key_with_invalid_length");

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath(keyFile.GetPath());

        auto keyProvider = CreateDefaultEncryptionKeyProvider();

        auto keyOrError = keyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyRingNotExist)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetKeyringId(-1);

        auto keyProvider = CreateDefaultEncryptionKeyProvider();

        auto keyOrError = keyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyPathIsEmpty)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);

        auto keyProvider = CreateDefaultEncryptionKeyProvider();

        auto keyOrError = keyProvider->GetKey(spec, {}).ExtractValue();
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore
