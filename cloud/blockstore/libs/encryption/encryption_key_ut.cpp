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

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();

        auto [key, error] = encryptionKeyProvider->GetKey(spec);
        UNIT_ASSERT(!HasError(error));

        UNIT_ASSERT_VALUES_EQUAL(encryptionKey, key.GetKey());
    }

    Y_UNIT_TEST(DefaultEncryptionSpecShouldProvideEmptyHash)
    {
        NProto::TEncryptionSpec spec;
        UNIT_ASSERT(spec.GetMode() == NProto::NO_ENCRYPTION);

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();
        auto hashOrError = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT_C(!HasError(hashOrError), hashOrError.GetError());
        UNIT_ASSERT_VALUES_EQUAL("", hashOrError.GetResult());
    }

    Y_UNIT_TEST(EmptyEncryptionSpecShouldProvideEmptyHash)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::NO_ENCRYPTION);
        spec.SetKeyHash("");

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();
        auto hashOrError = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT_C(!HasError(hashOrError), hashOrError.GetError());
        UNIT_ASSERT_VALUES_EQUAL("", hashOrError.GetResult());
    }

    Y_UNIT_TEST(ShouldComputeEncryptionKeyHash)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        const TString encryptionKey
            = "01234567890123456789012345678901";
        const TString expectedKeyHash
            = "gqKPgb5Hat9PnrAehwwaG0g3BBkSbmjDRyyVYIcnMUCIMF0NICXnZzkNJMdHOfOq";
        const TString otherEncryptionKey
            = "11111111111111111111111111111111";

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();

        TEncryptionKeyFile keyFile1(encryptionKey, "test_key1");
        keyPath.SetFilePath(keyFile1.GetPath());
        auto hashOrError1 = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT_C(!HasError(hashOrError1), hashOrError1.GetError());
        UNIT_ASSERT_VALUES_EQUAL(expectedKeyHash, hashOrError1.GetResult());

        TEncryptionKeyFile keyFile2(otherEncryptionKey, "test_key2");
        keyPath.SetFilePath(keyFile2.GetPath());
        auto hashOrError2 = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT_C(!HasError(hashOrError2), hashOrError2.GetError());
        UNIT_ASSERT(hashOrError2.GetResult() != otherEncryptionKey);

        UNIT_ASSERT(hashOrError1.GetResult() != hashOrError2.GetResult());
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyFileNotExist)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath("nonexistent_file");

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();

        auto keyOrError = encryptionKeyProvider->GetKey(spec);
        UNIT_ASSERT(HasError(keyOrError));

        auto hashOrError = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT(HasError(hashOrError));
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyLengthIsInvalid)
    {
        TEncryptionKeyFile keyFile("key_with_invalid_length");

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetFilePath(keyFile.GetPath());

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();

        auto keyOrError = encryptionKeyProvider->GetKey(spec);
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());

        auto hashOrError = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT(HasError(hashOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            hashOrError.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyRingNotExist)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.SetKeyringId(-1);

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();

        auto keyOrError = encryptionKeyProvider->GetKey(spec);
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());

        auto hashOrError = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT(HasError(hashOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            hashOrError.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFailToProvideKeyIfKeyPathIsEmpty)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();

        auto keyOrError = encryptionKeyProvider->GetKey(spec);
        UNIT_ASSERT(HasError(keyOrError));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            keyOrError.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReturnEmptyKeyHashIfEncryptionSpecIsEmpty)
    {
        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_AES_XTS);

        auto encryptionKeyProvider = CreateEncryptionKeyProvider();
        auto hashOrError = encryptionKeyProvider->GetKeyHash(spec);
        UNIT_ASSERT_C(!HasError(hashOrError), hashOrError.GetError());
        UNIT_ASSERT_VALUES_EQUAL("", hashOrError.GetResult());
    }
}

}   // namespace NCloud::NBlockStore
