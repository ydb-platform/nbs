#include "encryption_key.h"

#include "encryption_test.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestRootKmsKeyProvider final
    : IRootKmsKeyProvider
{
    const TString EncryptionKey;

    explicit TTestRootKmsKeyProvider(TString key)
        : EncryptionKey(std::move(key))
    {}

    auto GetKey(const NProto::TKmsKey& kmsKey, const TString& diskId)
        -> TFuture<TResultOrError<TEncryptionKey>> override
    {
        Y_UNUSED(kmsKey);
        Y_UNUSED(diskId);

        return MakeFuture(
            TResultOrError<TEncryptionKey>{TEncryptionKey{EncryptionKey}});
    }

    auto GenerateDataEncryptionKey(const TString& diskId)
        -> TFuture<TResultOrError<NProto::TKmsKey>> override
    {
        Y_UNUSED(diskId);
        return MakeFuture(
            TResultOrError<NProto::TKmsKey>{MakeError(E_NOT_IMPLEMENTED)});
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    IEncryptionKeyProviderPtr KeyProvider;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        KeyProvider = CreateDefaultEncryptionKeyProvider();
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

    Y_UNIT_TEST(ShouldProvideEncryptionKeyFromRootKMS)
    {
        const TString encryptionKey = "01234567890123456789012345678901";

        NProto::TEncryptionSpec spec;
        spec.SetMode(NProto::ENCRYPTION_DEFAULT_AES_XTS);
        auto& keyPath = *spec.MutableKeyPath();
        keyPath.MutableKmsKey()->SetKekId("nbs");
        keyPath.MutableKmsKey()->SetEncryptedDEK("42");

        auto keyProvider = CreateEncryptionKeyProvider(
            CreateKmsKeyProviderStub(),
            std::make_shared<TTestRootKmsKeyProvider>(encryptionKey));

        auto keyOrError = keyProvider->GetKey(spec, {}).ExtractValue();
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
