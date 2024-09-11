#include "encryption_key.h"

#include "encryption_test.h"

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestRootKmsKeyProvider final
    : IRootKmsKeyProvider
{
    const TString EncryptionKey;
    const TString KekId;

    TTestRootKmsKeyProvider(TString encryptionKey, TString kekId)
        : EncryptionKey(std::move(encryptionKey))
        , KekId(std::move(kekId))
    {}

    auto GetKey(const NProto::TKmsKey& kmsKey, const TString& diskId)
        -> TFuture<TResultOrError<TEncryptionKey>> override
    {
        Y_UNUSED(diskId);

        return MakeFuture(TResultOrError<TEncryptionKey>(
            Base64Decode(kmsKey.GetEncryptedDEK())));
    }

    auto GenerateDataEncryptionKey(const TString& diskId)
        -> TFuture<TResultOrError<NProto::TKmsKey>> override
    {
        Y_UNUSED(diskId);

        NProto::TKmsKey kmsKey;
        kmsKey.SetKekId(KekId);
        kmsKey.SetEncryptedDEK(Base64Encode(EncryptionKey));

        return MakeFuture(TResultOrError<NProto::TKmsKey>(std::move(kmsKey)));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TString EncryptionKey = "01234567890123456789012345678901";
    const TString KekId = "nbs";
    const TString DiskId = "vol0";

    IDefaultEncryptionKeyProviderPtr KeyProvider;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        KeyProvider = CreateDefaultEncryptionKeyProvider(
            std::make_shared<TTestRootKmsKeyProvider>(EncryptionKey, KekId));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDefaultEncryptionKeyTest)
{
    Y_UNIT_TEST_F(ShouldProvideEncryptionKey, TFixture)
    {
        auto [encryptedKey, error] =
            KeyProvider->GenerateDataEncryptionKey(DiskId).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        UNIT_ASSERT_VALUES_EQUAL(KekId, encryptedKey.GetKekId());
        UNIT_ASSERT_VALUES_EQUAL(
            Base64Encode(EncryptionKey),
            encryptedKey.GetEncryptedDEK());

        {
            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::ENCRYPTION_AES_XTS);

            auto [_, error] =
                KeyProvider->GetKey(spec, DiskId).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::NO_ENCRYPTION);

            auto [_, error] =
                KeyProvider->GetKey(spec, DiskId).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::ENCRYPTION_DEFAULT_AES_XTS);

            auto [_, error] =
                KeyProvider->GetKey(spec, DiskId).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            NProto::TEncryptionSpec spec;
            spec.SetMode(NProto::ENCRYPTION_DEFAULT_AES_XTS);
            *spec.MutableKeyPath()->MutableKmsKey() = encryptedKey;

            auto [key, error] =
                KeyProvider->GetKey(spec, DiskId).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(EncryptionKey, key.GetKey());
        }
    }
}

}   // namespace NCloud::NBlockStore
