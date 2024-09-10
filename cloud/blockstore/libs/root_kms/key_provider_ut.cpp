#include "key_provider.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRootKmsKeyProviderTest)
{
    Y_UNIT_TEST(ShouldGenerateUnencyptedKey)
    {
        const TString diskId = "vol0";
        auto keyProvider = CreateRootKmsKeyProvider();

        NProto::TKmsKey generatedKey = [&] {
            auto [key, error] =
                keyProvider->GenerateDataEncryptionKey(diskId).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL("", key.GetKekId());
            UNIT_ASSERT_VALUES_UNEQUAL("", key.GetEncryptedDEK());

            return key;
        }();

        {
            NProto::TKmsKey kmsKey;
            kmsKey.SetKekId("kek");
            kmsKey.SetEncryptedDEK("xxx");
            const auto& [_, error] =
                keyProvider->GetKey(kmsKey, diskId).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                error.GetCode(),
                error);
        }

        const auto& [key, error] =
            keyProvider->GetKey(generatedKey, diskId).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        UNIT_ASSERT_VALUES_EQUAL(
            Base64Decode(generatedKey.GetEncryptedDEK()),
            key.GetKey());
    }
}

}   // namespace NCloud::NBlockStore
