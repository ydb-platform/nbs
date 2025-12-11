#include "keyring.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKeyringTest)
{
    Y_UNIT_TEST(ShouldHandleKeyrings)
    {
        const TString rootDesc = "nbs_test_" + CreateGuidAsString();
        TString subRootDesc = "test_sub_keyring";

        THashMap<TString, TString> loadedUserKeys;
        for (size_t i = 0; i < 5; ++i) {
            auto id = std::to_string(i + 1);
            loadedUserKeys.emplace("test_key" + id, "test_data" + id);
        }

        // create keyring tree
        {
            auto process = TKeyring::GetRoot(TKeyring::Process);
            UNIT_ASSERT(process);

            auto root = process.AddKeyring(rootDesc);
            UNIT_ASSERT(root);

            auto subRoot = root.AddKeyring(subRootDesc);
            UNIT_ASSERT(subRoot);

            for (auto it: loadedUserKeys) {
                auto userKey = subRoot.AddUserKey(it.first, it.second);
                UNIT_ASSERT(userKey);
            }

            auto user = TKeyring::GetRoot(TKeyring::User);
            UNIT_ASSERT(user);

            bool linkRes = user.LinkKeyring(root);
            UNIT_ASSERT(linkRes);
        }

        // remove keyring tree
        Y_DEFER
        {
            auto process = TKeyring::GetRoot(TKeyring::Process);
            UNIT_ASSERT(process);

            auto root = process.SearchKeyring(rootDesc);
            UNIT_ASSERT(root);

            auto res1 = process.UnlinkKeyring(root);
            UNIT_ASSERT(res1);

            auto user = TKeyring::GetRoot(TKeyring::User);
            UNIT_ASSERT(user);

            auto res2 = user.UnlinkKeyring(root);
            UNIT_ASSERT(res2);
        }

        auto RootChecker = [&](const TKeyring& root)
        {
            UNIT_ASSERT(root);

            auto subRoot = root.SearchKeyring(subRootDesc);
            UNIT_ASSERT(subRoot);

            auto userKeys = subRoot.GetUserKeys();
            UNIT_ASSERT_EQUAL(userKeys.size(), loadedUserKeys.size());

            for (const auto& userKey: userKeys) {
                auto it = loadedUserKeys.find(userKey.GetDesc());
                UNIT_ASSERT(it != loadedUserKeys.end());
                UNIT_ASSERT_EQUAL(it->second, userKey.GetValue());
            }
        };

        // read keyring tree using /proc/keys
        {
            auto root = TKeyring::GetProcKey(rootDesc);
            RootChecker(root);
        }

        // read keyring tree using keyctl
        {
            auto user = TKeyring::GetRoot(TKeyring::User);

            auto root = user.SearchKeyring(rootDesc);
            RootChecker(root);
        }
    }
}

}   // namespace NCloud
