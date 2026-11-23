#include <cloud/storage/core/libs/config/config_schema_path.h>
#include <cloud/storage/core/libs/config/ut/protos/runtime_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

namespace NCloud::NConfig {

Y_UNIT_TEST_SUITE(TConfigSchemaPathTest)
{
    // Verify paths for reused message types, sibling fields, and collections,
    // including the lifetime of a saved path after leaving its scope.
    Y_UNIT_TEST(ShouldBuildPathsDuringCallerTraversal)
    {
        const auto* root = NProto::NTest::TRuntimeConfig::descriptor();
        const auto* parameters = NProto::NTest::TParameters::descriptor();
        TString saved;

        // Build each branch independently although both use the same type.
        {
            TConfigSchemaPath path;
            UNIT_ASSERT_VALUES_EQUAL("", path.GetPath());
            for (const auto* name: {"Allowed", "Frozen"}) {
                path.Enter(*root->FindFieldByName(name));
                Y_DEFER
                {
                    path.Leave();
                };
                {
                    path.Enter(*parameters->FindFieldByName("Mutable"));
                    Y_DEFER
                    {
                        path.Leave();
                    };
                    saved = path.GetPath();
                    UNIT_ASSERT_VALUES_EQUAL(TString(name) + ".Mutable", saved);
                }
                UNIT_ASSERT_VALUES_EQUAL(name, path.GetPath());
            }
            UNIT_ASSERT_VALUES_EQUAL("", path.GetPath());

            // Use a collection suffix for both repeated and map fields.
            path.Enter(*root->FindFieldByName("Records"));
            UNIT_ASSERT_VALUES_EQUAL("Records[]", path.GetPath());
            path.Leave();
            path.Enter(*root->FindFieldByName("MessageMap"));
            UNIT_ASSERT_VALUES_EQUAL("MessageMap[]", path.GetPath());
            path.Leave();
        }
        UNIT_ASSERT_VALUES_EQUAL("Frozen.Mutable", saved);
    }

    // Verify that a oneof group and its message member are sibling paths,
    // with no group name inserted before the member's descendant fields.
    Y_UNIT_TEST(ShouldSeparateOneofGroupAndMemberPaths)
    {
        const auto* root = NProto::NTest::TRuntimeConfig::descriptor();
        TConfigSchemaPath path;
        path.Enter(*root->FindFieldByName("AllowedChild"));
        Y_DEFER
        {
            path.Leave();
        };

        // Visit the group as a terminal diagnostic node.
        {
            path.Enter(*root->FindOneofByName("FixedChoice"));
            Y_DEFER
            {
                path.Leave();
            };
            UNIT_ASSERT_VALUES_EQUAL(
                "AllowedChild.FixedChoice",
                path.GetPath());
        }

        // Descend through the selected member from the same parent.
        const auto* member = root->FindFieldByName("FixedMessage");
        path.Enter(*member);
        Y_DEFER
        {
            path.Leave();
        };
        path.Enter(*member->message_type()->FindFieldByName("Value"));
        Y_DEFER
        {
            path.Leave();
        };
        UNIT_ASSERT_VALUES_EQUAL(
            "AllowedChild.FixedMessage.Value",
            path.GetPath());
    }

    // Verify that invalid transitions throw without changing the current path,
    // and the same helper remains usable after catching the exception.
    Y_UNIT_TEST(ShouldRejectInvalidPathTransitions)
    {
        const auto* root = NProto::NTest::TRuntimeConfig::descriptor();
        TConfigSchemaPath path;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            path.Leave(),
            std::exception,
            "Cannot leave the root");
        UNIT_ASSERT_VALUES_EQUAL("", path.GetPath());

        // Reject descent through every kind of terminal node.
        for (const auto* name: {"FixedChoice", "Flag", "Records", "MessageMap"})
        {
            if (const auto* group = root->FindOneofByName(name)) {
                path.Enter(*group);
            } else {
                path.Enter(*root->FindFieldByName(name));
            }
            const auto previousPath = path.GetPath();
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                path.Enter(*root->FindFieldByName("Flag")),
                std::exception,
                "below terminal config schema path '" + previousPath + "'");
            UNIT_ASSERT_VALUES_EQUAL(previousPath, path.GetPath());
            path.Leave();
        }
        UNIT_ASSERT_VALUES_EQUAL("", path.GetPath());
    }
}

}   // namespace NCloud::NConfig
