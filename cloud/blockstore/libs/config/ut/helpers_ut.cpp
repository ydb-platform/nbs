#include <cloud/blockstore/libs/config/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockstoreConfigHelpersTest)
{
    // Check that filtering removes static-only fields leaving others untouched.
    Y_UNIT_TEST(ShouldRemoveStaticOnlyFields)
    {
        NProto::TBlockstoreConfig config;
        config.MutableStorageService()->SetWriteBlobThreshold(300);
        config.MutableStorageService()->SetNodeType("other");
        config.MutableStorageService()->SetSchemeShardDir("/Root/other");
        auto* settings =
            config.MutableStorageService()->MutableConfigDispatcherSettings();
        settings->MutableDenyList()->AddNames("PrivateDatabaseConfigItem");
        auto* label = settings->AddAdditionalNodeLabels();
        label->SetKey("zone");
        label->SetValue("dynamic");
        config.MutableServer()
            ->MutableServerConfig()
            ->SetDynamicYamlConfigurationEnabled(false);

        RemoveStaticOnlyBlockstoreFields(config);

        UNIT_ASSERT_VALUES_EQUAL(
            300,
            config.GetStorageService().GetWriteBlobThreshold());
        UNIT_ASSERT(!config.GetStorageService().HasNodeType());
        UNIT_ASSERT(!config.GetStorageService().HasSchemeShardDir());
        UNIT_ASSERT(!config.GetStorageService().HasConfigDispatcherSettings());
        UNIT_ASSERT(!config.GetServer()
                         .GetServerConfig()
                         .HasDynamicYamlConfigurationEnabled());
    }

    // Check that filtering does not create absent configuration sections.
    Y_UNIT_TEST(ShouldNotCreateAbsentSections)
    {
        NProto::TBlockstoreConfig config;

        RemoveStaticOnlyBlockstoreFields(config);

        UNIT_ASSERT(!config.HasStorageService());
        UNIT_ASSERT(!config.HasServer());
    }
}

}   // namespace NCloud::NBlockStore
