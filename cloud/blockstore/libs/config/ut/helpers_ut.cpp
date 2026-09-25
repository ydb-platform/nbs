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

    // Check that filtering preserves explicit defaults and sibling sections
    // while removing an emptied ServerConfig.
    Y_UNIT_TEST(ShouldPreserveDefaultsAndSiblingSections)
    {
        // Keep explicit default values alongside ignored overrides.
        NProto::TBlockstoreConfig config;
        config.MutableStorageService()->SetWriteBlobThreshold(0);
        config.MutableStorageService()->SetNodeType("other");
        auto* server = config.MutableServer();
        server->MutableServerConfig()->SetPort(0);
        server->MutableServerConfig()->SetDynamicYamlConfigurationEnabled(
            false);
        server->MutableLocalServiceConfig()->SetDataDir("/tmp/local");

        RemoveStaticOnlyBlockstoreFields(config);

        UNIT_ASSERT(config.GetStorageService().HasWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            config.GetStorageService().GetWriteBlobThreshold());
        UNIT_ASSERT(config.GetServer().GetServerConfig().HasPort());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            config.GetServer().GetServerConfig().GetPort());

        // Remove the empty child without discarding another Server section.
        config.MutableServer()->MutableServerConfig()->ClearPort();
        RemoveStaticOnlyBlockstoreFields(config);

        UNIT_ASSERT(!config.GetServer().HasServerConfig());
        UNIT_ASSERT_VALUES_EQUAL(
            "/tmp/local",
            config.GetServer().GetLocalServiceConfig().GetDataDir());
    }

    // Check that filtering does not create absent configuration sections.
    Y_UNIT_TEST(ShouldNotCreateAbsentSections)
    {
        NProto::TBlockstoreConfig config;

        RemoveStaticOnlyBlockstoreFields(config);

        UNIT_ASSERT(!config.HasStorageService());
        UNIT_ASSERT(!config.HasServer());
    }

    // Check that parser errors retain diagnostics and remain distinguishable
    // from unexpected payload types regardless of the original error code.
    Y_UNIT_TEST(ShouldDistinguishParserErrorsFromUnexpectedPayloads)
    {
        // Classify a parser error independently of the code it carries.
        const auto parseError =
            MakeError(E_INVALID_STATE, "parser diagnostics");
        const auto result = ExtractBlockstoreConfig(parseError);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            result.GetError().GetMessage(),
            FormatError(parseError));

        // Reject even a success-coded TError instead of accepting an empty
        // config.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ExtractBlockstoreConfig(NCloud::NProto::TError())
                .GetError()
                .GetCode());

        // Identify an unexpected type without exposing its field values.
        NProto::TStorageServiceConfig unexpectedConfig;
        unexpectedConfig.SetNodeType("private-secret-value");
        const auto unexpectedResult = ExtractBlockstoreConfig(unexpectedConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            unexpectedResult.GetError().GetCode());
        UNIT_ASSERT_STRING_CONTAINS(
            unexpectedResult.GetError().GetMessage(),
            NProto::TStorageServiceConfig::descriptor()->full_name());
        UNIT_ASSERT_STRING_CONTAINS(
            unexpectedResult.GetError().GetMessage(),
            NProto::TBlockstoreConfig::descriptor()->full_name());
        UNIT_ASSERT(!unexpectedResult.GetError().GetMessage().Contains(
            "private-secret-value"));
    }
}

}   // namespace NCloud::NBlockStore
