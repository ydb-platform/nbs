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
        config.MutableDiskAgent()->SetDedicatedDiskAgent(true);

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
        UNIT_ASSERT(!config.HasDiskAgent());
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
        UNIT_ASSERT(!config.HasDiskAgent());
    }

    // Verify that normalization preserves the static agent role, allows other
    // overrides, keeps extraction lossless, and remains idempotent.
    Y_UNIT_TEST(ShouldNormalizeDiskAgentOverrides)
    {
        // Extract the complete payload before applying process constraints.
        NProto::TBlockstoreConfig payload;
        payload.MutableStorageService()->SetNodeType("other");
        payload.MutableDiskAgent()->SetDedicatedDiskAgent(false);
        payload.MutableDiskAgent()->SetEnabled(true);
        payload.MutableDiskAgent()->SetAgentId("dynamic-agent");
        const auto originalPayload = payload.SerializeAsString();
        auto [config, error] = ExtractBlockstoreConfig(payload);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(originalPayload, config.SerializeAsString());

        // Preserve the initializer's prohibition of an embedded agent.
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableDiskAgent()->SetDedicatedDiskAgent(true);
        staticConfig.MutableDiskAgent()->SetEnabled(false);
        const auto originalStaticConfig = staticConfig.SerializeAsString();
        NormalizeDynamicBlockstoreConfig(staticConfig, config);
        UNIT_ASSERT(!config.HasStorageService());
        UNIT_ASSERT(!config.GetDiskAgent().HasDedicatedDiskAgent());
        UNIT_ASSERT(!config.GetDiskAgent().HasEnabled());
        UNIT_ASSERT_VALUES_EQUAL(
            "dynamic-agent",
            config.GetDiskAgent().GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(originalPayload, payload.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(
            originalStaticConfig,
            staticConfig.SerializeAsString());

        // Keep normalization idempotent, including an emptied agent section.
        const auto normalizedConfig = config.SerializeAsString();
        NormalizeDynamicBlockstoreConfig(staticConfig, config);
        UNIT_ASSERT_VALUES_EQUAL(normalizedConfig, config.SerializeAsString());
        config.MutableDiskAgent()->ClearAgentId();
        NormalizeDynamicBlockstoreConfig(staticConfig, config);
        UNIT_ASSERT(!config.HasDiskAgent());
        NormalizeDynamicBlockstoreConfig(staticConfig, config);
        UNIT_ASSERT_VALUES_EQUAL(0, config.ByteSizeLong());

        // Allow enabling an ordinary agent without changing its static role.
        staticConfig.MutableDiskAgent()->SetDedicatedDiskAgent(false);
        config.MutableDiskAgent()->SetDedicatedDiskAgent(true);
        config.MutableDiskAgent()->SetEnabled(true);
        NormalizeDynamicBlockstoreConfig(staticConfig, config);
        UNIT_ASSERT(!config.GetDiskAgent().HasDedicatedDiskAgent());
        UNIT_ASSERT(config.GetDiskAgent().HasEnabled());
        UNIT_ASSERT(config.GetDiskAgent().GetEnabled());

        // Preserve an explicit disabling override as well as an enabling one.
        config.MutableDiskAgent()->SetEnabled(false);
        NormalizeDynamicBlockstoreConfig(staticConfig, config);
        UNIT_ASSERT(config.GetDiskAgent().HasEnabled());
        UNIT_ASSERT(!config.GetDiskAgent().GetEnabled());
    }

    // Verify that equal static ports remain linked unless discovery is explicitly
    // overridden, including defaults and independent insecure/secure settings.
    Y_UNIT_TEST(ShouldNormalizeDiscoveryPorts)
    {
        // Link explicitly configured discovery ports to their server ports.
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableServer()->MutableServerConfig()->SetPort(100);
        staticConfig.MutableServer()->MutableServerConfig()->SetSecurePort(101);
        staticConfig.MutableDiscoveryService()->SetConductorInstancePort(100);
        staticConfig.MutableDiscoveryService()->SetConductorSecureInstancePort(
            101);

        {
            // Follow both server overrides and preserve other discovery fields.
            NProto::TBlockstoreConfig config;
            config.MutableServer()->MutableServerConfig()->SetPort(200);
            config.MutableServer()->MutableServerConfig()->SetSecurePort(201);
            config.MutableDiscoveryService()->SetConductorApiUrl("conductor");
            NormalizeDynamicBlockstoreConfig(staticConfig, config);
            UNIT_ASSERT_VALUES_EQUAL(
                200,
                config.GetDiscoveryService().GetConductorInstancePort());
            UNIT_ASSERT_VALUES_EQUAL(
                201,
                config.GetDiscoveryService().GetConductorSecureInstancePort());
            UNIT_ASSERT_VALUES_EQUAL(
                "conductor",
                config.GetDiscoveryService().GetConductorApiUrl());

            // Repeated normalization must leave the prepared source unchanged.
            const auto normalized = config.SerializeAsString();
            NormalizeDynamicBlockstoreConfig(staticConfig, config);
            UNIT_ASSERT_VALUES_EQUAL(normalized, config.SerializeAsString());

            // Follow a TLS-only update without adding the insecure port.
            config.MutableServer()->MutableServerConfig()->ClearPort();
            config.ClearDiscoveryService();
            NormalizeDynamicBlockstoreConfig(staticConfig, config);
            UNIT_ASSERT(!config.GetDiscoveryService().HasConductorInstancePort());
            UNIT_ASSERT_VALUES_EQUAL(
                201,
                config.GetDiscoveryService().GetConductorSecureInstancePort());
        }

        {
            // Respect explicit discovery overrides, including zero.
            NProto::TBlockstoreConfig config;
            config.MutableServer()->MutableServerConfig()->SetPort(200);
            config.MutableServer()->MutableServerConfig()->SetSecurePort(201);
            config.MutableDiscoveryService()->SetConductorInstancePort(300);
            config.MutableDiscoveryService()->SetConductorSecureInstancePort(0);
            NormalizeDynamicBlockstoreConfig(staticConfig, config);
            UNIT_ASSERT_VALUES_EQUAL(
                300,
                config.GetDiscoveryService().GetConductorInstancePort());
            UNIT_ASSERT(
                config.GetDiscoveryService().HasConductorSecureInstancePort());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                config.GetDiscoveryService().GetConductorSecureInstancePort());
        }

        {
            // Keep unequal ports independent while preserving the TLS link.
            auto independentConfig = staticConfig;
            independentConfig.MutableDiscoveryService()
                ->SetConductorInstancePort(300);
            NProto::TBlockstoreConfig config;
            config.MutableServer()->MutableServerConfig()->SetPort(200);
            config.MutableServer()->MutableServerConfig()->SetSecurePort(201);
            NormalizeDynamicBlockstoreConfig(independentConfig, config);
            UNIT_ASSERT(!config.GetDiscoveryService().HasConductorInstancePort());
            UNIT_ASSERT_VALUES_EQUAL(
                201,
                config.GetDiscoveryService().GetConductorSecureInstancePort());

            // Keep both ports absent when neither static pair is linked.
            independentConfig.MutableDiscoveryService()
                ->SetConductorSecureInstancePort(301);
            config.ClearDiscoveryService();
            NormalizeDynamicBlockstoreConfig(independentConfig, config);
            UNIT_ASSERT(!config.HasDiscoveryService());
        }

        {
            // Do not create discovery overrides when server ports are absent.
            NProto::TBlockstoreConfig config;
            config.MutableServer()->MutableServerConfig()->SetThreadsCount(2);
            NormalizeDynamicBlockstoreConfig(staticConfig, config);
            UNIT_ASSERT(!config.HasDiscoveryService());
        }

        {
            // Compare effective defaults when static server fields are absent.
            NProto::TBlockstoreConfig defaults;
            defaults.MutableDiscoveryService()->SetConductorInstancePort(9766);
            NProto::TBlockstoreConfig config;
            config.MutableServer()->MutableServerConfig()->SetPort(200);
            config.MutableServer()->MutableServerConfig()->SetSecurePort(201);
            NormalizeDynamicBlockstoreConfig(defaults, config);
            UNIT_ASSERT_VALUES_EQUAL(
                200,
                config.GetDiscoveryService().GetConductorInstancePort());
            UNIT_ASSERT_VALUES_EQUAL(
                201,
                config.GetDiscoveryService().GetConductorSecureInstancePort());
        }
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
