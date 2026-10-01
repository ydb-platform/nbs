#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/config/blockstore_config_holder.h>
#include <cloud/blockstore/libs/config/blockstore_config_provider.h>
#include <cloud/blockstore/libs/config/blockstore_config_provider_private.h>
#include <cloud/blockstore/libs/config/opaque_config_parser.h>

#include <cloud/storage/core/libs/config/runtime_config.h>

#include <contrib/ydb/core/control/immediate_control_board_impl.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>

#include <atomic>
#include <thread>
#include <utility>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TBlockstoreConfig MakeConfig(ui32 value)
{
    NProto::TBlockstoreConfig config;
    config.MutableServer()->MutableServerConfig()->SetPort(value);
    config.MutableStorageService()->SetWriteBlobThreshold(value);
    return config;
}

NCloud::NProto::TFeatureConfig* AddFeature(
    NProto::TBlockstoreConfig& config,
    const TString& name,
    const TString& value,
    const TString& cloudId)
{
    auto* feature = config.MutableFeatures()->AddFeatures();
    feature->SetName(name);
    feature->SetValue(value);
    feature->MutableWhitelist()->AddCloudIds(cloudId);
    return feature;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// Aggregate protobuf merge, runtime wrapper, immutable lifetime, atomic
// publication, and explicit ICB override coverage.
Y_UNIT_TEST_SUITE(TBlockstoreConfigTest)
{
    // Verify runtime marker consistency across the complete BlockStore schema.
    Y_UNIT_TEST(ShouldValidateRuntimeConfigSchema)
    {
        NConfig::ValidateRuntimeConfigSchema(
            *NProto::TBlockstoreConfig::descriptor());
    }

    // Verify startup acceptance, runtime RO preservation, RW fallback on
    // removal, and adoption of a changed RO value at the next node startup.
    Y_UNIT_TEST(ShouldSeparateStartupAndRuntimeMerge)
    {
        // Distinguish the static fallback from accepted startup overrides.
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableStorageService()->SetWriteBlobThreshold(100);
        staticConfig.MutableStorageService()->SetListVolumesConcurrency(10);
        staticConfig.MutableStorageService()->SetSchemeShardDir("/static");
        auto dynamicConfig = staticConfig;
        dynamicConfig.MutableStorageService()->SetWriteBlobThreshold(200);
        dynamicConfig.MutableStorageService()->SetListVolumesConcurrency(20);
        dynamicConfig.MutableStorageService()->SetSchemeShardDir("/forbidden");
        NormalizeDynamicBlockstoreConfig(staticConfig, dynamicConfig);
        const auto startup = MergeBlockstoreConfigSources(
            staticConfig,
            dynamicConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            startup.GetStorageService().GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            startup.GetStorageService().GetListVolumesConcurrency());
        UNIT_ASSERT_VALUES_EQUAL(
            "/static",
            startup.GetStorageService().GetSchemeShardDir());

        // Apply a mixed update and report the startup-only parameter path.
        dynamicConfig.MutableStorageService()->SetWriteBlobThreshold(300);
        dynamicConfig.MutableStorageService()->SetListVolumesConcurrency(30);
        NProto::TBlockstoreConfig runtime;
        const auto diagnostics = PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            runtime.GetStorageService().GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            runtime.GetStorageService().GetListVolumesConcurrency());
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at(
                "StorageService.ListVolumesConcurrency") ==
            NConfig::ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);

        // Remove the dynamic source without pinning the startup RW override.
        NProto::TBlockstoreConfig removed;
        const auto removedDiagnostics =
            PrepareRuntimeBlockstoreConfig(staticConfig, startup, {}, removed);
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            removed.GetStorageService().GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            removed.GetStorageService().GetListVolumesConcurrency());
        UNIT_ASSERT_VALUES_EQUAL(1, removedDiagnostics.IgnoredPaths.size());

        // Start a new node from the current source and accept its new RO value.
        const auto restarted = MergeBlockstoreConfigSources(
            staticConfig,
            dynamicConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            30,
            restarted.GetStorageService().GetListVolumesConcurrency());
    }

    // Verify replay comparison after repeated merge, runtime Features keyed
    // replacement, and restoration of static Features on source removal.
    Y_UNIT_TEST(ShouldUpdateFeaturesAndPreserveStartupCollections)
    {
        // Accept a startup collection appended to static and a feature
        // override.
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableStorageService()->AddKnownSpareNodes("static-node");
        AddFeature(staticConfig, "A", "static", "static-cloud");
        NProto::TBlockstoreConfig dynamicConfig;
        dynamicConfig.MutableStorageService()->AddKnownSpareNodes(
            "startup-node");
        AddFeature(dynamicConfig, "A", "startup", "startup-cloud");
        const auto startup = MergeBlockstoreConfigSources(
            staticConfig,
            dynamicConfig);

        // Compare the merged replay, not the raw source, to the startup list.
        NProto::TBlockstoreConfig replay;
        const auto replayDiagnostics = PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            replay);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            replay.GetStorageService().KnownSpareNodesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, replayDiagnostics.IgnoredPaths.size());

        // Replace a complete feature record and keep the startup-only list.
        dynamicConfig.MutableStorageService()->ClearKnownSpareNodes();
        dynamicConfig.MutableFeatures()->ClearFeatures();
        dynamicConfig.MutableFeatures()->AddFeatures()->SetName("A");
        AddFeature(dynamicConfig, "A", "ignored-duplicate", "ignored-cloud");
        AddFeature(dynamicConfig, "B", "runtime", "runtime-cloud");
        NProto::TBlockstoreConfig runtime;
        const auto diagnostics = PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetFeatures().FeaturesSize());
        UNIT_ASSERT(!runtime.GetFeatures().GetFeatures(0).HasValue());
        UNIT_ASSERT(!runtime.GetFeatures().GetFeatures(0).HasWhitelist());
        UNIT_ASSERT_VALUES_EQUAL(
            "runtime",
            runtime.GetFeatures().GetFeatures(1).GetValue());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            runtime.GetStorageService().KnownSpareNodesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("StorageService.KnownSpareNodes[]") ==
            NConfig::ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);

        // Remove runtime Features and recover the static keyed record.
        // Reuse the previous output to verify that preparation replaces it.
        auto removed = runtime;
        const auto removedDiagnostics =
            PrepareRuntimeBlockstoreConfig(staticConfig, startup, {}, removed);
        UNIT_ASSERT_VALUES_EQUAL(1, removed.GetFeatures().FeaturesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "static",
            removed.GetFeatures().GetFeatures(0).GetValue());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            removed.GetStorageService().KnownSpareNodesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, removedDiagnostics.IgnoredPaths.size());
    }

    // Verify that the startup factory normalizes CMS overrides while keeping
    // static-only values, the dedicated agent role, and source messages intact.
    Y_UNIT_TEST(ShouldNormalizeStartupSourcesBeforeBuildingAdapters)
    {
        // Link static discovery and server ports and fix the agent role.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableDiscoveryService()->SetConductorInstancePort(100);
        staticConfig.MutableDiskAgent()->SetDedicatedDiskAgent(true);
        staticConfig.MutableDiskAgent()->SetEnabled(false);
        staticConfig.MutableStorageService()->SetSchemeShardDir("/static");
        const auto staticBytes = staticConfig.SerializeAsString();

        // Change the server port while attempting forbidden host overrides.
        auto dynamicConfig = MakeConfig(200);
        dynamicConfig.MutableDiskAgent()->SetDedicatedDiskAgent(false);
        dynamicConfig.MutableDiskAgent()->SetEnabled(true);
        dynamicConfig.MutableStorageService()->SetSchemeShardDir("/cms");
        const auto dynamicBytes = dynamicConfig.SerializeAsString();
        const auto config = MakeStartupBlockstoreConfig(
            staticConfig,
            dynamicConfig,
            std::make_shared<NStorage::TStorageConfigControls>());

        // Apply the linked port change and retain the static host restrictions.
        UNIT_ASSERT_VALUES_EQUAL(200, config->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            config->GetDiscoveryServiceConfig()->GetConductorInstancePort());
        UNIT_ASSERT(config->GetDiskAgentConfig()->GetDedicatedDiskAgent());
        UNIT_ASSERT(!config->GetDiskAgentConfig()->GetEnabled());
        UNIT_ASSERT_VALUES_EQUAL(
            "/static",
            config->GetStorageConfig()->GetSchemeShardDir());
        // Keep normalization and merging isolated from both source messages.
        UNIT_ASSERT_VALUES_EQUAL(staticBytes, staticConfig.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(
            dynamicBytes,
            dynamicConfig.SerializeAsString());
    }

    // Verify that the runtime factory preserves its sources, filters overrides,
    // returns separate diagnostics, and leaves live ICB defaults unchanged.
    Y_UNIT_TEST(ShouldBuildRuntimeConfigWithoutChangingSourcesOrControls)
    {
        // Keep distinct static, startup, and operator values.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetListVolumesConcurrency(10);
        staticConfig.MutableStorageService()->SetSchemeShardDir("/static");
        staticConfig.MutableDiskAgent()->SetDedicatedDiskAgent(true);
        staticConfig.MutableDiskAgent()->SetEnabled(false);
        auto startupConfig = staticConfig;
        startupConfig.MutableStorageService()->SetWriteBlobThreshold(200);
        startupConfig.MutableStorageService()->SetListVolumesConcurrency(20);
        auto controls = std::make_shared<NStorage::TStorageConfigControls>(
            startupConfig.GetStorageService());
        NKikimr::TControlBoard board;
        controls->Register(board);
        NKikimr::TControlWrapper control;
        UNIT_ASSERT(!board.RegisterSharedControl(
            control,
            "BlockStore_WriteBlobThreshold"));
        TAtomic previousValue = {};
        board.SetValue("BlockStore_WriteBlobThreshold", 400, previousValue);

        // Request mutable values together with startup-only and host overrides.
        auto dynamicConfig = MakeConfig(300);
        dynamicConfig.MutableStorageService()->SetListVolumesConcurrency(30);
        dynamicConfig.MutableStorageService()->SetSchemeShardDir("/cms");
        dynamicConfig.MutableDiskAgent()->SetDedicatedDiskAgent(false);
        dynamicConfig.MutableDiskAgent()->SetEnabled(true);
        const auto staticBytes = staticConfig.SerializeAsString();
        const auto startupBytes = startupConfig.SerializeAsString();
        const auto dynamicBytes = dynamicConfig.SerializeAsString();
        TBlockstoreConfigExtraParameters extraParameters;
        extraParameters.DiskAgent.Rack = "rack";
        extraParameters.DiskAgent.NetworkMbitThroughput = 1000;
        NConfig::TRuntimeConfigDiagnostics diagnostics;
        const auto config = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            dynamicConfig,
            controls,
            diagnostics,
            extraParameters);

        // Keep host restrictions and startup-only values in the ready adapters.
        const auto& storage = *config->GetStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            storage.GetConfigProto().GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(20, storage.GetListVolumesConcurrency());
        UNIT_ASSERT_VALUES_EQUAL("/static", storage.GetSchemeShardDir());
        UNIT_ASSERT_VALUES_EQUAL(100, config->GetServerConfig()->GetPort());
        UNIT_ASSERT(config->GetDiskAgentConfig()->GetDedicatedDiskAgent());
        UNIT_ASSERT(!config->GetDiskAgentConfig()->GetEnabled());
        UNIT_ASSERT_VALUES_EQUAL(
            "rack",
            config->GetDiskAgentConfig()->GetRack());
        UNIT_ASSERT_VALUES_EQUAL(
            1000,
            config->GetDiskAgentConfig()->GetNetworkMbitThroughput());
        UNIT_ASSERT_VALUES_EQUAL(2, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at(
                "StorageService.ListVolumesConcurrency") ==
            NConfig::ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);

        // Leave publication and control updates to the caller.
        UNIT_ASSERT_VALUES_EQUAL(200, control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(400, storage.GetWriteBlobThreshold());
        UNIT_ASSERT(storage.GetControls() == controls);
        UNIT_ASSERT_VALUES_EQUAL(staticBytes, staticConfig.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(
            startupBytes,
            startupConfig.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(
            dynamicBytes,
            dynamicConfig.SerializeAsString());

        // Rebuild from an absent source and replace the previous diagnostics.
        const auto removed = MakeRuntimeBlockstoreConfig(
            staticConfig,
            startupConfig,
            {},
            controls,
            diagnostics);
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            removed->GetStorageConfig()
                ->GetConfigProto()
                .GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            removed->GetStorageConfig()->GetListVolumesConcurrency());
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT_VALUES_EQUAL(200, control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(400, static_cast<i64>(control));
    }

    // Check that the aggregate proto contains 19 top-level configuration
    // sections, including the representative sections below.
    Y_UNIT_TEST(ShouldExposeCompleteAggregateSchema)
    {
        const auto* descriptor = NProto::TBlockstoreConfig::descriptor();

        UNIT_ASSERT_VALUES_EQUAL(19, descriptor->field_count());

        // Selective check
        UNIT_ASSERT(descriptor->FindFieldByName("Server"));
        UNIT_ASSERT(descriptor->FindFieldByName("StorageService"));
        UNIT_ASSERT(descriptor->FindFieldByName("Features"));
        UNIT_ASSERT(descriptor->FindFieldByName("LocalNVMe"));
    }

    // Check presence for every singular scalar reachable from the aggregate,
    // including fields inside repeated messages and map values.
    Y_UNIT_TEST(ShouldTrackPresenceForAllConfigurationScalars)
    {
        THashSet<const google::protobuf::Descriptor*> visited;
        TVector<const google::protobuf::Descriptor*> pending = {
            NProto::TBlockstoreConfig::descriptor()};
        TString missingPresence;

        // Visit shared types once and follow message-valued map entries without
        // imposing presence on the generated map key and value fields.
        while (!pending.empty()) {
            const auto* descriptor = pending.back();
            pending.pop_back();
            if (!visited.insert(descriptor).second) {
                continue;
            }

            for (int i = 0; i < descriptor->field_count(); ++i) {
                const auto* field = descriptor->field(i);
                if (const auto* nested = field->message_type()) {
                    if (field->is_map()) {
                        nested = nested->map_value()->message_type();
                    }
                    if (nested) {
                        pending.push_back(nested);
                    }
                } else if (!field->is_repeated() && !field->has_presence()) {
                    missingPresence += field->full_name();
                    missingPresence += '\n';
                }
            }
        }

        // Report all offending fields so schema changes cannot silently turn
        // explicit default values into absent overrides.
        UNIT_ASSERT_C(missingPresence.empty(), missingPresence);
    }

    // Check that YAML preserves explicit scalar defaults through snapshot
    // construction and that omitted fields retain the static configuration.
    Y_UNIT_TEST(ShouldApplyExplicitYamlDefaultsToSnapshot)
    {
        const TString yamlConfigs[] = {
            R"(
rdma:
  client_enabled: false
  server_enabled: false
  disk_agent_target_enabled: false
  blockstore_server_target_enabled: false
  client:
    aligned_data_enabled: false
    source_interface: ""
    wait_mode: WAIT_MODE_POLL
server: {server_config: {keep_alive_enabled: false}}
kms_client: {request_timeout: 0}
root_kms: {address: ""}
)",
            R"(
rdma:
  client_enabled: true
  server_enabled: true
  disk_agent_target_enabled: true
  blockstore_server_target_enabled: true
  client:
    aligned_data_enabled: true
    source_interface: ib0
    wait_mode: WAIT_MODE_BUSY_WAIT
server: {server_config: {keep_alive_enabled: true}}
kms_client: {request_timeout: 42}
root_kms: {address: kms}
)"};
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        const auto parse = [&](const TString& yaml)
        {
            const auto message = parser(yaml);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::TBlockstoreConfig::descriptor(),
                message->GetDescriptor());
            NProto::TBlockstoreConfig config;
            config.CopyFrom(*message);
            return config;
        };

        // Exercise both override directions and absent input against each base.
        for (const ui32 dynamicValue: {0, 1}) {
            auto staticConfig = parse(yamlConfigs[1 - dynamicValue]);
            staticConfig.MutableRdma()->MutableClient()->SetPollerThreads(7);
            for (const bool applyOverride: {false, true}) {
                const auto config = MakeStartupBlockstoreConfig(
                    staticConfig,
                    parse(applyOverride ? yamlConfigs[dynamicValue] : "{}"),
                    std::make_shared<NStorage::TStorageConfigControls>());
                const bool enabled =
                    applyOverride ? dynamicValue : 1 - dynamicValue;

                // Read the published wrappers, including proto3 fields that
                // already track presence, to protect their existing contract.
                const auto& rdma = config->GetRdmaConfig();
                UNIT_ASSERT_VALUES_EQUAL(enabled, rdma->GetClientEnabled());
                UNIT_ASSERT_VALUES_EQUAL(enabled, rdma->GetServerEnabled());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled,
                    rdma->GetDiskAgentTargetEnabled());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled,
                    rdma->GetBlockstoreServerTargetEnabled());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled,
                    config->GetServerConfig()->GetKeepAliveEnabled());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? 42 : 0,
                    config->GetKmsClientConfig()->GetRequestTimeout());
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? "kms" : "",
                    config->GetRootKmsConfig()->GetAddress());

                // Preserve the selected RDMA values when creating its working
                // configuration and retain the omitted nested parameter.
                const auto client = NCloud::NStorage::NRdma::CreateClientConfig(
                    rdma->GetClient());
                UNIT_ASSERT_VALUES_EQUAL(enabled, client.AlignedDataEnabled);
                UNIT_ASSERT_VALUES_EQUAL(
                    enabled ? "ib0" : "",
                    client.SourceInterface);
                UNIT_ASSERT(
                    client.WaitMode ==
                    (enabled ? NCloud::NStorage::NRdma::EWaitMode::BusyWait
                             : NCloud::NStorage::NRdma::EWaitMode::Poll));
                UNIT_ASSERT_VALUES_EQUAL(7, client.PollerThreads);
            }
        }
    }

    // Merge static and dynamic configs. Check that a dynamic scalar overrides
    // the static value, an omitted scalar stays unchanged, and Features with
    // different names are kept in source order.
    Y_UNIT_TEST(ShouldMergePresentFieldsAndAppendDistinctFeatures)
    {
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableStorageService()->SetWriteBlobThreshold(10);
        staticConfig.MutableStorageService()->SetFlushThreshold(20);
        auto* staticFeature = staticConfig.MutableFeatures()->AddFeatures();
        staticFeature->SetName("static");

        NProto::TBlockstoreConfig dynamicConfig;
        dynamicConfig.MutableStorageService()->SetWriteBlobThreshold(30);
        auto* dynamicFeature = dynamicConfig.MutableFeatures()->AddFeatures();
        dynamicFeature->SetName("dynamic");

        const auto result = MergeBlockstoreConfigSources(
            staticConfig,
            dynamicConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            30,
            result.GetStorageService().GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            result.GetStorageService().GetFlushThreshold());
        UNIT_ASSERT_VALUES_EQUAL(2, result.GetFeatures().FeaturesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "static",
            result.GetFeatures().GetFeatures(0).GetName());
        UNIT_ASSERT_VALUES_EQUAL(
            "dynamic",
            result.GetFeatures().GetFeatures(1).GetName());
    }

    // Merge Features with repeated names. Check that the first record in each
    // source is used, a dynamic record fully replaces the same-name static
    // record, and runtime lookups use only the resulting records.
    Y_UNIT_TEST(ShouldReplaceStaticFeaturesWithDynamicFeatures)
    {
        NProto::TBlockstoreConfig staticConfig;
        auto* staticA =
            AddFeature(staticConfig, "A", "static-A", "static-A-cloud");
        staticA->SetCloudProbability(1);
        AddFeature(staticConfig, "B", "static-B", "static-B-cloud");
        AddFeature(
            staticConfig,
            "B",
            "duplicate-static-B",
            "duplicate-static-B-cloud");

        NProto::TBlockstoreConfig dynamicConfig;
        AddFeature(dynamicConfig, "A", "dynamic-A", "dynamic-A-cloud");
        AddFeature(dynamicConfig, "C", "dynamic-C", "dynamic-C-cloud");
        AddFeature(
            dynamicConfig,
            "C",
            "duplicate-dynamic-C",
            "duplicate-dynamic-C-cloud");

        const auto result = MergeBlockstoreConfigSources(
            staticConfig,
            dynamicConfig);

        UNIT_ASSERT_VALUES_EQUAL(3, result.GetFeatures().FeaturesSize());

        const auto& mergedA = result.GetFeatures().GetFeatures(0);
        UNIT_ASSERT_VALUES_EQUAL("A", mergedA.GetName());
        UNIT_ASSERT_VALUES_EQUAL("dynamic-A", mergedA.GetValue());
        UNIT_ASSERT(!mergedA.HasCloudProbability());
        UNIT_ASSERT_VALUES_EQUAL(1, mergedA.GetWhitelist().CloudIdsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "dynamic-A-cloud",
            mergedA.GetWhitelist().GetCloudIds(0));

        const auto& mergedB = result.GetFeatures().GetFeatures(1);
        UNIT_ASSERT_VALUES_EQUAL("B", mergedB.GetName());
        UNIT_ASSERT_VALUES_EQUAL("static-B", mergedB.GetValue());

        const auto& mergedC = result.GetFeatures().GetFeatures(2);
        UNIT_ASSERT_VALUES_EQUAL("C", mergedC.GetName());
        UNIT_ASSERT_VALUES_EQUAL("dynamic-C", mergedC.GetValue());

        const auto config = MakeStartupBlockstoreConfig(
            staticConfig,
            dynamicConfig,
            std::make_shared<NStorage::TStorageConfigControls>());
        const auto& runtimeFeatures = *config->GetFeaturesConfig();

        UNIT_ASSERT_VALUES_EQUAL(
            result.GetFeatures().SerializeAsString(),
            runtimeFeatures.GetConfigProto().SerializeAsString());

        UNIT_ASSERT(
            runtimeFeatures.IsFeatureEnabled("dynamic-A-cloud", {}, {}, "A"));
        UNIT_ASSERT_VALUES_EQUAL(
            "dynamic-A",
            runtimeFeatures.GetFeatureValue("dynamic-A-cloud", {}, {}, "A"));
        UNIT_ASSERT(
            !runtimeFeatures.IsFeatureEnabled("static-A-cloud", {}, {}, "A"));
        UNIT_ASSERT(
            !runtimeFeatures.IsFeatureEnabled("other-cloud", {}, {}, "A"));

        UNIT_ASSERT(
            runtimeFeatures.IsFeatureEnabled("static-B-cloud", {}, {}, "B"));
        UNIT_ASSERT(
            !runtimeFeatures
                 .IsFeatureEnabled("duplicate-static-B-cloud", {}, {}, "B"));

        UNIT_ASSERT(
            runtimeFeatures.IsFeatureEnabled("dynamic-C-cloud", {}, {}, "C"));
        UNIT_ASSERT(
            !runtimeFeatures
                 .IsFeatureEnabled("duplicate-dynamic-C-cloud", {}, {}, "C"));
    }

    // Set the ICB value to 100 and rebuild the config with default 300. Check
    // that the override stays 100 and RestoreDefault switches the value to 300.
    Y_UNIT_TEST(ShouldKeepExplicitIcbOverrideAcrossConfigs)
    {
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        NKikimr::TControlBoard controlBoard;
        controls->Register(controlBoard);

        auto first = MakeStartupBlockstoreConfig(
            MakeConfig(100),
            MakeConfig(200),
            controls);

        UNIT_ASSERT_VALUES_EQUAL(
            200,
            first->GetStorageConfig()->GetWriteBlobThreshold());

        TAtomic previous = 0;
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            100,
            previous));
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            first->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            first->GetStorageConfig()
                ->GetEffectiveStorageConfigProto()
                .GetWriteBlobThreshold());

        auto second = MakeStartupBlockstoreConfig(
            MakeConfig(100),
            MakeConfig(300),
            controls);

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            second->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            second->GetStorageConfig()
                ->GetEffectiveStorageConfigProto()
                .GetWriteBlobThreshold());

        controlBoard.RestoreDefault("BlockStore_WriteBlobThreshold");
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            second->GetStorageConfig()->GetWriteBlobThreshold());

        auto third = MakeStartupBlockstoreConfig(
            MakeConfig(100),
            {},
            controls);

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            third->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Publish a config with default 300 while the ICB value is 150. Check that
    // the override stays 150 and RestoreDefault selects the new default 300.
    Y_UNIT_TEST(ShouldKeepIcbOverrideWhenCurrentConfigIsUpdated)
    {
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        NKikimr::TControlBoard controlBoard;
        controls->Register(controlBoard);

        TBlockstoreConfigHolder holder(MakeStartupBlockstoreConfig(
            MakeConfig(100),
            MakeConfig(200),
            controls));

        TAtomic previous = 0;
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            150,
            previous));

        holder.Set(MakeStartupBlockstoreConfig(
            MakeConfig(100),
            MakeConfig(300),
            controls));

        const auto currentConfig = holder.Get();
        UNIT_ASSERT_VALUES_EQUAL(
            150,
            currentConfig->GetStorageConfig()->GetWriteBlobThreshold());

        controlBoard.RestoreDefault("BlockStore_WriteBlobThreshold");
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            holder.Get()->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Supply Server and Diagnostics fields from both config layers. Check
    // that their typed runtime wrappers expose every merged value.
    Y_UNIT_TEST(ShouldExposeMergedTopLevelConfigs)
    {
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableServer()->MutableServerConfig()->SetPort(100);
        staticConfig.MutableDiagnostics()->SetNbsMonPort(200);

        NProto::TBlockstoreConfig dynamicConfig;
        dynamicConfig.MutableServer()->MutableServerConfig()->SetDataPort(300);
        dynamicConfig.MutableDiagnostics()->SetUseAsyncLogger(true);

        auto blockstoreConfig = MakeStartupBlockstoreConfig(
            staticConfig,
            dynamicConfig,
            std::make_shared<NStorage::TStorageConfigControls>());

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            blockstoreConfig->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            blockstoreConfig->GetServerConfig()->GetDataPort());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            blockstoreConfig->GetDiagnosticsConfig()->GetNbsMonPort());
        UNIT_ASSERT(
            blockstoreConfig->GetDiagnosticsConfig()->GetUseAsyncLogger());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            blockstoreConfig->GetDiskAgentConfig()->GetRack());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            blockstoreConfig->GetDiskAgentConfig()->GetNetworkMbitThroughput());
    }

    // Build from initialized Storage and DiskAgent adapters. Check that the
    // aggregate owns independent adapters, preserves DiskAgent host values,
    // and shares the registered Storage ICB controls.
    Y_UNIT_TEST(ShouldOwnBootstrapAdaptersAndShareTheirIcbControls)
    {
        auto config = MakeConfig(100);
        config.MutableFeatures()->AddFeatures()->SetName("config-feature");
        config.MutableDiskAgent()->SetAgentId("merged-agent");

        auto features = std::make_shared<NFeatures::TFeaturesConfig>();
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto storage = std::make_shared<NStorage::TStorageConfig>(
            config.GetStorageService(),
            features,
            controls);
        NKikimr::TControlBoard controlBoard;
        controls->Register(controlBoard);
        NProto::TDiskAgentConfig diskAgentProto;
        diskAgentProto.SetAgentId("bootstrap-agent");
        NStorage::TDiskAgentConfig diskAgent(
            std::move(diskAgentProto),
            "rack",
            10'000);

        auto blockstoreConfig = MakeStartupBlockstoreConfig(
            config,
            {},
            *storage,
            diskAgent);

        UNIT_ASSERT_UNEQUAL(
            features.get(),
            blockstoreConfig->GetFeaturesConfig().get());
        UNIT_ASSERT_UNEQUAL(
            storage.get(),
            blockstoreConfig->GetStorageConfig().get());
        UNIT_ASSERT_UNEQUAL(
            &diskAgent,
            blockstoreConfig->GetDiskAgentConfig().get());
        UNIT_ASSERT_VALUES_EQUAL(
            "config-feature",
            blockstoreConfig->GetFeaturesConfig()
                ->GetConfigProto()
                .GetFeatures(0)
                .GetName());
        UNIT_ASSERT_VALUES_EQUAL(
            "bootstrap-agent",
            blockstoreConfig->GetDiskAgentConfig()->GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(
            "rack",
            blockstoreConfig->GetDiskAgentConfig()->GetRack());
        UNIT_ASSERT_VALUES_EQUAL(
            10'000,
            blockstoreConfig->GetDiskAgentConfig()->GetNetworkMbitThroughput());

        TAtomic previous = 0;
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            200,
            previous));
        UNIT_ASSERT_VALUES_EQUAL(200, storage->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            blockstoreConfig->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Store an owning pointer to a top-level configuration section and release
    // the aggregate config. The section must remain alive and keep its value.
    Y_UNIT_TEST(ShouldKeepTopLevelSectionAliveAfterAggregateConfigIsReleased)
    {
        NServer::TServerAppConfigConstPtr serverConfig;
        {
            const auto config = MakeStartupBlockstoreConfig(
                MakeConfig(100),
                {},
                std::make_shared<NStorage::TStorageConfigControls>());
            serverConfig = config->GetServerConfig();
        }

        UNIT_ASSERT_VALUES_EQUAL(100, serverConfig->GetPort());
    }

    // Build DiskAgent from a dynamic proto plus rack and throughput parameters.
    // Check that both the proto value and host-only values reach the wrapper.
    Y_UNIT_TEST(ShouldPreserveDiskAgentHostContext)
    {
        const auto currentConfig = MakeStartupBlockstoreConfig(
            {},
            {},
            NStorage::TStorageConfig({}, nullptr),
            NStorage::TDiskAgentConfig({}, "rack-1", 42));
        NProto::TBlockstoreConfig dynamicConfig;
        dynamicConfig.MutableDiskAgent()->SetAgentId("updated-agent");

        const auto config = MakeStartupBlockstoreConfig(
            {},
            dynamicConfig,
            std::make_shared<NStorage::TStorageConfigControls>(),
            GetBlockstoreConfigExtraParameters(*currentConfig));

        UNIT_ASSERT_VALUES_EQUAL(
            "updated-agent",
            config->GetDiskAgentConfig()->GetConfigProto().GetAgentId());
        UNIT_ASSERT_VALUES_EQUAL(
            "rack-1",
            config->GetDiskAgentConfig()->GetRack());
        UNIT_ASSERT_VALUES_EQUAL(
            42,
            config->GetDiskAgentConfig()->GetNetworkMbitThroughput());
    }

    // Publish snapshots with matching Server and Storage values while four
    // threads read them. No reader may combine values from different snapshots,
    // and a retained old snapshot must remain unchanged.
    Y_UNIT_TEST(ShouldPublishConfigsAtomically)
    {
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto initial = MakeStartupBlockstoreConfig(
            MakeConfig(1),
            {},
            controls);
        auto retained = IBlockstoreConfigConstPtr(initial);
        auto holder =
            std::make_shared<TBlockstoreConfigHolder>(std::move(initial));
        const IBlockstoreConfigProviderPtr provider = holder;

        std::atomic<bool> stop = false;
        std::atomic<bool> consistent = true;
        TVector<std::thread> readers;

        for (ui32 i = 0; i != 4; ++i) {
            readers.emplace_back(
                [&]
                {
                    while (!stop.load()) {
                        const auto config = provider->Get();
                        if (config->GetServerConfig()->GetPort() !=
                            config->GetStorageConfig()->GetWriteBlobThreshold())
                        {
                            consistent.store(false);
                        }
                    }
                });
        }

        for (ui32 value = 2; value != 100; ++value) {
            holder->Set(MakeStartupBlockstoreConfig(
                MakeConfig(value),
                {},
                controls));
        }

        stop.store(true);
        for (auto& reader: readers) {
            reader.join();
        }

        UNIT_ASSERT(consistent.load());
        UNIT_ASSERT_VALUES_EQUAL(1, retained->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            retained->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            99,
            provider->Get()->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Check that local providers isolate publications and keep their holder
    // alive after the writer handle is released.
    Y_UNIT_TEST(ShouldKeepLocalProvidersIndependent)
    {
        // Create independent publication points without binding the global
        // provider or sharing controls.
        auto firstControls =
            std::make_shared<NStorage::TStorageConfigControls>();
        auto secondControls =
            std::make_shared<NStorage::TStorageConfigControls>();
        auto firstHolder = std::make_shared<TBlockstoreConfigHolder>(
            MakeStartupBlockstoreConfig(MakeConfig(100), {}, firstControls));
        auto secondHolder = std::make_shared<TBlockstoreConfigHolder>(
            MakeStartupBlockstoreConfig(MakeConfig(200), {}, secondControls));
        IBlockstoreConfigProviderPtr firstProvider = firstHolder;
        const IBlockstoreConfigProviderPtr secondProvider = secondHolder;
        const auto retained = firstProvider->Get();

        // Publish through one writer and preserve the other provider and the
        // retained snapshot.
        firstHolder->Set(MakeStartupBlockstoreConfig(
            MakeConfig(300),
            {},
            firstControls));
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            firstProvider->Get()->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            secondProvider->Get()->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(100, retained->GetServerConfig()->GetPort());

        // Release writer and provider handles separately to check both levels
        // of ownership.
        firstHolder.reset();
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            firstProvider->Get()->GetServerConfig()->GetPort());
        firstProvider.reset();
        UNIT_ASSERT_VALUES_EQUAL(100, retained->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            secondProvider->Get()->GetServerConfig()->GetPort());
    }

    // Initialize the process provider with value 100, then publish value 200
    // through its holder. The getter must return 200 while the retained initial
    // snapshot must still return 100.
    Y_UNIT_TEST(ShouldExposeCurrentBlockstoreConfig)
    {
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto holder = InitializeBlockstoreConfigProvider(
            MakeStartupBlockstoreConfig(MakeConfig(100), {}, controls));
        const IBlockstoreConfigProviderPtr provider = holder;
        Y_DEFER
        {
            ResetBlockstoreConfigProvider();
        };

        const auto initial = GetCurrentBlockstoreConfig();
        UNIT_ASSERT_EQUAL(initial.Get(), provider->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(100, initial->GetServerConfig()->GetPort());

        holder->Set(MakeStartupBlockstoreConfig(
            MakeConfig(200),
            {},
            controls));

        const auto current = GetCurrentBlockstoreConfig();
        UNIT_ASSERT_EQUAL(current.Get(), provider->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(200, current->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(100, initial->GetServerConfig()->GetPort());
    }

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
}

}   // namespace NCloud::NBlockStore
