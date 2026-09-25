#include <cloud/blockstore/libs/config/blockstore_config.h>
#include <cloud/blockstore/libs/config/blockstore_config_holder.h>
#include <cloud/blockstore/libs/config/blockstore_config_provider.h>
#include <cloud/blockstore/libs/config/blockstore_config_provider_private.h>
#include <cloud/blockstore/libs/config/opaque_config_parser.h>

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
        const auto parse = [&] (const TString& yaml) {
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
                const auto config = MakeBlockstoreConfig(
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

        const auto result = MergeBlockstoreConfig(staticConfig, dynamicConfig);

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

        const auto result = MergeBlockstoreConfig(staticConfig, dynamicConfig);

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

        const auto config = MakeBlockstoreConfig(
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

        auto first =
            MakeBlockstoreConfig(MakeConfig(100), MakeConfig(200), controls);

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

        auto second =
            MakeBlockstoreConfig(MakeConfig(100), MakeConfig(300), controls);

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            second->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            second->GetStorageConfig()
                ->GetEffectiveStorageConfigProto()
                .GetWriteBlobThreshold());

        UNIT_ASSERT(controls->RestoreDefault("WriteBlobThreshold"));
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            second->GetStorageConfig()->GetWriteBlobThreshold());

        auto third = MakeBlockstoreConfig(MakeConfig(100), {}, controls);

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

        TBlockstoreConfigHolder holder(
            MakeBlockstoreConfig(MakeConfig(100), MakeConfig(200), controls));

        TAtomic previous = 0;
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            150,
            previous));

        holder.Set(
            MakeBlockstoreConfig(MakeConfig(100), MakeConfig(300), controls));

        const auto currentConfig = holder.Get();
        UNIT_ASSERT_VALUES_EQUAL(
            150,
            currentConfig->GetStorageConfig()->GetWriteBlobThreshold());

        UNIT_ASSERT(controls->RestoreDefault("WriteBlobThreshold"));
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

        auto blockstoreConfig = MakeBlockstoreConfig(
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
        storage->Register(controlBoard);
        NProto::TDiskAgentConfig diskAgentProto;
        diskAgentProto.SetAgentId("bootstrap-agent");
        NStorage::TDiskAgentConfig diskAgent(
            std::move(diskAgentProto),
            "rack",
            10'000);

        auto blockstoreConfig =
            MakeBlockstoreConfig(config, {}, *storage, diskAgent);

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
            const auto config = MakeBlockstoreConfig(
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
        const auto currentConfig = MakeBlockstoreConfig(
            {},
            {},
            NStorage::TStorageConfig({}, nullptr),
            NStorage::TDiskAgentConfig({}, "rack-1", 42));
        NProto::TBlockstoreConfig dynamicConfig;
        dynamicConfig.MutableDiskAgent()->SetAgentId("updated-agent");

        const auto config = MakeBlockstoreConfig(
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
        auto initial = MakeBlockstoreConfig(MakeConfig(1), {}, controls);
        auto retained = IBlockstoreConfigConstPtr(initial);
        TBlockstoreConfigHolder holder(std::move(initial));

        std::atomic<bool> stop = false;
        std::atomic<bool> consistent = true;
        TVector<std::thread> readers;

        for (ui32 i = 0; i != 4; ++i) {
            readers.emplace_back(
                [&]
                {
                    while (!stop.load()) {
                        const auto config = holder.Get();
                        if (config->GetServerConfig()->GetPort() !=
                            config->GetStorageConfig()->GetWriteBlobThreshold())
                        {
                            consistent.store(false);
                        }
                    }
                });
        }

        for (ui32 value = 2; value != 100; ++value) {
            holder.Set(MakeBlockstoreConfig(MakeConfig(value), {}, controls));
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
            holder.Get()->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Initialize the process provider with value 100, then publish value 200
    // through its holder. The getter must return 200 while the retained initial
    // snapshot must still return 100.
    Y_UNIT_TEST(ShouldExposeCurrentBlockstoreConfig)
    {
        auto controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto holder = InitializeBlockstoreConfigProvider(
            MakeBlockstoreConfig(MakeConfig(100), {}, controls));
        Y_DEFER
        {
            ResetBlockstoreConfigProvider();
        };

        const auto initial = GetCurrentBlockstoreConfig();
        UNIT_ASSERT_VALUES_EQUAL(100, initial->GetServerConfig()->GetPort());

        holder->Set(MakeBlockstoreConfig(MakeConfig(200), {}, controls));

        const auto current = GetCurrentBlockstoreConfig();
        UNIT_ASSERT_VALUES_EQUAL(200, current->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(100, initial->GetServerConfig()->GetPort());
    }
}

}   // namespace NCloud::NBlockStore
