#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/config/blockstore_config.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/discovery/config.h>
#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/blockstore/libs/ydbstats/config.h>
#include <cloud/storage/core/config/features.pb.h>
#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/version/version.h>

#include <contrib/ydb/core/protos/blobstorage.pb.h>
#include <contrib/ydb/core/protos/feature_flags.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/system/sanitizers.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ParseProtoTextFromString(const TString& text, T& dst)
{
    TStringInput in(text);
    ParseFromTextFormat(in, dst);
}

TOptionsYdbPtr CreateOptions()
{
    auto options = std::make_shared<TOptionsYdb>();
    return options;
}

// Initialize static sections, including RDMA only when its file is provided.
void InitStaticConfigs(TConfigInitializerYdb& ci)
{
    // Initialize Server and Features before the sections that depend on them.
    ci.InitServerConfig();
    ci.InitEndpointConfig();
    ci.InitHostPerformanceProfile();
    ci.InitFeaturesConfig();
    ci.InitStorageConfig();
    ci.InitDiskRegistryProxyConfig();
    ci.InitDiagnosticsConfig();
    ci.InitStatsUploadConfig();
    ci.InitDiscoveryConfig();
    ci.InitSpdkEnvConfig();
    ci.InitLogbrokerConfig();
    ci.InitNotifyConfig();
    ci.InitIamClientConfig();
    ci.InitKmsClientConfig();
    ci.InitRootKmsConfig();
    ci.InitComputeClientConfig();
    ci.InitCellsConfig();
    ci.InitLocalNVMeConfig();
    ci.InitDiskAgentConfig();
    if (ci.Options->RdmaConfig) {
        ci.InitRdmaConfig();
    }
}

/**
 * *LoadKikimrFeaturesFromCms implementation
 */
void ShouldLoadKikimrFeatureFromCms(
    const std::string& featureName,
    bool cmsEmpty,
    bool valueEmpty,
    bool shouldLoad)
{
    auto ci = TConfigInitializerYdb(CreateOptions());
    ci.InitKikimrConfig();
    InitStaticConfigs(ci);

    NKikimrConfig::TAppConfig appCfg;
    auto* cmsFeatureFlags = appCfg.MutableFeatureFlags();
    auto* featureFlags = ci.KikimrConfig->MutableFeatureFlags();

    const auto* reflection = featureFlags->GetReflection();
    const auto* descriptor = featureFlags->GetDescriptor();
    const auto* field = descriptor->FindFieldByName(featureName.c_str());

    UNIT_ASSERT(field);
    UNIT_ASSERT(field->type() == google::protobuf::FieldDescriptor::TYPE_BOOL);

    if (valueEmpty) {
        reflection->ClearField(featureFlags, field);
    } else {
        // Use default value
        reflection->SetBool(
            featureFlags,
            field,
            reflection->GetBool(*featureFlags, field));
    }
    UNIT_ASSERT(reflection->HasField(*featureFlags, field) == !valueEmpty);

    auto oldValue = reflection->GetBool(*featureFlags, field);

    if (cmsEmpty) {
        reflection->ClearField(cmsFeatureFlags, field);
    } else {
        reflection->SetBool(cmsFeatureFlags, field, !oldValue);
    }

    ci.ApplyCustomCMSConfigs(appCfg);

    TStringStream testInfo;
    testInfo << "featureName: " << featureName << ", cmsEmpty = " << cmsEmpty
             << ", valueEmpty = " << valueEmpty << ", should = " << shouldLoad;
    auto&& comment = testInfo.Str();

    if (shouldLoad) {
        if (cmsEmpty) {
            if (valueEmpty) {
                UNIT_ASSERT_C(
                    !reflection->HasField(*featureFlags, field),
                    comment);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    reflection->GetBool(*featureFlags, field),
                    oldValue,
                    comment);
            }
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(
                reflection->GetBool(*featureFlags, field),
                reflection->GetBool(*cmsFeatureFlags, field),
                comment);
        }
    } else {
        if (valueEmpty) {
            UNIT_ASSERT_C(!reflection->HasField(*featureFlags, field), comment);
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(
                reflection->GetBool(*featureFlags, field),
                oldValue,
                comment);
        }
    }
}

void ShouldLoadKikimrFeaturesFromCms(
    const std::vector<std::string>& featureNames,
    bool shouldLoad)
{
    for (auto&& featureName: featureNames) {
        // CmsEmpty -> Empty = Empty
        // CmsEmpty -> Value = Value
        // CmsValue -> Empty = shouldLoad ? CmsValue : Empty
        // CmsValue -> Value = shouldLoad ? CmsValue : Value

        for (bool cmsEmpty: {true, false}) {
            for (bool valueEmpty: {true, false}) {
                ShouldLoadKikimrFeatureFromCms(
                    featureName,
                    cmsEmpty,
                    valueEmpty,
                    shouldLoad);
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigInitializerTest)
{
    // Verify that DynamicYamlConfigurationEnabled is false by default and
    // can be set to true.
    Y_UNIT_TEST(ShouldExposeDynamicYamlConfigurationFlag)
    {
        TServerAppConfig defaultConfig;
        UNIT_ASSERT(!defaultConfig.GetDynamicYamlConfigurationEnabled());

        NProto::TServerAppConfig proto;
        proto.MutableServerConfig()->SetDynamicYamlConfigurationEnabled(true);
        TServerAppConfig enabledConfig(proto);
        UNIT_ASSERT(enabledConfig.GetDynamicYamlConfigurationEnabled());
    }

    // Verify that CMS changes the current ServerConfig without changing the
    // saved static snapshot or the ICB controls selected before CMS.
    Y_UNIT_TEST(ShouldKeepStaticSnapshotAndSharedControls)
    {
        for (const bool staticValue: {false, true}) {
            // Initialize with each static DynamicYamlConfigurationEnabled
            // value.
            TTempDir dir;
            auto configPath = dir.Path() / "server.txt";
            TOFStream(configPath.GetPath()).Write(TStringBuilder()
                << "ServerConfig { DynamicYamlConfigurationEnabled: "
                << (staticValue ? "true" : "false") << " }");

            auto options = CreateOptions();
            options->ServerConfig = configPath.GetPath();
            auto ci = TConfigInitializerYdb(std::move(options));

            InitStaticConfigs(ci);
            const auto staticConfig = ci.GetCurrentBlockstoreConfig();
            const auto staticText = staticConfig.SerializeAsString();
            auto* controls = ci.StorageConfigControls.get();

            UNIT_ASSERT_VALUES_EQUAL(
                staticValue,
                ci.GetDynamicYamlConfigurationEnabled());
            UNIT_ASSERT_VALUES_EQUAL(staticValue, controls != nullptr);

            // Apply CMS with the opposite DynamicYamlConfigurationEnabled
            // value.
            ci.ApplyServerAppConfig(TStringBuilder()
                << "ServerConfig { DynamicYamlConfigurationEnabled: "
                << (!staticValue ? "true" : "false") << " }");

            // Accept the CMS value in ServerConfig while preserving the
            // original snapshot and controls used by StorageConfig.
            UNIT_ASSERT_VALUES_EQUAL(
                !staticValue,
                ci.GetDynamicYamlConfigurationEnabled());
            UNIT_ASSERT_EQUAL(controls, ci.StorageConfigControls.get());
            UNIT_ASSERT_EQUAL(
                controls,
                ci.StorageConfig->GetStorageConfigControls().get());
            UNIT_ASSERT_VALUES_EQUAL(
                staticValue,
                staticConfig.GetServer()
                    .GetServerConfig()
                    .GetDynamicYamlConfigurationEnabled());

            // Omit the flag in another update. Its default must not change
            // the saved static configuration or the selected controls.
            ci.ApplyServerAppConfig("ServerConfig {}");
            UNIT_ASSERT(!ci.GetDynamicYamlConfigurationEnabled());
            UNIT_ASSERT_EQUAL(controls, ci.StorageConfigControls.get());
            UNIT_ASSERT_EQUAL(
                controls,
                ci.StorageConfig->GetStorageConfigControls().get());
            UNIT_ASSERT_VALUES_EQUAL(
                staticText,
                staticConfig.SerializeAsString());
        }
    }

    // Verify that only PROTO configuration applies NamedConfigs and allowed
    // feature flags, while both modes apply the same direct config sections.
    Y_UNIT_TEST(ShouldApplyNamedConfigsOnlyWhenDynamicYamlIsDisabled)
    {
        for (const bool useYamlConfig: {false, true}) {
            // Set DynamicYamlConfigurationEnabled locally and give CMS
            // different values to check which source each mode applies.
            TTempDir dir;
            const auto serverPath = dir.Path() / "server.txt";
            TOFStream(serverPath.GetPath()).Write(TStringBuilder()
                << "ServerConfig { DynamicYamlConfigurationEnabled: "
                << (useYamlConfig ? "true" : "false") << " }");
            auto options = CreateOptions();
            options->ServerConfig = serverPath.GetPath();
            options->MonitoringPort = 1234;
            auto ci = TConfigInitializerYdb(std::move(options));
            ci.InitKikimrConfig();
            InitStaticConfigs(ci);
            ci.KikimrConfig->MutableLogConfig()->SetDefaultLevel(3);
            ci.KikimrConfig->MutableFeatureFlags()
                ->SetEnableNodeBrokerDeltaProtocol(true);
            const auto localThreshold =
                ci.StorageConfig->GetWriteBlobThreshold();

            // Supply supported sections and conflicting direct/named values.
            NKikimrConfig::TAppConfig cms;
            ParseProtoTextFromString(R"(
                BlobStorageConfig {
                    ServiceSet { AvailabilityDomains: 42 }
                }
                DomainsConfig { Domain { Name: "cms" } }
                NameserviceConfig { ClusterUUID: "cms" }
                DynamicNameserviceConfig { MaxStaticNodeId: 456 }
                LogConfig { DefaultLevel: 9 }
                MonitoringConfig { MonitoringPort: 4321 }
                InterconnectConfig { StartTcp: false }
                FeatureFlags {
                    EnableNodeBrokerDeltaProtocol: false
                    EnableVPatch: false
                }
                BlockstoreConfig {
                    VolumePreemptionType: PREEMPTION_MOVE_LEAST_HEAVY
                }
                NamedConfigs {
                    Name: "Cloud.NBS.LogConfig"
                    Config: "DefaultLevel: 7"
                }
                NamedConfigs {
                    Name: "Cloud.NBS.StorageServiceConfig"
                    Config: "WriteBlobThreshold: 42"
                }
            )", cms);

            // Apply CMS through the common entry point in both modes.
            ci.ApplyCMSConfigs(cms);

            // Apply BlobStorageConfig, DomainsConfig, NameserviceConfig and
            // DynamicNameserviceConfig in both configuration modes.
            const auto& config = *ci.KikimrConfig;
            UNIT_ASSERT_VALUES_EQUAL(42, config.GetBlobStorageConfig()
                .GetServiceSet().GetAvailabilityDomains(0));
            UNIT_ASSERT_VALUES_EQUAL(
                "cms", config.GetDomainsConfig().GetDomain(0).GetName());
            UNIT_ASSERT_VALUES_EQUAL(
                "cms", config.GetNameserviceConfig().GetClusterUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                456, config.GetDynamicNameserviceConfig().GetMaxStaticNodeId());

            // Keep local LogConfig, StorageConfig and feature flags in YAML
            // mode. Preserve CLI monitoring and interconnect in both modes.
            UNIT_ASSERT_VALUES_EQUAL(
                useYamlConfig ? 3 : 7, config.GetLogConfig().GetDefaultLevel());
            UNIT_ASSERT_VALUES_EQUAL(
                1234, config.GetMonitoringConfig().GetMonitoringPort());
            UNIT_ASSERT(config.GetInterconnectConfig().GetStartTcp());
            UNIT_ASSERT_VALUES_EQUAL(
                useYamlConfig ? localThreshold : 42,
                ci.StorageConfig->GetWriteBlobThreshold());
            UNIT_ASSERT_EQUAL(
                NProto::PREEMPTION_MOVE_LEAST_HEAVY,
                ci.StorageConfig->GetVolumePreemptionType());
            UNIT_ASSERT_VALUES_EQUAL(
                useYamlConfig,
                config.GetFeatureFlags().GetEnableNodeBrokerDeltaProtocol());
            UNIT_ASSERT(config.GetFeatureFlags().GetEnableVPatch());
        }
    }

    // Verify that the static configuration includes all sections when an RDMA
    // file is provided
    Y_UNIT_TEST(ShouldBuildAllBlockstoreConfigs)
    {
        TTempDir dir;
        const auto rdmaPath = dir.Path() / "rdma.txt";
        TOFStream(rdmaPath.GetPath()).Write("ClientEnabled: true");
        auto options = CreateOptions();
        options->RdmaConfig = rdmaPath.GetPath();
        auto ci = TConfigInitializerYdb(std::move(options));
        InitStaticConfigs(ci);

        const auto config = ci.GetCurrentBlockstoreConfig();
        TVector<const google::protobuf::FieldDescriptor*> fields;
        config.GetReflection()->ListFields(config, &fields);

        UNIT_ASSERT_VALUES_EQUAL(19, fields.size());
    }

    // Verify that the Server flag selects shared controls before other sections
    // are initialized.
    Y_UNIT_TEST(ShouldInitializeSharedControlsFromServerConfig)
    {
        // Initialize ServerConfig with dynamic YAML enabled.
        auto ci = TConfigInitializerYdb(CreateOptions());
        NProto::TServerAppConfig server;
        server.MutableServerConfig()->SetDynamicYamlConfigurationEnabled(true);
        ci.ServerConfig = std::make_shared<TServerAppConfig>(server);

        // Create shared ICB controls before initializing the remaining
        // sections.
        ci.InitFeaturesConfig();
        ci.InitStorageConfig();

        UNIT_ASSERT(ci.GetDynamicYamlConfigurationEnabled());
        UNIT_ASSERT(ci.StorageConfigControls);
    }

    // Verify that CMS-derived RDMA settings appear only in the current
    // configuration and previously returned snapshots remain unchanged.
    Y_UNIT_TEST(ShouldBuildIndependentBlockstoreConfigSnapshots)
    {
        // Save the initialized configuration before applying CMS.
        auto ci = TConfigInitializerYdb(CreateOptions());
        InitStaticConfigs(ci);
        const auto staticConfig = ci.GetCurrentBlockstoreConfig();
        const auto staticText = staticConfig.SerializeAsString();
        UNIT_ASSERT(!staticConfig.HasRdma());
        UNIT_ASSERT(!ci.RdmaConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            staticText,
            ci.GetCurrentBlockstoreConfig().SerializeAsString());

        // Apply CMS ports, RDMA settings and the ServerConfig YAML flag.
        NKikimrConfig::TAppConfig cms;
        auto* server = cms.AddNamedConfigs();
        server->SetName("Cloud.NBS.ServerAppConfig");
        server->SetConfig(R"(ServerConfig {
            Port: 12345
            DynamicYamlConfigurationEnabled: true
            RdmaClientEnabled: true
        })");
        auto* diagnostics = cms.AddNamedConfigs();
        diagnostics->SetName("Cloud.NBS.DiagnosticsConfig");
        diagnostics->SetConfig("NbsMonPort: 23456");
        ci.ApplyCustomCMSConfigs(cms);
        ci.InitRdmaConfig();

        // Check that both the current proto and the startup configuration
        // contain the CMS port values.
        auto current = ci.GetCurrentBlockstoreConfig();
        UNIT_ASSERT_VALUES_EQUAL(
            12345,
            current.GetServer().GetServerConfig().GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            23456,
            current.GetDiagnostics().GetNbsMonPort());
        UNIT_ASSERT(current.GetRdma().GetClientEnabled());
        UNIT_ASSERT(
            current.GetServer().GetServerConfig()
                .GetDynamicYamlConfigurationEnabled());
        const auto aggregate = MakeBlockstoreConfig(
            current,
            {},
            *ci.StorageConfig,
            *ci.DiskAgentConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            12345,
            aggregate->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            23456,
            aggregate->GetDiagnosticsConfig()->GetNbsMonPort());

        // Change the returned protobuf message. Check that the initializer
        // still returns the CMS values and the initial snapshot remains
        // unchanged.
        current.MutableDiagnostics()->SetNbsMonPort(34567);
        UNIT_ASSERT_VALUES_EQUAL(
            23456,
            ci.GetCurrentBlockstoreConfig().GetDiagnostics().GetNbsMonPort());
        UNIT_ASSERT_VALUES_EQUAL(staticText, staticConfig.SerializeAsString());
    }

    Y_UNIT_TEST(ShouldLoadStorageConfigFromCms)
    {
        auto ci = TConfigInitializerYdb(CreateOptions());
        InitStaticConfigs(ci);

        NKikimrConfig::TAppConfig appCfg;
        auto& featuresCfg = *appCfg.MutableNamedConfigs();

        {
            auto* featureCfg = featuresCfg.Add();
            featureCfg->SetName("Cloud.NBS.FeaturesConfig");
            auto configStr = R"(Features { Name: "Balancer" Whitelist { CloudIds: "yc.disk-manager.cloud" }})";
            featureCfg->SetConfig(configStr);
        }

        {
            auto* featureCfg = featuresCfg.Add();
            featureCfg->SetName("Cloud.NBS.StorageServiceConfig");
            auto configStr = R"(MultipartitionVolumesEnabled: true)";
            featureCfg->SetConfig(configStr);
        }

        ci.ApplyCustomCMSConfigs(appCfg);
        UNIT_ASSERT_VALUES_EQUAL(true, !!ci.StorageConfig);
        UNIT_ASSERT_VALUES_EQUAL(true, ci.StorageConfig->IsBalancerFeatureEnabled(
            "yc.disk-manager.cloud",
            "yc.disk-manager.folder",
            ""));
        UNIT_ASSERT_VALUES_EQUAL(true, ci.StorageConfig->GetMultipartitionVolumesEnabled());
    }

    Y_UNIT_TEST(ShouldUpdateStorageConfigWithFeaturesFromCms)
    {
        auto ci = TConfigInitializerYdb(CreateOptions());
        InitStaticConfigs(ci);

        {
            NProto::TFeaturesConfig config;
            ci.FeaturesConfig =
                std::make_shared<NFeatures::TFeaturesConfig>(config);

            auto storageConfigStr = R"(MultipartitionVolumesEnabled: true)";
            NProto::TStorageServiceConfig storageConfig;
            ParseProtoTextFromString(storageConfigStr, storageConfig);

            ci.StorageConfig = std::make_shared<NStorage::TStorageConfig>(
                storageConfig,
                ci.FeaturesConfig);
        }

        NKikimrConfig::TAppConfig appCfg;
        auto& featuresCfg = *appCfg.MutableNamedConfigs();

        {
            auto* featureCfg = featuresCfg.Add();
            featureCfg->SetName("Cloud.NBS.FeaturesConfig");
            auto configStr = R"(Features { Name: "Balancer" Whitelist { CloudIds: "yc.disk-manager.cloud" }})";
            featureCfg->SetConfig(configStr);
        }

        ci.ApplyCustomCMSConfigs(appCfg);
        UNIT_ASSERT_VALUES_EQUAL(true, !!ci.StorageConfig);
        UNIT_ASSERT_VALUES_EQUAL(true, ci.StorageConfig->IsBalancerFeatureEnabled(
            "yc.disk-manager.cloud",
            "yc.disk-manager.folder",
            ""));
        UNIT_ASSERT_VALUES_EQUAL(true, ci.StorageConfig->GetMultipartitionVolumesEnabled());
    }

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsAndComponentsInStaticLogConfig)
    {
        auto configStr = R"(
            Entry {
                Component: "BLOCKSTORE_SERVER"
                Level: 6
            }
            Entry {
                Component: "UNKNOWN_COMPONENT"
                Level: 6
            }
            SysLog: true
            DefaultLevel: 4
            UnknownField: "xxx"
            SysLogService: "NBS_SERVER"
        )";

        TTempDir dir;
        auto configPath = dir.Path() / "component.txt";

        TOFStream(configPath.GetPath()).Write(configStr);

        auto options = CreateOptions();

        // - TConfigInitializerYdbBase
        options->LogConfig = configPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));

        // - TConfigInitializerYdbBase
        UNIT_ASSERT_NO_EXCEPTION(ci.InitKikimrConfig());

        const auto& logConfig = ci.KikimrConfig->GetLogConfig();
        UNIT_ASSERT(logConfig.GetSysLog());
        UNIT_ASSERT(logConfig.GetIgnoreUnknownComponents());
        UNIT_ASSERT_VALUES_EQUAL(4, logConfig.GetDefaultLevel());
        UNIT_ASSERT_VALUES_EQUAL("NBS_SERVER", logConfig.GetSysLogService());
        UNIT_ASSERT_VALUES_EQUAL(2, logConfig.EntrySize());
        UNIT_ASSERT_VALUES_EQUAL(
            "BLOCKSTORE_SERVER",
            logConfig.GetEntry(0).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(0).GetLevel());
        UNIT_ASSERT_VALUES_EQUAL(
            "UNKNOWN_COMPONENT",
            logConfig.GetEntry(1).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(1).GetLevel());
    }

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInStaticNbsConfigs)
    {
        auto configStr = R"(
            NoSuchField: "x"
        )";

        TTempDir dir;
        auto configPath = dir.Path() / "component.txt";

        TOFStream(configPath.GetPath()).Write(configStr);

        auto options = CreateOptions();

        // clang-format off
        // - TConfigInitializerCommon: TOptionsBase, TOptionsCommon
        options->DiagnosticsConfig       =
        options->DiscoveryConfig         =
        options->DiskAgentConfig         =
        options->DiskRegistryProxyConfig =
        options->EndpointConfig          =
        options->ServerConfig            =
        options->RdmaConfig              =
        options->CellsConfig             = configPath.GetPath();
        // - TConfigInitializerYdb: TOptionsYdb
        options->FeaturesConfig     =
        options->LogbrokerConfig    =
        options->NotifyConfig       =
        options->StatsUploadConfig  =
        options->StorageConfig      =
        options->IamConfig          =
        options->KmsConfig          =
        options->RootKmsConfig      =
        options->ComputeConfig      =
        options->LocalNVMeConfig    = configPath.GetPath();
        // clang-format on

        auto ci = TConfigInitializerYdb(std::move(options));

        // - TConfigInitializerCommon
        UNIT_ASSERT_NO_EXCEPTION(ci.InitServerConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitDiagnosticsConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitDiscoveryConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitDiskAgentConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitDiskRegistryProxyConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitEndpointConfig());
        // InitHostPerformanceProfile() - not loaded from file
        // InitSpdkEnvConfig()          - not loaded from file
        UNIT_ASSERT_NO_EXCEPTION(ci.InitRdmaConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitCellsConfig());
        // - TConfigInitializerYdb
        UNIT_ASSERT_NO_EXCEPTION(ci.InitFeaturesConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitLogbrokerConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitNotifyConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitStatsUploadConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitStorageConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitIamClientConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitKmsClientConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitRootKmsConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitComputeClientConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitLocalNVMeConfig());
    }

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInCmsConfigs)
    {
        auto configStr = R"(
             NoSuchField: "x"
        )";

        // clang-format off
        // Elements ordered as in TConfigInitializerYdb::ApplyNamedConfigs()
        const TVector<TString> configNames {
            "ActorSystemConfig",
            "AuthConfig",
            "DiagnosticsConfig",
            "DiscoveryServiceConfig",
            "DiskAgentConfig",
            "DiskRegistryProxyConfig",
            "FeaturesConfig",
            "InterconnectConfig",
            "LogbrokerConfig",
            "LogConfig",
            "MonitoringConfig",
            "NotifyConfig",
            "ServerAppConfig",
            "SpdkEnvConfig",
            "StorageServiceConfig",
            "YdbStatsConfig",
            "IamClientConfig",
            "KmsClientConfig",
            "RootKmsConfig",
            "ComputeClientConfig",
            "LocalNVMeConfig",
        };
        // clang-format on

        auto ci = TConfigInitializerYdb(CreateOptions());
        InitStaticConfigs(ci);

        ci.KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        NKikimrConfig::TAppConfig appCfg;
        auto& directFullNamedConfigs = *appCfg.MutableNamedConfigs();

        for (const auto& configName: configNames) {
            auto* namedConfig = directFullNamedConfigs.Add();
            namedConfig->SetName("Cloud.NBS." + configName);
            namedConfig->SetConfig(configStr);
        }

        UNIT_ASSERT_NO_EXCEPTION(ci.ApplyCMSConfigs(appCfg));
    }

    Y_UNIT_TEST(ShouldApplyCmsConfigsInAnyOrder)
    {
        auto configStr = "";

        // clang-format off
        // Elements ordered as in TConfigInitializerYdb::ApplyNamedConfigs()
        const TVector<TString> configNames {
            "ActorSystemConfig",
            "AuthConfig",
            "DiagnosticsConfig",
            "DiscoveryServiceConfig",
            "DiskAgentConfig",
            "DiskRegistryProxyConfig",
            "FeaturesConfig",
            "InterconnectConfig",
            "LogbrokerConfig",
            "LogConfig",
            "MonitoringConfig",
            "NotifyConfig",
            "ServerAppConfig",
            "SpdkEnvConfig",
            "StorageServiceConfig",
            "YdbStatsConfig",
            "IamClientConfig",
            "KmsClientConfig",
            "RootKmsConfig",
            "ComputeClientConfig",
            "LocalNVMeConfig",
        };
        // clang-format on

        auto ci = TConfigInitializerYdb(CreateOptions());
        InitStaticConfigs(ci);

        // To detect possible mutual dependencies:
        //  - one at time
        //  - all at once, direct and reverse order

        // 1. One at time
        {
            for (const auto& configName: configNames) {
                ci.KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();
                NKikimrConfig::TAppConfig appCfg;
                auto& namedConfigs = *appCfg.MutableNamedConfigs();

                ::NKikimrConfig::TNamedConfig* namedConfig = nullptr;
                for (auto i = 0; i < 3; i++) {
                    // May be several NamedConfigs[] with same name
                    namedConfig = namedConfigs.Add();
                    namedConfig->SetName("Cloud.NBS." + configName);
                    namedConfig->SetConfig(configStr);
                }

                Cerr << Endl << "Apply NamedConfigs['" << namedConfig->GetName()
                     << "']" << Endl;
                UNIT_ASSERT_NO_EXCEPTION(ci.ApplyCMSConfigs(appCfg));
            }
        }

        // 2. All at once, direct order, single entities
        {
            ci.KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();
            NKikimrConfig::TAppConfig appCfg;
            auto& namedConfigs = *appCfg.MutableNamedConfigs();

            for (const auto& configName: configNames) {
                auto* namedConfig = namedConfigs.Add();
                namedConfig->SetName("Cloud.NBS." + configName);
                namedConfig->SetConfig(configStr);
            }

            Cerr << Endl << "Apply all NamedConfigs[] in direct order" << Endl;
            UNIT_ASSERT_NO_EXCEPTION(ci.ApplyCMSConfigs(appCfg));
        }

        // 3. All at once, reverse order, multiple entities
        {
            ci.KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();
            NKikimrConfig::TAppConfig appCfg;
            auto& namedConfigs = *appCfg.MutableNamedConfigs();

            for (auto i = 0; i < 3; i++) {
                // May be several NamedConfigs[] with same name
                for (auto configName = configNames.rbegin();
                     configName != configNames.rend();
                     configName++)
                {
                    auto* namedConfig = namedConfigs.Add();
                    namedConfig->SetName("Cloud.NBS." + *configName);
                    namedConfig->SetConfig(configStr);
                }
            }

            Cerr << Endl << "Apply all NamedConfigs[] in reverse order" << Endl;
            UNIT_ASSERT_NO_EXCEPTION(ci.ApplyCMSConfigs(appCfg));
        }
    }

    Y_UNIT_TEST(ShouldInitHostPerformanceProfile)
    {
        NClient::THostPerformanceProfile expected = {
            .CpuCount = 60,
            .NetworkMbitThroughput = 20'000,
            .IsTightServiceMemoryPlatform = true,
        };

        TTempDir dir;

        auto throttlingConfigStr = Sprintf(
            R"({
              "interfaces": [{"eth0": {"speed": "%d"}}],
              "compute_cores_num": %d,
              "is_tight_service_memory_platform": %s
            })",
            expected.NetworkMbitThroughput,
            expected.CpuCount,
            expected.IsTightServiceMemoryPlatform ? "true" : "false");

        auto throttlingConfigPath = dir.Path() / "nbs-throttling.txt";
        TOFStream(throttlingConfigPath.GetPath()).Write(throttlingConfigStr);

        auto clientConfigStr = Sprintf(R"(
            ClientConfig {
                ThrottlingConfig {
                    InfraThrottlingConfigPath: "%s"
                }
            })",
            throttlingConfigPath.GetPath().c_str());

        auto clientConfigPath = dir.Path() / "nbs-client.txt";
        TOFStream(clientConfigPath.GetPath()).Write(clientConfigStr);

        auto options = CreateOptions();
        options->EndpointConfig = clientConfigPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitEndpointConfig();
        ci.InitHostPerformanceProfile();

        auto& actual = ci.HostPerformanceProfile;
        UNIT_ASSERT_VALUES_EQUAL(
            expected.CpuCount, actual.CpuCount);
        UNIT_ASSERT_VALUES_EQUAL(
            expected.NetworkMbitThroughput, actual.NetworkMbitThroughput);
        UNIT_ASSERT_VALUES_EQUAL(
            expected.IsTightServiceMemoryPlatform,
            actual.IsTightServiceMemoryPlatform);
    }

    Y_UNIT_TEST(ShouldInitHostPerformanceProfileWithoutThrottlingConfigFile)
    {
        TTempDir dir;
        auto wrongPath = dir.Path() / "nbs-throttling.txt";
        auto clientConfigStr = Sprintf(R"(
            ClientConfig {
                ThrottlingConfig {
                    InfraThrottlingConfigPath: "%s"
                    DefaultHostCpuCount: 42
                    DefaultNetworkMbitThroughput: 325
                }
            })",
            wrongPath.GetPath().c_str());

        auto clientConfigPath = dir.Path() / "nbs-client.txt";
        TOFStream(clientConfigPath.GetPath()).Write(clientConfigStr);

        auto options = CreateOptions();
        options->EndpointConfig = clientConfigPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitEndpointConfig();
        ci.InitHostPerformanceProfile();

        auto& actual = ci.HostPerformanceProfile;
        UNIT_ASSERT_VALUES_EQUAL(42, actual.CpuCount);
        UNIT_ASSERT_VALUES_EQUAL(325, actual.NetworkMbitThroughput);
        UNIT_ASSERT_VALUES_EQUAL(false, actual.IsTightServiceMemoryPlatform);
    }

    Y_UNIT_TEST(ShouldInitHostPerformanceProfileWithInvalidThrottlingConfigFile)
    {
        TTempDir dir;
        auto throttlingConfigStr = R"(
            {"interfaces": [{"eth0": {"speed": "-1"}}], "compute_cores_num": -42, "is_tight_service_memory_platform": 11}
        )";

        auto throttlingConfigPath = dir.Path() / "nbs-throttling.txt";
        TOFStream(throttlingConfigPath.GetPath()).Write(throttlingConfigStr);

        auto clientConfigStr = Sprintf(R"(
            ClientConfig {
                ThrottlingConfig {
                    InfraThrottlingConfigPath: "%s"
                    DefaultHostCpuCount: 42
                    DefaultNetworkMbitThroughput: 325
                }
            })",
            throttlingConfigPath.GetPath().c_str());

        auto clientConfigPath = dir.Path() / "nbs-client.txt";
        TOFStream(clientConfigPath.GetPath()).Write(clientConfigStr);

        auto options = CreateOptions();
        options->EndpointConfig = clientConfigPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitEndpointConfig();
        ci.InitHostPerformanceProfile();

        auto& actual = ci.HostPerformanceProfile;
        UNIT_ASSERT_VALUES_EQUAL(42, actual.CpuCount);
        UNIT_ASSERT_VALUES_EQUAL(325, actual.NetworkMbitThroughput);
        UNIT_ASSERT_VALUES_EQUAL(false, actual.IsTightServiceMemoryPlatform);
    }

    Y_UNIT_TEST(ShouldInitKikimrFeatures)
    {
        TTempDir dir;
        auto configPath = dir.Path() / "kikimr-features.txt";

        {
            TOFStream(configPath.GetPath()).Write("");

            auto options = std::make_shared<TOptionsYdb>();
            options->KikimrFeaturesConfig = configPath.GetPath();

            auto ci = TConfigInitializerYdb(std::move(options));
            ci.InitKikimrConfig();

            UNIT_ASSERT(ci.KikimrConfig->GetFeatureFlags().GetEnableVPatch());
        }

        {
            auto configStr = R"(
                EnableVPatch: false
            )";

            TOFStream(configPath.GetPath()).Write(configStr);

            auto options = CreateOptions();
            options->KikimrFeaturesConfig = configPath.GetPath();

            auto ci = TConfigInitializerYdb(std::move(options));
            ci.InitKikimrConfig();

            UNIT_ASSERT(!ci.KikimrConfig->GetFeatureFlags().GetEnableVPatch());
        }

        {
            auto options = CreateOptions();

            auto ci = TConfigInitializerYdb(std::move(options));
            ci.InitKikimrConfig();

            UNIT_ASSERT(ci.KikimrConfig->GetFeatureFlags().GetEnableVPatch());
        }
    }

    Y_UNIT_TEST(ShouldLoadAllowedKikimrFeaturesFromCms)
    {
        std::vector<std::string> featureNames = {
            "EnableNodeBrokerDeltaProtocol"};

        ShouldLoadKikimrFeaturesFromCms(featureNames, true);
    }

    Y_UNIT_TEST(ShouldNotLoadUnallowedKikimrFeaturesFromCms)
    {
        std::vector<std::string> featureNames = {
            "EnableSchemeBoard",
            "EnableGracefulShutdown"};

        ShouldLoadKikimrFeaturesFromCms(featureNames, false);
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParams)
    {
        TTempDir dir;

        auto serverConfigPath = dir.Path() / "nbs-server.txt";

        auto serverConfigStr = R"(ServerConfig {
                NodeRegistrationMaxAttempts: 100
                NodeRegistrationTimeout: 200
                NodeRegistrationErrorTimeout: 300
                NodeRegistrationToken: "xyz"
                NodeType: "abc"
            }
        )";

        TOFStream(serverConfigPath.GetPath()).Write(serverConfigStr);

        auto storageConfigPath = dir.Path() / "nbs-storage.txt";

        TOFStream(storageConfigPath.GetPath()).Write("");

        auto options = CreateOptions();
        options->ServerConfig = serverConfigPath.GetPath();
        options->StorageConfig = storageConfigPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitServerConfig();
        ci.InitStorageConfig();

        const auto proto = ci.StorageConfig->GetEffectiveStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("abc", proto.GetNodeType());
    }

    Y_UNIT_TEST(ShouldNotReplaceNodeRegistrationParamsInStorageConfig)
    {
        TTempDir dir;

        auto serverConfigPath = dir.Path() / "nbs-server.txt";

        auto serverConfigStr = R"(ServerConfig {
                NodeRegistrationMaxAttempts: 100
                NodeRegistrationTimeout: 200
                NodeRegistrationErrorTimeout: 300
                NodeRegistrationToken: "xyz"
                NodeType: "abc"
            }
        )";

        TOFStream(serverConfigPath.GetPath()).Write(serverConfigStr);

        auto storageConfigPath = dir.Path() / "nbs-storage.txt";

        auto storageConfigStr = R"(
            NodeType: "123"
        )";

        TOFStream(storageConfigPath.GetPath()).Write(storageConfigStr);

        auto options = CreateOptions();
        options->ServerConfig = serverConfigPath.GetPath();
        options->StorageConfig = storageConfigPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitServerConfig();
        ci.InitStorageConfig();

        const auto proto = ci.StorageConfig->GetEffectiveStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("123", proto.GetNodeType());
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParamsWhenLoadingFromCms)
    {
        auto ci = TConfigInitializerYdb(CreateOptions());
        InitStaticConfigs(ci);

        NKikimrConfig::TAppConfig appCfg;
        auto* serverCfg = appCfg.MutableNamedConfigs()->Add();

        serverCfg->SetName("Cloud.NBS.ServerAppConfig");
        auto serverConfigStr = R"(ServerConfig {
                NodeRegistrationMaxAttempts: 100
                NodeRegistrationTimeout: 200
                NodeRegistrationErrorTimeout: 300
                NodeRegistrationToken: "xyz"
                NodeType: "abc"
            }
        )";
        serverCfg->SetConfig(serverConfigStr);

        auto* storageCfg = appCfg.MutableNamedConfigs()->Add();
        storageCfg->SetName("Cloud.NBS.StorageServiceConfig");
        storageCfg->SetConfig("");

        ci.ApplyCustomCMSConfigs(appCfg);

        const auto proto = ci.StorageConfig->GetEffectiveStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("abc", proto.GetNodeType());
    }

    Y_UNIT_TEST(ShouldNotReplaceNodeRegistrationParamsInStorageConfigWithCms)
    {
        auto ci = TConfigInitializerYdb(CreateOptions());
        InitStaticConfigs(ci);

        NKikimrConfig::TAppConfig appCfg;
        auto* serverCfg = appCfg.MutableNamedConfigs()->Add();

        serverCfg->SetName("Cloud.NBS.ServerAppConfig");
        auto serverConfigStr = R"(ServerConfig {
                NodeRegistrationMaxAttempts: 100
                NodeRegistrationTimeout: 200
                NodeRegistrationErrorTimeout: 300
                NodeRegistrationToken: "xyz"
                NodeType: "abc"
            }
        )";
        serverCfg->SetConfig(serverConfigStr);

        auto* storageCfg = appCfg.MutableNamedConfigs()->Add();
        storageCfg->SetName("Cloud.NBS.StorageServiceConfig");
        auto storageConfigStr = R"(
            NodeType: "123"
        )";
        storageCfg->SetConfig(storageConfigStr);

        ci.ApplyCustomCMSConfigs(appCfg);

        const auto proto = ci.StorageConfig->GetEffectiveStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("123", proto.GetNodeType());
    }
}

}   // namespace NCloud::NBlockStore::NServer
