#include "config_initializer.h"
#include "options.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
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
    Y_UNIT_TEST(ShouldLoadStorageConfigFromCms)
    {
        auto ci = TConfigInitializerYdb(CreateOptions());
        ci.InitStorageConfig();

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
        ci.InitStorageConfig();

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
        auto logConfigStr = R"(
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
        auto logConfigPath = dir.Path() / "nbs-log.txt";

        TOFStream(logConfigPath.GetPath()).Write(logConfigStr);

        auto options = CreateOptions();

        // - TConfigInitializerYdbBase
        options->LogConfig = logConfigPath.GetPath();

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
        auto nbsComponentConfigStr = R"(
            NoSuchField: "x"
        )";

        TTempDir dir;
        auto nbsComponentConfigPath = dir.Path() / "nbs-component.txt";

        TOFStream(nbsComponentConfigPath.GetPath()).Write(nbsComponentConfigStr);

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
        options->CellsConfig             = nbsComponentConfigPath.GetPath();
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
        options->LocalNVMeConfig    = nbsComponentConfigPath.GetPath();
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

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInDynamicConfigs)
    {
        auto nbsComponentConfigStr = R"(
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

        // To detect mutual dependencies:
        //  - one at time
        //  - all at once, direct and reverse order

        // 1. One at time
        for (const auto& configName: configNames) {
            ci.KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();
            NKikimrConfig::TAppConfig singleAppCfg;
            auto& singleNamedConfigs = *singleAppCfg.MutableNamedConfigs();

            auto* singleNamedConfig = singleNamedConfigs.Add();
            singleNamedConfig->SetName("Cloud.NBS." + configName);
            singleNamedConfig->SetConfig(nbsComponentConfigStr);

            Cerr << Endl << "Apply NamedConfigs['"
                 << singleNamedConfig->GetName() << "'] = '"
                 << singleNamedConfig->GetConfig() << "'" << Endl;
            UNIT_ASSERT_NO_EXCEPTION(ci.ApplyCMSConfigs(singleAppCfg));
        }

        // 2. All at once, direct order
        ci.KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        NKikimrConfig::TAppConfig directFullAppCfg;
        auto& directFullNamedConfigs = *directFullAppCfg.MutableNamedConfigs();

        for (const auto& configName: configNames) {
            auto* directFullNamedConfig = directFullNamedConfigs.Add();
            directFullNamedConfig->SetName("Cloud.NBS." + configName);
            directFullNamedConfig->SetConfig(nbsComponentConfigStr);
        }

        Cerr << Endl << "Apply all NamedConfigs[] in direct order" << Endl;
        UNIT_ASSERT_NO_EXCEPTION(ci.ApplyCMSConfigs(directFullAppCfg));

        // 3. All at once, reverse order
        ci.KikimrConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        NKikimrConfig::TAppConfig reverseFullAppCfg;
        auto& reverseFullNamedConfigs =
            *reverseFullAppCfg.MutableNamedConfigs();

        for (auto configName = configNames.rbegin();
             configName != configNames.rend();
             configName++)
        {
            auto* reverseFullNamedConfig = reverseFullNamedConfigs.Add();
            reverseFullNamedConfig->SetName("Cloud.NBS." + *configName);
            reverseFullNamedConfig->SetConfig(nbsComponentConfigStr);
        }

        Cerr << Endl << "Apply all NamedConfigs[] in reverse order" << Endl;
        UNIT_ASSERT_NO_EXCEPTION(ci.ApplyCMSConfigs(reverseFullAppCfg));
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

        const auto& proto = ci.StorageConfig->GetStorageConfigProto();

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

        const auto& proto = ci.StorageConfig->GetStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("123", proto.GetNodeType());
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParamsWhenLoadingFromCms)
    {
        auto ci = TConfigInitializerYdb(CreateOptions());
        ci.InitStorageConfig();

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

        const auto& proto = ci.StorageConfig->GetStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("abc", proto.GetNodeType());
    }

    Y_UNIT_TEST(ShouldNotReplaceNodeRegistrationParamsInStorageConfigWithCms)
    {
        auto ci = TConfigInitializerYdb(CreateOptions());
        ci.InitStorageConfig();

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

        const auto& proto = ci.StorageConfig->GetStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("123", proto.GetNodeType());
    }
}

}   // namespace NCloud::NBlockStore::NServer
