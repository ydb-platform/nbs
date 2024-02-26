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
            "yc.disk-manager.folder"));
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
            "yc.disk-manager.folder"));
        UNIT_ASSERT_VALUES_EQUAL(true, ci.StorageConfig->GetMultipartitionVolumesEnabled());
    }

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInLogConfigAndNbsConfigs)
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

        auto nbsComponentConfigStr = R"(
            NoSuchField: "x"
        )";

        TTempDir dir;
        auto logConfigPath = dir.Path() / "nbs-log.txt";
        auto nbsComponentConfigPath = dir.Path() / "nbs-component.txt";

        TOFStream(logConfigPath.GetPath()).Write(logConfigStr);
        TOFStream(nbsComponentConfigPath.GetPath()).Write(nbsComponentConfigStr);

        auto options = CreateOptions();
        options->LogConfig = logConfigPath.GetPath();
        options->StatsUploadConfig = nbsComponentConfigPath.GetPath();
        options->DiscoveryConfig = nbsComponentConfigPath.GetPath();
        options->DiagnosticsConfig = nbsComponentConfigPath.GetPath();
        options->StorageConfig = nbsComponentConfigPath.GetPath();
        options->DiskRegistryProxyConfig = nbsComponentConfigPath.GetPath();
        options->DiskAgentConfig = nbsComponentConfigPath.GetPath();
        options->ServerConfig = nbsComponentConfigPath.GetPath();
        options->EndpointConfig = nbsComponentConfigPath.GetPath();
        options->FeaturesConfig = nbsComponentConfigPath.GetPath();
        options->LogbrokerConfig = nbsComponentConfigPath.GetPath();
        options->NotifyConfig = nbsComponentConfigPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitKikimrConfig();
        ci.InitStatsUploadConfig();
        ci.InitServerConfig();
        ci.InitDiscoveryConfig();
        ci.InitDiagnosticsConfig();
        ci.InitStorageConfig();
        ci.InitDiskRegistryProxyConfig();
        ci.InitDiskAgentConfig();
        ci.InitEndpointConfig();
        ci.InitFeaturesConfig();
        ci.InitLogbrokerConfig();
        ci.InitNotifyConfig();

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

    Y_UNIT_TEST(ShouldInitHostPerformanceProfile)
    {
        NClient::THostPerformanceProfile expected = {
            .CpuCount = 60,
            .NetworkMbitThroughput = 20'000,
        };

        TTempDir dir;

        auto throttlingConfigStr = Sprintf(R"(
            {"interfaces": [{"eth0": {"speed": "%d"}}], "compute_cores_num": %d}
            )",
            expected.NetworkMbitThroughput,
            expected.CpuCount);

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
    }

    Y_UNIT_TEST(ShouldInitHostPerformanceProfileWithInvalidThrottlingConfigFile)
    {
        TTempDir dir;
        auto throttlingConfigStr = R"(
            {"interfaces": [{"eth0": {"speed": "-1"}}], "compute_cores_num": -42}
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
}

}   // namespace NCloud::NBlockStore::NServer
