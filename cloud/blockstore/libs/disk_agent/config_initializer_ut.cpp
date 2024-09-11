#include "config_initializer.h"

#include "options.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/spdk/iface/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/libs/grpc/threadpool.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/protobuf/util/pb_io.h>

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

TOptionsPtr CreateOptions()
{
    auto options = std::make_shared<TOptions>();
    return options;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigInitializerTest)
{
    Y_UNIT_TEST(ShouldLoadStorageConfigFromCms)
    {
        auto ci = TConfigInitializer(CreateOptions());
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
        auto ci = TConfigInitializer(CreateOptions());
        ci.InitStorageConfig();

        {
            NCloud::NProto::TFeaturesConfig config;
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

        auto ci = TConfigInitializer(std::move(options));
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

        auto ci = TConfigInitializer(std::move(options));
        ci.InitServerConfig();
        ci.InitStorageConfig();

        const auto& proto = ci.StorageConfig->GetStorageConfigProto();

        UNIT_ASSERT_VALUES_EQUAL(100, proto.GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(200, proto.GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(300, proto.GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL("xyz", proto.GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("123", proto.GetNodeType());
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParamsWhebLoadingFromCms)
    {
        auto ci = TConfigInitializer(CreateOptions());
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
        auto ci = TConfigInitializer(CreateOptions());
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

    Y_UNIT_TEST(ShouldHandleNodeTypeInOptionsWithHighestPriority)
    {
        TTempDir dir;

        auto serverConfigPath = dir.Path() / "nbs-server.txt";

        auto serverConfigStr = R"(ServerConfig {
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
        options->NodeType = "agent-123";

        auto ci = TConfigInitializer(options);
        ci.InitServerConfig();
        ci.InitStorageConfig();

        UNIT_ASSERT_VALUES_EQUAL(
            "agent-123",
            ci.StorageConfig->GetStorageConfigProto().GetNodeType());

        NKikimrConfig::TAppConfig appCfg;
        auto* serverCfg = appCfg.MutableNamedConfigs()->Add();

        serverCfg->SetName("Cloud.NBS.ServerAppConfig");
        serverCfg->SetConfig(serverConfigStr);

        auto* storageCfg = appCfg.MutableNamedConfigs()->Add();
        storageCfg->SetName("Cloud.NBS.StorageServiceConfig");
        storageCfg->SetConfig(storageConfigStr);

        ci.ApplyCustomCMSConfigs(appCfg);

        UNIT_ASSERT_VALUES_EQUAL(
            "agent-123",
            ci.StorageConfig->GetStorageConfigProto().GetNodeType());
    }
}

}   // namespace NCloud::NBlockStore::NServer
