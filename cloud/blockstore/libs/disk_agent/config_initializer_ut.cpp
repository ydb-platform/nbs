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
#include <util/generic/size_literals.h>
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
}

}   // namespace NCloud::NBlockStore::NServer
