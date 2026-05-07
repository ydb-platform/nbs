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

TOptionsPtr CreateOptions()
{
    auto options = std::make_shared<TOptions>();
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
    auto ci = TConfigInitializer(CreateOptions());
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

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsAndComponentsInStaticLogConfig)
    {
        auto logConfigStr = R"(
            Entry {
                Component: "BLOCKSTORE_DISK_AGENT"
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

        auto ci = TConfigInitializer(std::move(options));

        // - TConfigInitializerYdbBase
        UNIT_ASSERT_NO_EXCEPTION(ci.InitKikimrConfig());

        const auto& logConfig = ci.KikimrConfig->GetLogConfig();
        UNIT_ASSERT(logConfig.GetSysLog());
        UNIT_ASSERT(logConfig.GetIgnoreUnknownComponents());
        UNIT_ASSERT_VALUES_EQUAL(4, logConfig.GetDefaultLevel());
        UNIT_ASSERT_VALUES_EQUAL("NBS_SERVER", logConfig.GetSysLogService());
        UNIT_ASSERT_VALUES_EQUAL(2, logConfig.EntrySize());
        UNIT_ASSERT_VALUES_EQUAL(
            "BLOCKSTORE_DISK_AGENT",
            logConfig.GetEntry(0).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(0).GetLevel());
        UNIT_ASSERT_VALUES_EQUAL(
            "UNKNOWN_COMPONENT",
            logConfig.GetEntry(1).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(1).GetLevel());
    }

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInStaticConfigs)
    {
        auto componentConfigStr = R"(
            NoSuchField: "x"
        )";

        TTempDir dir;
        auto configPath = dir.Path() / "component.txt";

        TOFStream(configPath.GetPath()).Write(componentConfigStr);

        auto options = CreateOptions();

        // clang-format off
        // - TOptionsYdbBase
        options->LogConfig                =
        options->SysConfig                =
        options->DomainsConfig            =
        options->NameServiceConfig        =
        options->DynamicNameServiceConfig =
        options->AuthConfig               =
        options->KikimrFeaturesConfig     =
        options->SharedCacheConfig        =
        options->ImmediateControlsConfig  =
        options->InterconnectConfig       =
        options->MonitoringConfig         = configPath.GetPath();
        // - TOptionsBase
        options->DiagnosticsConfig       =
        options->ServerConfig            = configPath.GetPath();
        // - TOptions
        options->DiskAgentConfig         =
        options->DiskRegistryProxyConfig =
        options->FeaturesConfig          =
        options->RdmaConfig              =
        options->StorageConfig           = configPath.GetPath();
        // clang-format on

        auto ci = TConfigInitializer(std::move(options));

        UNIT_ASSERT_NO_EXCEPTION(ci.InitKikimrConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitServerConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitDiagnosticsConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitDiskAgentConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitDiskRegistryProxyConfig());
        // InitSpdkEnvConfig()          - not loaded from file
        UNIT_ASSERT_NO_EXCEPTION(ci.InitRdmaConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitFeaturesConfig());
        UNIT_ASSERT_NO_EXCEPTION(ci.InitStorageConfig());
    }

    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInCmsConfigs)
    {
        auto configStr = R"(
             NoSuchField: "x"
        )";

        // clang-format off
        // Elements ordered as in TConfigInitializer::ApplyNamedConfigs()
        const TVector<TString> configNames {
            "ActorSystemConfig",
            "AuthConfig",
            "DiagnosticsConfig",
            "DiskAgentConfig",
            "DiskRegistryProxyConfig",
            "FeaturesConfig",
            "InterconnectConfig",
            "LogConfig",
            "MonitoringConfig",
            "ServerAppConfig",
            "StorageServiceConfig",
        };
        // clang-format on

        auto ci = TConfigInitializer(CreateOptions());

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
        // Elements ordered as in TConfigInitializer::ApplyNamedConfigs()
        const TVector<TString> configNames {
            "ActorSystemConfig",
            "AuthConfig",
            "DiagnosticsConfig",
            "DiskAgentConfig",
            "DiskRegistryProxyConfig",
            "FeaturesConfig",
            "InterconnectConfig",
            "LogConfig",
            "MonitoringConfig",
            "ServerAppConfig",
            "StorageServiceConfig",
        };
        // clang-format on

        auto ci = TConfigInitializer(CreateOptions());

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
