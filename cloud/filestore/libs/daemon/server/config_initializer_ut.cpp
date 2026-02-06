#include "config_initializer.h"

#include "options.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/server/config.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <contrib/ydb/core/protos/feature_flags.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/system/sanitizers.h>

namespace NCloud::NFileStore::NDaemon {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ParseProtoTextFromString(const TString& text, T& dst)
{
    TStringInput in(text);
    ParseFromTextFormat(in, dst);
}

TOptionsServerPtr CreateOptions()
{
    auto options = std::make_shared<TOptionsServer>();
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
    auto ci = TConfigInitializerServer(CreateOptions());
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
    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInLogConfigAndNfsConfigs)
    {
        auto logConfigStr = R"(
            Entry {
                Component: "FILESTORE_SERVER"
                Level: 6
            }
            Entry {
                Component: "UNKNOWN_COMPONENT"
                Level: 6
            }
            SysLog: true
            DefaultLevel: 4
            UnknownField: "xxx"
            SysLogService: "NFS_SERVER"
        )";

        auto nfsComponentConfigStr = R"(
            NoSuchField: "x"
        )";

        TTempDir dir;
        auto logConfigPath = dir.Path() / "nfs-log.txt";
        auto nbsComponentConfigPath = dir.Path() / "nfs-component.txt";

        TOFStream(logConfigPath.GetPath()).Write(logConfigStr);
        TOFStream(nbsComponentConfigPath.GetPath()).Write(nfsComponentConfigStr);

        auto options = CreateOptions();
        options->LogConfig = logConfigPath.GetPath();
        options->DiagnosticsConfig = nbsComponentConfigPath.GetPath();
        options->StorageConfig = nbsComponentConfigPath.GetPath();
        options->ServerConfig = nbsComponentConfigPath.GetPath();

        auto ci = TConfigInitializerServer(std::move(options));
        ci.InitKikimrConfig();
        ci.InitDiagnosticsConfig();
        ci.InitStorageConfig();

        const auto& logConfig = ci.KikimrConfig->GetLogConfig();
        UNIT_ASSERT(logConfig.GetSysLog());
        UNIT_ASSERT(logConfig.GetIgnoreUnknownComponents());
        UNIT_ASSERT_VALUES_EQUAL(4, logConfig.GetDefaultLevel());
        UNIT_ASSERT_VALUES_EQUAL("NFS_SERVER", logConfig.GetSysLogService());
        UNIT_ASSERT_VALUES_EQUAL(2, logConfig.EntrySize());
        UNIT_ASSERT_VALUES_EQUAL(
            "FILESTORE_SERVER",
            logConfig.GetEntry(0).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(0).GetLevel());
        UNIT_ASSERT_VALUES_EQUAL(
            "UNKNOWN_COMPONENT",
            logConfig.GetEntry(1).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(1).GetLevel());
    }

    Y_UNIT_TEST(ShouldLoadConfigsFromCmsAtStartup)
    {
        NKikimrConfig::TAppConfig appConfig;

        {
            auto configStr = R"(
                BastionNameSuffix: "xyz"
            )";

            auto* diagCfg = appConfig.AddNamedConfigs();
            diagCfg->SetName("Cloud.Filestore.DiagnosticsConfig");
            diagCfg->SetConfig(std::move(configStr));
        }

        {
            auto configStr = R"(
                NodeType: "xyz"
            )";

            auto* diagCfg = appConfig.AddNamedConfigs();
            diagCfg->SetName("Cloud.Filestore.StorageConfig");
            diagCfg->SetConfig(std::move(configStr));
        }

        {
            auto configStr = R"(
                Features {
                    Name: "xyz"
                    Value: "abc"
                    Whitelist {
                        EntityIds: "fs1"
                        EntityIds: "fs2"
                    }
                }
            )";

            auto* diagCfg = appConfig.AddNamedConfigs();
            diagCfg->SetName("Cloud.Filestore.FeaturesConfig");
            diagCfg->SetConfig(std::move(configStr));
        }

        {
            auto configStr = R"(
                ServerConfig {
                    Host: "abc"
                }
            )";

            auto* diagCfg = appConfig.AddNamedConfigs();
            diagCfg->SetName("Cloud.Filestore.ServerAppConfig");
            diagCfg->SetConfig(std::move(configStr));
        }

        auto options = CreateOptions();

        auto ci = TConfigInitializerServer(std::move(options));
        ci.ApplyCustomCMSConfigs(appConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            "xyz",
            ci.DiagnosticsConfig->GetBastionNameSuffix());
        UNIT_ASSERT_VALUES_EQUAL(
            "xyz",
            ci.StorageConfig->GetNodeType());

        const auto& features = ci.FeaturesConfig->CollectAllFeatures();
        UNIT_ASSERT_VALUES_EQUAL(1, features.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "abc",
            ci.FeaturesConfig->GetFeatureValue("", "", "fs1", "xyz"));
        UNIT_ASSERT_VALUES_EQUAL(
            "abc",
            ci.FeaturesConfig->GetFeatureValue("", "", "fs2", "xyz"));

        UNIT_ASSERT_VALUES_EQUAL(
            "abc",
            ci.ServerConfig->GetHost());
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
}

}   // namespace NCloud::NFileStore::NDaemon
