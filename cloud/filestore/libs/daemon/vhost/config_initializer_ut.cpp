#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/endpoint_vhost/config.h>
#include <cloud/filestore/libs/server/config.h>
#include <cloud/filestore/libs/storage/core/config.h>

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

TOptionsVhostPtr CreateOptions()
{
    auto options = std::make_shared<TOptionsVhost>();
    return options;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigInitializerTest)
{
    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInLogConfigAndNfsConfigs)
    {
        auto logConfigStr = R"(
            Entry {
                Component: "FILESTORE_VHOST"
                Level: 6
            }
            Entry {
                Component: "UNKNOWN_COMPONENT"
                Level: 6
            }
            SysLog: true
            DefaultLevel: 4
            UnknownField: "xxx"
            SysLogService: "NFS_VHOST"
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

        auto ci = TConfigInitializerVhost(std::move(options));
        ci.InitKikimrConfig();
        ci.InitDiagnosticsConfig();
        ci.InitStorageConfig();

        const auto& logConfig = ci.KikimrConfig->GetLogConfig();
        UNIT_ASSERT(logConfig.GetSysLog());
        UNIT_ASSERT(logConfig.GetIgnoreUnknownComponents());
        UNIT_ASSERT_VALUES_EQUAL(4, logConfig.GetDefaultLevel());
        UNIT_ASSERT_VALUES_EQUAL("NFS_VHOST", logConfig.GetSysLogService());
        UNIT_ASSERT_VALUES_EQUAL(2, logConfig.EntrySize());
        UNIT_ASSERT_VALUES_EQUAL(
            "FILESTORE_VHOST",
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
                VhostServiceConfig {
                    RootKeyringName: "xyz"
                }
            )";

            auto* diagCfg = appConfig.AddNamedConfigs();
            diagCfg->SetName("Cloud.Filestore.VHostAppConfig");
            diagCfg->SetConfig(std::move(configStr));
        }

        auto options = CreateOptions();

        auto ci = TConfigInitializerVhost(std::move(options));
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

        UNIT_ASSERT_VALUES_EQUAL(
            "xyz",
            ci.VhostServiceConfig->GetRootKeyringName());
    }
}

}   // namespace NCloud::NFileStore::NDaemon
