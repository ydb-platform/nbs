#include "helper.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/string.h>
#include <util/stream/file.h>

namespace NCloud::NBlockStore::NPlugin {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultConfigHost = "testDefault";
const TString FallbackConfigHost = "testFallback";

const TString DefaultConfigFileName = "client.txt";
const TString FallbackConfigFileName = "client-default.txt";

////////////////////////////////////////////////////////////////////////////////

void PutFile(const TFsPath& path, const TString& text)
{
    TOFStream(path.GetPath()).Write(text);
}

TString GetDefaultConfig()
{
    NProto::TClientAppConfig clientAppConfig;
    auto& clientConfig = *clientAppConfig.MutableClientConfig();
    clientConfig.SetHost(DefaultConfigHost);
    return clientAppConfig.DebugString();
}

TString GetFallbackConfig()
{
    NProto::TClientAppConfig clientAppConfig;
    auto& clientConfig = *clientAppConfig.MutableClientConfig();
    clientConfig.SetHost(FallbackConfigHost);
    return clientAppConfig.DebugString();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TClientAppConfigParseTest)
{
    Y_UNIT_TEST(ShouldParseDefaultConfig)
    {
        TTempDir dir;
        auto defaultConfigPath = dir.Path() / DefaultConfigFileName;
        PutFile(defaultConfigPath, GetDefaultConfig());

        auto fallbackConfigPath = dir.Path() / FallbackConfigFileName;
        PutFile(fallbackConfigPath, GetFallbackConfig());

        NProto::TPluginConfig pluginConfig;
        NProto::TClientAppConfig appConfig = ParseClientAppConfig(
            pluginConfig,
            defaultConfigPath.GetPath(),
            fallbackConfigPath.GetPath());

        UNIT_ASSERT_VALUES_EQUAL(
            DefaultConfigHost,
            appConfig.GetClientConfig().GetHost());
    }

    Y_UNIT_TEST(ShouldParseDefaultConfigFromPluginConfig)
    {
        TTempDir dir;
        auto defaultConfigPath = dir.Path() / DefaultConfigFileName;
        PutFile(defaultConfigPath, GetDefaultConfig());

        NProto::TPluginConfig pluginConfig;
        pluginConfig.SetClientConfig(defaultConfigPath.GetPath());
        NProto::TClientAppConfig appConfig =
            ParseClientAppConfig(pluginConfig, "", "");

        UNIT_ASSERT_VALUES_EQUAL(
            DefaultConfigHost,
            appConfig.GetClientConfig().GetHost());
    }

    Y_UNIT_TEST(ShouldParseFallbackConfig)
    {
        TTempDir dir;
        auto fallbackConfigPath = dir.Path() / FallbackConfigFileName;
        PutFile(fallbackConfigPath, GetFallbackConfig());

        NProto::TPluginConfig pluginConfig;
        NProto::TClientAppConfig appConfig = ParseClientAppConfig(
            pluginConfig,
            DefaultConfigFileName,
            fallbackConfigPath.GetPath());

        UNIT_ASSERT_VALUES_EQUAL(
            FallbackConfigHost,
            appConfig.GetClientConfig().GetHost());
    }

    Y_UNIT_TEST(ShouldFailIfNoFile)
    {
        NProto::TPluginConfig pluginConfig;
        UNIT_ASSERT_EXCEPTION(
            ParseClientAppConfig(
                pluginConfig,
                DefaultConfigFileName,
                FallbackConfigFileName),
            yexception);
    }
}

}   // namespace NCloud::NBlockStore::NPlugin
