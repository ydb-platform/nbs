#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/endpoint_vhost/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NCloud::NFileStore::NVhost;

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerVhost::TConfigInitializerVhost(TOptionsVhostPtr options)
    : TConfigInitializerCommon(options)
    , Options(std::move(options))
{}

void TConfigInitializerVhost::InitAppConfig()
{
    if (Options->AppConfig) {
        ParseProtoTextFromFileRobust(Options->AppConfig, AppConfig);
    }

    auto& serverConfig = *AppConfig.MutableServerConfig();
    if (Options->ServerPort) {
        serverConfig.SetPort(Options->ServerPort);
    }

    if (Options->SecureServerPort) {
        serverConfig.SetSecurePort(Options->SecureServerPort);
    }

    ServerConfig = std::make_shared<NServer::TServerConfig>(serverConfig);

    VhostServiceConfig = std::make_shared<TVhostServiceConfig>(
        AppConfig.GetVhostServiceConfig());
}

void TConfigInitializerVhost::ApplyCustomCMSConfigs(
    const NKikimrConfig::TAppConfig& config)
{
    TConfigInitializerCommon::ApplyCustomCMSConfigs(config);

    using TSelf = TConfigInitializerVhost;

    const THashMap<TString, TApplyConfigFn> map {
        { "VHostAppConfig",    bind_front(&TSelf::ApplyVHostAppConfig, this)  },
    };

    ApplyConfigs(config, map);
}

void TConfigInitializerVhost::ApplyVHostAppConfig(const TString& text)
{
    AppConfig.Clear();
    ParseProtoTextFromStringRobust(text, AppConfig);

    ServerConfig = std::make_shared<NServer::TServerConfig>(
        AppConfig.GetServerConfig());

    VhostServiceConfig = std::make_shared<TVhostServiceConfig>(
        AppConfig.GetVhostServiceConfig());
}

}   // namespace NCloud::NFileStore::NDaemon
