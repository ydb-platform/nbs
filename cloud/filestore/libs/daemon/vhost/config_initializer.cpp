#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/endpoint_vhost/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NCloud::NFileStore::NVhost;
using namespace NCloud::NStorage;

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
    const NKikimrConfig::TAppConfig&)
{
    // nothing to do
}

}   // namespace NCloud::NFileStore::NDaemon
