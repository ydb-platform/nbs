#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/server/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NFileStore::NDaemon {

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerServer::TConfigInitializerServer(TOptionsServerPtr options)
    : TConfigInitializerCommon(options)
    , Options(std::move(options))
{}

void TConfigInitializerServer::InitAppConfig()
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
}

void TConfigInitializerServer::ApplyCustomCMSConfigs(
    const NKikimrConfig::TAppConfig&)
{
    // nothing to do
}

}   // namespace NCloud::NFileStore::NDaemon
