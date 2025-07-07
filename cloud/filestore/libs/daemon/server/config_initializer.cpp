#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/server/config.h>

#include <cloud/storage/core/libs/common/proto_helpers.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerServer::TConfigInitializerServer(TOptionsServerPtr options)
    : TConfigInitializerCommon(
          {{"ServerAppConfig",
            bind_front(&TConfigInitializerServer::ApplyServerAppConfig, this)}},
          options)
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

void TConfigInitializerServer::ApplyServerAppConfig(const TString& text)
{
    AppConfig.Clear();
    ParseProtoTextFromStringRobust(text, AppConfig);

    ServerConfig = std::make_shared<NServer::TServerConfig>(
        AppConfig.GetServerConfig());
}

}   // namespace NCloud::NFileStore::NDaemon
