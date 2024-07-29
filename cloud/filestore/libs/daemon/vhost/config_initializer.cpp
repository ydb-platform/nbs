#include "config_initializer.h"
#include "options.h"

#include <cloud/filestore/libs/endpoint_vhost/config.h>
#include <cloud/filestore/libs/server/config.h>

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
    if (ServerConfig) {
        return;
    }

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

void TConfigInitializerVhost::ApplyVHostAppConfig(const TString& text)
{
    AppConfig.Clear();
    ParseProtoTextFromStringRobust(text, AppConfig);

    ServerConfig = std::make_shared<NServer::TServerConfig>(
        AppConfig.GetServerConfig());

    VhostServiceConfig = std::make_shared<TVhostServiceConfig>(
        AppConfig.GetVhostServiceConfig());
}

TNodeRegistrationSettings
    TConfigInitializerVhost::GetNodeRegistrationSettings()
{
    InitAppConfig();

    TNodeRegistrationSettings settings;
    settings.MaxAttempts = Options->NodeRegistrationMaxAttempts;
    settings.RegistrationTimeout = Options->NodeRegistrationTimeout;
    settings.ErrorTimeout = Options->NodeRegistrationErrorTimeout;
    settings.PathToGrpcCaFile = ServerConfig->GetRootCertsFile();
    settings.PathToGrpcCertFile = GetCertFileFromConfig(ServerConfig);
    settings.PathToGrpcPrivateKeyFile = GetCertPrivateKeyFileFromConfig(ServerConfig);
    settings.NodeRegistrationToken = ServerConfig->GetNodeRegistrationToken();
    settings.NodeType = ServerConfig->GetNodeType();
    return settings;
}

}   // namespace NCloud::NFileStore::NDaemon
