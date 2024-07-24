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
}

void TConfigInitializerServer::ApplyCustomCMSConfigs(
    const NKikimrConfig::TAppConfig& config)
{
    TConfigInitializerCommon::ApplyCustomCMSConfigs(config);

    using TSelf = TConfigInitializerServer;
    using TApplyFn = void (TSelf::*)(const TString&);

    const THashMap<TString, TApplyFn> map {
        { "ServerAppConfig",    &TSelf::ApplyServerAppConfig},
    };

    for (auto& item : config.GetNamedConfigs()) {
        TStringBuf name = item.GetName();
        if (!name.SkipPrefix("Cloud.NFS.")) {
            continue;
        }

        auto it = map.find(name);
        if (it != map.end()) {
            std::invoke(it->second, this, item.GetConfig());
        }
    }
}

void TConfigInitializerServer::ApplyServerAppConfig(const TString& text)
{
    AppConfig.Clear();
    ParseProtoTextFromStringRobust(text, AppConfig);

    ServerConfig = std::make_shared<NServer::TServerConfig>(
        AppConfig.GetServerConfig());
}

TNodeRegistrationSettings
    TConfigInitializerServer::GetNodeRegistrationSettings()
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
