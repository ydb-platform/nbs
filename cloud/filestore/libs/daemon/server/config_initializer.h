#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>
#include <cloud/filestore/libs/daemon/common/config_initializer.h>
#include <cloud/filestore/libs/server/public.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerServer
    : public TConfigInitializerCommon
{
    TOptionsServerPtr Options;

    NProto::TServerAppConfig AppConfig;
    NServer::TServerConfigPtr ServerConfig;

    TConfigInitializerServer(TOptionsServerPtr options);

    void InitAppConfig();

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) override;

    virtual NCloud::NStorage::TNodeRegistrationSettings
        GetNodeRegistrationSettings() override;
};

}   // namespace NCloud::NFileStore::NDaemon
