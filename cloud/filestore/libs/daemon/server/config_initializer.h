#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>
#include <cloud/filestore/libs/daemon/common/config_initializer.h>
#include <cloud/filestore/libs/server/public.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

class TConfigInitializerServer
    : public TConfigInitializerCommon
{
public:
    TOptionsServerPtr Options;

    NProto::TServerAppConfig AppConfig;
    NServer::TServerConfigPtr ServerConfig;

    TConfigInitializerServer(TOptionsServerPtr options);

    void InitAppConfig();

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) override;

private:
    void ApplyServerAppConfig(const TString& text);
};

}   // namespace NCloud::NFileStore::NDaemon
