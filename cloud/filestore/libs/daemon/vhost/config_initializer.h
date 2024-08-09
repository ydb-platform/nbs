#pragma once

#include "public.h"

#include <cloud/filestore/config/vhost.pb.h>
#include <cloud/filestore/libs/daemon/common/config_initializer.h>
#include <cloud/filestore/libs/endpoint_vhost/public.h>
#include <cloud/filestore/libs/server/config.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

class TConfigInitializerVhost final
    : public TConfigInitializerCommon
{
public:
    TOptionsVhostPtr Options;

    NProto::TVhostAppConfig AppConfig;
    NVhost::TVhostServiceConfigPtr VhostServiceConfig;
    NServer::TServerConfigPtr ServerConfig;

    TConfigInitializerVhost(TOptionsVhostPtr options);

    void InitAppConfig();

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) override;

private:
    void ApplyVHostAppConfig(const TString& text);
};

}   // namespace NCloud::NFileStore::NDaemon
