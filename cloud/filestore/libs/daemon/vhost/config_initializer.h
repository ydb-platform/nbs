#pragma once

#include "public.h"

#include <cloud/filestore/config/vhost.pb.h>
#include <cloud/filestore/libs/daemon/common/config_initializer.h>
#include <cloud/filestore/libs/endpoint_vhost/public.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerVhost final
    : public TConfigInitializerCommon
{
    TOptionsVhostPtr Options;

    NProto::TVhostAppConfig AppConfig;
    NServer::TServerConfigPtr ServerConfig;
    NVhost::TVhostServiceConfigPtr VhostServiceConfig;

    TConfigInitializerVhost(TOptionsVhostPtr options);

    void InitAppConfig();

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) override;

    virtual NCloud::NStorage::TNodeRegistrationSettings
        GetNodeRegistrationSettings() override;

    void ApplyVHostAppConfig(const TString& text);
};

}   // namespace NCloud::NFileStore::NDaemon
