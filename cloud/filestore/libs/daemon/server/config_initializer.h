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
    const TOptionsServerPtr Options;

    NProto::TServerAppConfig AppConfig;
    NServer::TServerConfigPtr ServerConfig;

    explicit TConfigInitializerServer(TOptionsServerPtr options);

    void InitAppConfig();

private:
    void ApplyServerAppConfig(const TString& text);
};

}   // namespace NCloud::NFileStore::NDaemon
