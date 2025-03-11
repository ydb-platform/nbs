#pragma once

#include "public.h"

#include <cloud/storage/core/libs/daemon/options.h>

#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsYdbBase
    : public virtual TOptionsBase
{
    TString LogConfig;
    TString SysConfig;
    TString DomainsConfig;
    TString NameServiceConfig;
    TString DynamicNameServiceConfig;
    TString AuthConfig;
    TString KikimrFeaturesConfig;
    TString SharedCacheConfig;

    TString InterconnectConfig;
    ui32 InterconnectPort = 0;

    TString MonitoringConfig;

    TString Domain;
    TString SchemeShardDir;
    TString NodeBrokerAddress;
    ui32 NodeBrokerPort = 0;
    ui32 NodeBrokerSslPort = 0;
    bool UseNodeBrokerSsl = false;
    TString LocationFile;

    TString RestartsCountFile;
    bool SuppressVersionCheck = false;

    bool LoadCmsConfigs = false;

    TOptionsYdbBase();
    virtual ~TOptionsYdbBase() = default;
};

}   // namespace NCloud::NStorage
