#pragma once

#include "public.h"

#include <cloud/storage/core/libs/daemon/options.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsCommon
    : public virtual TOptionsBase
{
public:
    ui32 DataServerPort = 0;
    TString UnixSocketPath;

    bool TemporaryServer = false;
    bool SkipDeviceLocalityValidation = false;

    TString DiscoveryConfig;
    TString DiskAgentConfig;
    TString DiskRegistryProxyConfig;
    TString EndpointConfig;
    TString RdmaConfig;
    TString CellsConfig;

    enum class EServiceKind {
        Null   /* "null"   */ ,
        Local  /* "local"  */ ,
        Ydb    /* "kikimr" */ ,
    };

    EServiceKind ServiceKind = EServiceKind::Null;

    TOptionsCommon();
};

}   // namespace NCloud::NBlockStore::NServer
