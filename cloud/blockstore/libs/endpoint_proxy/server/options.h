#pragma once

#include "public.h"

#include <cloud/blockstore/libs/daemon/common/options.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptions final: TOptionsBase
{
    TString RootCertsFile;
    TString KeyFile;
    TString CertFile;
    TString UnixSocketPath;
    bool Netlink = false;
    TString StoredEndpointsPath;
    TDuration NbdRequestTimeout = TDuration::Minutes(10);

    TOptions();

    void Parse(int argc, char** argv) override;
};

}   // namespace NCloud::NBlockStore::NServer
