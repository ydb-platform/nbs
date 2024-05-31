#pragma once

#include "public.h"

#include <cloud/blockstore/libs/daemon/common/options.h>

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

    TOptions();

    void Parse(int argc, char** argv) override;
};

}   // namespace NCloud::NBlockStore::NServer
