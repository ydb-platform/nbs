#pragma once

#include "public.h"

#include <cloud/blockstore/libs/daemon/common/options.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptions final: TOptionsBase
{
    TString SocketsDir;
    TString RootCertsFile;
    TString KeyFile;
    TString CertFile;

    TOptions();

    void Parse(int argc, char** argv) override;
};

}   // namespace NCloud::NBlockStore::NServer
