#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TFileStoreRdmaServerConfig
{
    // Empty means the host's FQDN.
    TString ListenAddress;
    // Differs from the NBS disk agent RDMA port (10020): rdma_cm ports are
    // node-global and filestore often runs next to NBS.
    ui32 ListenPort = 10021;
};

////////////////////////////////////////////////////////////////////////////////

// Node-scoped RDMA target: decodes incoming tablet requests and hands
// (TabletId, MethodId, body) to requestHandler.
IStartablePtr CreateFileStoreRdmaServiceTarget(
    TFileStoreRdmaServerConfigPtr config,
    ILoggingServicePtr logging,
    NCloud::NStorage::NRdma::IServerPtr server,
    IRdmaTabletRequestHandlerPtr requestHandler);

}   // namespace NCloud::NFileStore::NStorage
