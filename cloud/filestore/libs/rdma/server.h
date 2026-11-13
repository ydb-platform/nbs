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
    TString ListenAddress = "::";
    ui32 ListenPort = 10021;
};

////////////////////////////////////////////////////////////////////////////////

IStartablePtr CreateFileStoreRdmaServiceTarget(
    TFileStoreRdmaServerConfigPtr config,
    ILoggingServicePtr logging,
    NCloud::NStorage::NRdma::IServerPtr server,
    IRdmaTabletRequestHandlerPtr requestHandler);

}   // namespace NCloud::NFileStore::NStorage
