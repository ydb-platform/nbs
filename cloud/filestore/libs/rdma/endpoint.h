#pragma once

#include "public.h"

#include <cloud/storage/core/libs/rdma/iface/public.h>

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Starts the endpoint of the RDMA server that serves tablet requests: it
// decodes the request envelope, passes the request to requestHandler and
// encodes its response. Throws if the endpoint cannot be started.
void StartRdmaTabletEndpoint(
    NCloud::NStorage::NRdma::IServer& server,
    const TString& host,
    ui32 port,
    TLog log,
    IRdmaTabletRequestHandlerPtr requestHandler);

}   // namespace NCloud::NFileStore::NStorage
