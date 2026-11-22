#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Handler of the RDMA server endpoint that serves tablet requests: decodes the
// request envelope, passes the request to IRdmaTabletRequestHandler and
// encodes its response.
struct IRdmaTabletServerHandler
    : public NCloud::NStorage::NRdma::IServerHandler
{
    // The responses wait for the endpoint, so it must be set (empty if the
    // endpoint was not started) once StartEndpoint() returns or throws.
    virtual void SetEndpoint(
        std::weak_ptr<NCloud::NStorage::NRdma::IServerEndpoint> endpoint) = 0;
};

using IRdmaTabletServerHandlerPtr = std::shared_ptr<IRdmaTabletServerHandler>;

IRdmaTabletServerHandlerPtr CreateRdmaTabletServerHandler(
    ILoggingServicePtr logging,
    IRdmaTabletRequestHandlerPtr requestHandler);

}   // namespace NCloud::NFileStore::NStorage
