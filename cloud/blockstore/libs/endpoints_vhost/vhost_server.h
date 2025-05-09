#pragma once

#include "public.h"

#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/vhost/public.h>
#include <cloud/blockstore/config/server.pb.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateVhostEndpointListener(
    NVhost::IServerPtr server,
    const NProto::TChecksumFlags& checksumFlags,
    bool vhostDiscardEnabled);

}   // namespace NCloud::NBlockStore::NServer
