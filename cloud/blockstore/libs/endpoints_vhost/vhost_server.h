#pragma once

#include "public.h"

#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/vhost/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateVhostEndpointListener(NVhost::IServerPtr server);

}   // namespace NCloud::NBlockStore::NServer
