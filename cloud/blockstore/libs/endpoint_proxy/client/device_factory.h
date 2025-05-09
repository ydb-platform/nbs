#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nbd/public.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TProxyDeviceFactoryConfig
{
    ui32 DefaultSectorSize = 0;
    ui64 MaxZeroBlocksSubRequestSize = 0;
};

NBD::IDeviceFactoryPtr CreateProxyDeviceFactory(
    TProxyDeviceFactoryConfig config,
    IEndpointProxyClientPtr client);

}   // namespace NCloud::NBlockStore::NClient
