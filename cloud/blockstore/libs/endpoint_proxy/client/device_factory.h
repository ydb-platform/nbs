#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nbd/public.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TProxyDeviceConnectionFactoryConfig
{
    ui32 DefaultSectorSize = 0;
};

NBD::IDeviceConnectionFactoryPtr CreateProxyDeviceConnectionFactory(
    TProxyDeviceConnectionFactoryConfig config,
    IEndpointProxyClientPtr client);

}   // namespace NCloud::NBlockStore::NClient
