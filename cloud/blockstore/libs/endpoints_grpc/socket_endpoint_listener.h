#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/server/public.h>

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct ISocketEndpointListener
    : public IEndpointListener
    , public IStartable
{
    virtual void SetClientStorageFactory(IClientStorageFactoryPtr factory) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISocketEndpointListenerPtr CreateSocketEndpointListener(
    ILoggingServicePtr logging,
    ui32 socketBacklog,
    ui32 socketAccessMode);

}   // namespace NCloud::NBlockStore::NServer
