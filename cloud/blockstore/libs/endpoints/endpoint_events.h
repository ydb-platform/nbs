#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointEventHandler
{
    virtual ~IEndpointEventHandler() = default;

    virtual void OnVolumeConnectionEstablished(const TString& diskId) = 0;
};

struct IEndpointEventProxy: public IEndpointEventHandler
{
    virtual ~IEndpointEventProxy() = default;

    virtual void Register(IEndpointEventHandlerPtr eventHandler) = 0;
};

IEndpointEventProxyPtr CreateEndpointEventProxy();

}   // namespace NCloud::NBlockStore::NServer
