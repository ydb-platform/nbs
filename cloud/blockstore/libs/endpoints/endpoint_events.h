#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointEventHandler
{
    virtual ~IEndpointEventHandler() = default;

    virtual NThreading::TFuture<NProto::TError> SwitchEndpointIfNeeded(
        const TString& diskId,
        const TString& reason) = 0;
};

struct IEndpointEventProxy: public IEndpointEventHandler
{
    virtual ~IEndpointEventProxy() = default;

    virtual void Register(IEndpointEventHandlerPtr eventHandler) = 0;
};

IEndpointEventProxyPtr CreateEndpointEventProxy();

}   // namespace NCloud::NBlockStore::NServer
