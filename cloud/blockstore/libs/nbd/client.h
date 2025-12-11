#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <util/network/address.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IClient: public IStartable
{
    virtual IBlockStorePtr CreateEndpoint(
        const TNetworkAddress& connectAddress,
        IClientHandlerPtr handler,
        IBlockStorePtr volumeClient) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(ILoggingServicePtr logging, ui32 threadsCount);

}   // namespace NCloud::NBlockStore::NBD
