#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct IMultiClientEndpoint: public IBlockStore
{
    virtual IBlockStorePtr CreateClientEndpoint(
        const TString& clientId,
        const TString& instanceId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IMultiClientEndpointPtr CreateMultiClientEndpoint(
    IMultiHostClientPtr client,
    const TString& host,
    ui32 port,
    bool isSecure);

}   // namespace NCloud::NBlockStore::NClient
