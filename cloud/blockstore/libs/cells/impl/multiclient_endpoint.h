#pragma once

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct IMultiClientEndpoint: public IBlockStore
{
    virtual IBlockStorePtr CreateClientEndpoint(
        const TString& clientId,
        const TString& instanceId) = 0;
};

using IMultiClientEndpointPtr = std::shared_ptr<IMultiClientEndpoint>;

////////////////////////////////////////////////////////////////////////////////

IMultiClientEndpointPtr CreateMultiClientEndpoint(
    NClient::IMultiHostClientPtr client,
    const TString& host,
    ui32 port,
    bool isSecure);

}   // namespace NCloud::NBlockStore::NCells
