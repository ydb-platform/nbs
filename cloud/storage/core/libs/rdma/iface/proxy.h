#pragma once

#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct IProxy
{
    virtual ~IProxy() = 0;

    virtual IClientEndpointPtr GetEndpoint(const TString& host, ui32 port) = 0;
    virtual bool IsAlignedDataEnabled() const = 0;
};

IProxyPtr CreateProxy(
    IClientPtr client,
    IClientEndpointHandlerPtr handler,
    TVector<std::pair<TString, ui32>> endponts);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NStorage::NRdma
