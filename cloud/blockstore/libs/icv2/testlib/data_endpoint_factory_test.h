#pragma once

#include <cloud/blockstore/libs/icv2/iface/data_endpoint_factory.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TDataEndpointRequest
{
    TString CellId;
    TString Host;
};

////////////////////////////////////////////////////////////////////////////////

// Queue one result per creation call. Endpoints forward R/W/Z to the supplied
// service and keep independent channel state. They never start or stop it.
// Complete pending backend requests to let endpoint Stop finish.
class TTestDataEndpointFactory final: public IDataEndpointFactory
{
    struct TImpl;
    const std::unique_ptr<TImpl> Impl;

public:
    TTestDataEndpointFactory();
    ~TTestDataEndpointFactory() override;

    void EnqueueEndpoint(IBlockStorePtr service);
    void EnqueueError(NProto::TError error);
    TVector<TDataEndpointRequest> GetRequests() const;

    TResultOrError<IBlockStorePtr> CreateDataEndpoint(
        const TString& cellId,
        const TString& host,
        IClientEndpointHandlerPtr handler) override;
    THostChannelStatus GetDataEndpointStatus(
        const IBlockStorePtr& endpoint) override;

    void SetStatus(
        const IBlockStorePtr& endpoint,
        const THostChannelStatus& status);
    // Updates status and notifies synchronously.
    void NotifyConnected(const IBlockStorePtr& endpoint);
    void NotifyUnavailable(const IBlockStorePtr& endpoint);
    // Only notifies after Connected.
    void NotifyDisconnected(const IBlockStorePtr& endpoint);
};

}   // namespace NCloud::NBlockStore::NIcV2
