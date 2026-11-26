#pragma once

#include <cloud/blockstore/libs/icv2/data/endpoint.h>

#include <cloud/storage/core/libs/icv2/peer/peer_manager.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

class THostDataEndpoint
{
    NCloud::NIcV2::IPeerPtr Peer;
    IDataEndpointPtr Endpoint;

public:
    THostDataEndpoint() = default;
    THostDataEndpoint(NCloud::NIcV2::IPeerPtr peer, IDataEndpointPtr endpoint);

    [[nodiscard]] IDataEndpointPtr GetEndpoint() const;
    [[nodiscard]] NCloud::NIcV2::IPeerPtr GetPeer() const;
    [[nodiscard]] NCloud::NIcV2::TPeerStatus GetStatus() const;
};

// Start the peer manager first. Pass {} to omit notifications.
// The handler may run before this call returns.
//
// Reuse the data endpoint for this host's disks. While connecting, requests
// return E_UNAVAILABLE. Wrap the endpoint in a durable client for retries.
// A retry may apply a write or zero twice.
//
// Call Stop() on the data endpoint, then on the peer manager. Stop the
// scheduler and completion queue after those calls return.
TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler,
    NCloud::NIcV2::IPeerManagerPtr peers,
    TDataConfig dataConfig);

}   // namespace NCloud::NBlockStore::NIcV2
