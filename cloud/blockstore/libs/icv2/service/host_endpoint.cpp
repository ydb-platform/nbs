#include "host_endpoint.h"

#include "peer_request_channel.h"

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::TPeerStatus;

////////////////////////////////////////////////////////////////////////////////

THostDataEndpoint::THostDataEndpoint(IPeerPtr peer, IDataEndpointPtr endpoint)
    : Peer(std::move(peer))
    , Endpoint(std::move(endpoint))
{}

IDataEndpointPtr THostDataEndpoint::GetEndpoint() const
{
    return Endpoint;
}

TPeerStatus THostDataEndpoint::GetStatus() const
{
    Y_DEBUG_ABORT_UNLESS(Peer);

    return Peer->GetStatus();
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler,
    IPeerManagerPtr peers,
    TDataConfig dataConfig)
{
    if (!peers || !dataConfig.IsValid()) {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 host endpoint configuration");
    }

    auto [peer, error] =
        peers->CreatePeer(std::move(host), port, std::move(handler));
    if (HasError(error)) {
        return error;
    }
    auto channel = CreatePeerRequestChannel(peer);
    auto endpoint = CreateDataEndpoint(std::move(channel), dataConfig);
    return THostDataEndpoint(std::move(peer), std::move(endpoint));
}

}   // namespace NCloud::NBlockStore::NIcV2
