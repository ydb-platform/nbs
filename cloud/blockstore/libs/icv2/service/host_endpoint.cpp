#include "host_endpoint.h"

#include "peer_request_channel.h"

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IPeerManagerPtr;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler,
    IPeerManagerPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    TDataConfig dataConfig)
{
    if (!peers || !timer || !scheduler || !completions || !dataConfig.IsValid())
    {
        return MakeError(
            E_ARGUMENT,
            "invalid IC V2 host endpoint configuration");
    }

    auto [peer, error] =
        peers->CreatePeer(std::move(host), port, std::move(handler));
    if (HasError(error)) {
        return error;
    }
    auto channel = CreatePeerRequestChannel(
        peer,
        std::move(timer),
        std::move(scheduler),
        std::move(completions));
    const bool activated = channel->Activate();
    Y_DEBUG_ABORT_UNLESS(activated);

    auto [endpoint, endpointError] =
        CreateDataEndpoint(std::move(channel), dataConfig);
    if (HasError(endpointError)) {
        return endpointError;
    }
    return THostDataEndpoint{
        .Peer = std::move(peer),
        .Endpoint = std::move(endpoint),
    };
}

}   // namespace NCloud::NBlockStore::NIcV2
