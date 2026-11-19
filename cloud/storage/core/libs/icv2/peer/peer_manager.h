#pragma once

#include "public.h"

#include "peer.h"

#include <cloud/storage/core/libs/common/endpoint_handler.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>
#include <cloud/storage/core/libs/icv2/tcp/address_resolver.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Supply the request types and handler for the application using the peer.
// Calls may overlap. Return errors through the result.
struct IChannelFactory
{
    virtual ~IChannelFactory();

    // Create a channel from the connected sockets. XdcSocket may be empty.
    // Return it inactive for the manager to activate.
    virtual TChannelResult CreateChannel(
        const NCloud::NIcV2::THandshakeReady& connection) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TPeerManagerConfig
{
    // Shared budget for DNS, TCP connect and handshake. Starts at host
    // resolution.
    TDuration ConnectTimeout = TDuration::Seconds(5);
};

////////////////////////////////////////////////////////////////////////////////

// Call Start() once before creating peers. Keep an owning reference to the
// manager until Stop() returns. Stop() closes its connections and waits for
// connection callbacks and continuations of pending GetChannel() futures.
// Call Stop() outside those callbacks. Restart is unsupported.
struct IPeerManager: public IStartable
{
    // Pass a hostname or IP address and a nonzero port. Each successful call
    // creates an independent peer and starts connecting asynchronously.
    // Pass a handler for connection, disconnection and failed-attempt events,
    // or an empty pointer if you only need GetChannel(). Handler calls for
    // this peer are serialized and may begin before CreatePeer() returns.
    // Handlers and future continuations must not throw or wait for shutdown.
    virtual TResultOrError<IPeerPtr>
    CreatePeer(TString host, ui16 port, IClientEndpointHandlerPtr handler) = 0;

    // Begin shutdown and reject new peers and channel requests.
    // Safe to call from callbacks. Call Stop() elsewhere to await shutdown.
    virtual void RequestStop() = 0;
};

using IPeerManagerPtr = std::shared_ptr<IPeerManager>;

// Create a manager ready for Start(). Give it a fresh connector and handshake
// provider, which it starts and stops. Use the same clock for timer and
// scheduler. Keep the scheduler, resolver and completion executor usable
// until Stop() returns. Pass non-null dependencies and a positive timeout.
TResultOrError<IPeerManagerPtr> CreatePeerManager(
    TPeerManagerConfig config,
    IConnectorPtr connector,
    NCloud::NIcV2::IHandshakeProviderPtr handshake,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITaskQueuePtr completions,
    IChannelFactoryPtr channelFactory);

}   // namespace NCloud::NIcV2
