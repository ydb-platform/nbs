#pragma once

#include "public.h"

#include "peer.h"

#include <cloud/storage/core/libs/common/connection_handler.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>
#include <cloud/storage/core/libs/icv2/tcp/address_resolver.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Create channels with the application's request types and handler.
// Calls may overlap; return errors through TRequestChannelResult.
struct IRequestChannelFactory
{
    virtual ~IRequestChannelFactory();

    // Return an inactive channel. XdcSocket may be empty.
    virtual TRequestChannelResult CreateRequestChannel(
        const THandshakeReady& connection) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TPeerManagerConfig
{
    // Total timeout for address resolution, connection and handshake.
    TDuration ConnectTimeout = TDuration::Seconds(5);
};

////////////////////////////////////////////////////////////////////////////////

// Start once before creating peers. Stop() closes connections and waits for
// pending channel requests and callbacks. Call it outside callbacks and keep
// the manager alive until it returns. Restart is unsupported.
struct IPeerManager: public IStartable
{
    // Pass a hostname or IP address and a nonzero port. Creates an independent
    // peer and connects asynchronously. Pass {} to omit notifications.
    // Notifications are serialized per peer and may run on the caller's
    // thread before return. Handlers and future callbacks must not throw,
    // block or await shutdown.
    virtual TResultOrError<IPeerPtr> CreatePeer(
        TString host,
        ui16 port,
        IClientConnectionHandlerPtr handler) = 0;

    // Rejects new peers and channel requests without waiting. Safe in
    // callbacks; call Stop() outside callbacks to finish shutdown.
    virtual void RequestStop() = 0;
};

using IPeerManagerPtr = std::shared_ptr<IPeerManager>;

// Pass an unused connector and handshake provider; the manager starts and
// stops them. Use the same clock for timer and scheduler. Keep the scheduler,
// resolver and completions running until Stop() returns.
// Dependencies must be non-null and ConnectTimeout positive.
TResultOrError<IPeerManagerPtr> CreatePeerManager(
    TPeerManagerConfig config,
    IConnectorPtr connector,
    IHandshakeProviderPtr handshake,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IAddressResolverPtr resolver,
    ITaskQueuePtr completions,
    IRequestChannelFactoryPtr channelFactory);

}   // namespace NCloud::NIcV2
