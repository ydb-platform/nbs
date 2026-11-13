#pragma once

#include "public.h"

#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/icv2/iface/host_connection.h>

#include <cloud/storage/core/libs/common/endpoint_handler.h>
#include <cloud/storage/core/libs/icv2/handshake/handshake.h>
#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>
#include <cloud/storage/core/libs/icv2/tcp/address_resolver.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IConnectorPtr;
using NCloud::NIcV2::TSocketFuture;
using NCloud::NIcV2::TSocketResult;

////////////////////////////////////////////////////////////////////////////////

using TChannelResult = TResultOrError<NCloud::NIcV2::IRequestChannelPtr>;
using TChannelFuture = NThreading::TFuture<TChannelResult>;
using TChannelFactory =
    std::function<TChannelResult(const NCloud::NIcV2::THandshakeReady&)>;

// Start once; Stop externally before releasing the owner reference.
// The channel factory must return an inactive channel.
struct IPeerConnections: public IStartable
{
    // Reuses an open channel or shares one connection attempt for this cell.
    // HandshakeTimeoutMs covers TCP, hello and activation. A new call retries
    // a failed attempt. Callers enforce their own request deadlines.
    virtual TChannelFuture GetChannel(const TString& cellId) = 0;

    // A data binding connects and retries in the background, including DNS.
    // Only addresses of the selected host may be tried. No cross-host retry.
    // Binding IDs must increase across registrations and must never be reused.
    virtual NProto::TError RegisterHost(
        const TString& cellId,
        ui64 bindingId,
        TString host,
        ui16 port,
        IClientEndpointHandlerPtr handler) = 0;
    virtual TChannelFuture GetHostChannel(
        const TString& cellId,
        ui64 bindingId) = 0;
    // Reads the existing host binding without initiating a connection.
    virtual THostChannelStatus GetHostStatus(
        const TString& cellId,
        ui64 bindingId) = 0;
    // Invalidates attempts and closes only this binding's channel. Late
    // callbacks cannot recreate its entry or affect another binding.
    virtual void UnregisterHost(const TString& cellId, ui64 bindingId) = 0;

    // Callback-safe shutdown request. External Stop waits for callbacks;
    // keep the cache alive until it returns. Callbacks must not throw.
    virtual void RequestStop() = 0;
};

using IPeerConnectionsPtr = std::shared_ptr<IPeerConnections>;

// Resolves configured cell hosts during creation; RegisterHost resolves in
// background. Pass fresh connector/handshake workers; the cache manages their
// Start/Stop. Keep the resolver and completion executor running until cache
// Stop returns.
TResultOrError<IPeerConnectionsPtr> CreatePeerConnections(
    NCells::TCellsConfigConstPtr config,
    IConnectorPtr connector,
    NCloud::NIcV2::IHandshakeProviderPtr handshake,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITaskQueuePtr completions,
    TChannelFactory createChannel);

}   // namespace NCloud::NBlockStore::NIcV2
