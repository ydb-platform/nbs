#pragma once

#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/icv2/transport/handshake.h>
#include <cloud/blockstore/libs/icv2/transport/request_channel.h>
#include <cloud/storage/core/libs/icv2/tcp/connector.h>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IConnectorPtr;
using NCloud::NIcV2::TSocketFuture;
using NCloud::NIcV2::TSocketResult;

////////////////////////////////////////////////////////////////////////////////

using TChannelResult = TResultOrError<IRequestChannelPtr>;
using TChannelFuture = NThreading::TFuture<TChannelResult>;
using TChannelFactory =
    std::function<TChannelResult(const THandshakeReady&)>;

enum class EHostChannelState
{
    Idle,
    Connecting,
    Connected,
    Disconnected,
};

struct THostChannelStatus
{
    EHostChannelState State = EHostChannelState::Idle;
    ui64 ConnectionAttempts = 0;
    ui64 ConnectionFailures = 0;
};

// Internal outgoing-channel cache. The factory must return a prepared channel;
// the cache installs its close handler before activating it.
struct IPeerConnections: public IStartable
{
    // Concurrent callers share one attempt, its deadline and its future. The
    // configured HandshakeTimeoutMs covers TCP attempts, hello and activation.
    // An established channel is reused until it closes. No background retries
    // and no resending of requests: the next call starts a new attempt.
    // Callers enforce their own request deadlines, including time spent here.
    virtual TChannelFuture GetChannel(const TString& cellId) = 0;

    // A data binding owns a distinct entry. Addresses belong to one selected
    // host; only IP addresses of that host may be tried. No cross-host retry.
    // Binding IDs must increase across registrations and must never be reused.
    virtual NProto::TError RegisterHost(
        const TString& cellId, ui64 bindingId,
        TVector<NInterconnect::TAddress> addresses) = 0;
    virtual TChannelFuture GetHostChannel(const TString& cellId,
                                          ui64 bindingId) = 0;
    // Reads the existing host binding without initiating a connection.
    virtual THostChannelStatus GetHostStatus(
        const TString& cellId,
        ui64 bindingId) = 0;
    // Invalidates attempts and closes only this binding's channel. Late
    // callbacks cannot recreate its entry or affect another binding.
    virtual void UnregisterHost(const TString& cellId, ui64 bindingId) = 0;

    // Callback-safe. Rejects pending attempts and closes cached channels.
    // Stop is external-thread-only: joins connector/hello workers and waits for
    // admitted result continuations and channel callbacks. Neither callbacks
    // nor continuations may throw, wait for Stop or destroy the cache. The
    // owner must call Stop before releasing its last reference.
    virtual void RequestStop() = 0;
};

using IPeerConnectionsPtr = std::shared_ptr<IPeerConnections>;

// Resolves configured Hosts.Fqdn synchronously during creation, before workers
// start. All resolved addresses are tried in configuration/resolver order.
// An absent/zero IcV2Port leaves that peer unavailable for outgoing IC V2.
// Takes ownership of fresh connector/handshake workers and their lifecycle.
// The completion executor belongs to the caller; it must process tasks until
// Stop completes. The channel factory runs on a connector/hello callback
// thread.
TResultOrError<IPeerConnectionsPtr> CreatePeerConnections(
    NCells::TCellsConfigConstPtr config, IConnectorPtr connector,
    IHandshakeProviderPtr handshake, ITimerPtr timer, ITaskQueuePtr completions,
    TChannelFactory createChannel);

}   // namespace NCloud::NBlockStore::NIcV2
