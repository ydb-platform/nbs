#pragma once

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/icv2/tcp/socket.h>

#include <variant>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct THandshakeConfig
{
    bool EnableXdc = false;
};

// Use these sockets to create an IC V2 channel; XdcSocket may be empty.
struct THandshakeReady
{
    TSocketPtr Socket;
    TSocketPtr XdcSocket;
};

using TConnectHandshakeResult = TResultOrError<THandshakeReady>;
using TConnectHandshakeFuture = NThreading::TFuture<TConnectHandshakeResult>;

// No channel to create for this result.
struct THandshakeIncomplete
{
};

using TAcceptHandshakeOutcome =
    std::variant<THandshakeReady, THandshakeIncomplete>;
using TAcceptHandshakeResult = TResultOrError<TAcceptHandshakeOutcome>;
using TAcceptHandshakeFuture = NThreading::TFuture<TAcceptHandshakeResult>;

// Start once before Connect/Accept; both may be called repeatedly and
// concurrently. Stop cancels unfinished handshakes and waits for continuations;
// handshakes already completing may succeed. Restart is not supported, and
// completed connections survive Stop. Submit connected, nonblocking sockets
// and do not use them elsewhere; a failed handshake closes its socket.
// Each timeout starts at the call and includes queue time. After Start, the
// owner must keep a strong reference until Stop returns. Call Stop outside a
// continuation; RequestStop alone is not enough. Continuations must not throw.
struct IHandshakeProvider: public IStartable
{
    // Use for outgoing sockets. If both peers enable XDC, opens a second socket
    // to the same address and port within the timeout.
    virtual TConnectHandshakeFuture Connect(
        TSocketPtr socket,
        TDuration timeout) = 0;

    // Submit every accepted socket to the same provider. Create a channel for
    // THandshakeReady; ignore THandshakeIncomplete. Timeout includes
    // waiting for the other socket when XDC is enabled on both peers.
    virtual TAcceptHandshakeFuture Accept(
        TSocketPtr socket,
        TDuration timeout) = 0;

    // Safe from continuations. Rejects new work without waiting for completion;
    // call Stop elsewhere to wait.
    virtual void RequestStop() = 0;
};

using IHandshakeProviderPtr = std::shared_ptr<IHandshakeProvider>;

TResultOrError<IHandshakeProviderPtr> CreateHandshakeProvider(
    THandshakeConfig config);

}   // namespace NCloud::NIcV2
