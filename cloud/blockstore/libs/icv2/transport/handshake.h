#pragma once

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/icv2/tcp/socket.h>

#include <variant>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::TSocketPtr;

////////////////////////////////////////////////////////////////////////////////

struct THandshakeConfig
{
    bool EnableChecksum = false;
    bool EnableXdc = false;
};

// Use these sockets to create an IC V2 channel; XdcSocket may be empty.
struct THandshakeReady
{
    TSocketPtr Socket;
    TSocketPtr XdcSocket;
};

using THandshakeResult = TResultOrError<THandshakeReady>;
using THandshakeFuture = NThreading::TFuture<THandshakeResult>;

// No channel to create for this result; the connection belongs to another
// Accept.
struct THandshakeIncomplete
{
};

using TAcceptHandshakeOutcome =
    std::variant<THandshakeReady, THandshakeIncomplete>;
using TAcceptHandshakeResult = TResultOrError<TAcceptHandshakeOutcome>;
using TAcceptHandshakeFuture = NThreading::TFuture<TAcceptHandshakeResult>;

// Start once before Connect/Accept; both may be called repeatedly and
// concurrently. Stop cancels pending handshakes and waits for continuations.
// Restart is not supported; completed connections survive Stop. Submit
// connected, nonblocking sockets and do not use them elsewhere. Continuations
// must not throw, call Stop, or destroy the provider.
struct IHandshakeProvider: public IStartable
{
    // Use for outgoing sockets. Returns a ready connection or closes the socket
    // on failure. Timeout includes queue time and, when both peers enable XDC,
    // opening a second socket to the same address and port.
    virtual THandshakeFuture Connect(TSocketPtr socket, TDuration timeout) = 0;

    // Submit every accepted socket to the same provider. Create a channel for
    // THandshakeReady; ignore THandshakeIncomplete. Timeout includes
    // waiting for the other socket when XDC is enabled on both peers.
    virtual TAcceptHandshakeFuture Accept(
        TSocketPtr socket,
        TDuration timeout) = 0;

    // Safe from continuations. Rejects new work without waiting for completion;
    // call Stop elsewhere to wait. Already completing handshakes may succeed.
    virtual void RequestStop() = 0;
};

using IHandshakeProviderPtr = std::shared_ptr<IHandshakeProvider>;

TResultOrError<IHandshakeProviderPtr> CreateHandshakeProvider(
    THandshakeConfig config);

}   // namespace NCloud::NBlockStore::NIcV2
