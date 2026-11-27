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

// Start once before Connect()/Accept(); concurrent calls are allowed.
// Pass connected, nonblocking sockets and do not use them elsewhere.
// Failure closes the submitted socket. Timeout covers the entire operation.
// Stop() cancels pending handshakes and waits for callbacks; handshakes already
// completing may succeed. Completed sockets remain open after Stop().
// Keep the provider alive until Stop() returns. Call Stop() outside callbacks;
// callbacks must not throw. Restart is unsupported.
struct IHandshakeProvider: public IStartable
{
    // Use for outgoing sockets; negotiates XDC when both peers enable it.
    virtual TConnectHandshakeFuture Connect(
        TSocketPtr socket,
        TDuration timeout) = 0;

    // Submit all accepted sockets to the same provider. Create a channel only
    // for THandshakeReady; ignore THandshakeIncomplete.
    virtual TAcceptHandshakeFuture Accept(
        TSocketPtr socket,
        TDuration timeout) = 0;

    // Rejects new work without waiting. Safe in callbacks; call Stop() outside
    // callbacks to finish shutdown.
    virtual void RequestStop() = 0;
};

using IHandshakeProviderPtr = std::shared_ptr<IHandshakeProvider>;

TResultOrError<IHandshakeProviderPtr> CreateHandshakeProvider(
    THandshakeConfig config);

}   // namespace NCloud::NIcV2
