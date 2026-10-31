#pragma once

#include "socket.h"

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeSocketError(int error, const char* operation);
// Create an unconnected IPv4/IPv6 socket ready for nonblocking TCP use.
TSocketResult CreateSocket(const NInterconnect::TAddress& address);
// Returns 0 or positive errno; EINPROGRESS/EINTR require checking completion.
int ConnectSocket(
    const NInterconnect::TStreamSocket& socket,
    const NInterconnect::TAddress& address);
// Enables TCP_NODELAY.
TSocketResult PrepareSocket(TSocketPtr socket);

}   // namespace NCloud::NIcV2
