#pragma once

#include <util/system/types.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

enum class EPeerState
{
    Idle,
    Connecting,
    Connected,
    Disconnected,   // Waiting to reconnect or permanently closed.
};

////////////////////////////////////////////////////////////////////////////////

// Counts connection attempts and failed attempts for this peer.
struct TPeerStatus
{
    EPeerState State = EPeerState::Idle;
    ui64 ConnectionAttempts = 0;
    ui64 ConnectionFailures = 0;
};

}   // namespace NCloud::NIcV2
