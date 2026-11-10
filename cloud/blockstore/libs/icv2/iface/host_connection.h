#pragma once

#include "public.h"

#include <util/system/types.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

// Reports closure of an established connection. Ignore notifications belonging
// to a replaced binding; callbacks may arrive from another thread.
// Do not stop the endpoint or transport from a callback.
struct IHostConnectionHandler
{
    virtual ~IHostConnectionHandler();

    virtual void HandleDisconnected() noexcept = 0;
};

}   // namespace NCloud::NBlockStore::NIcV2
