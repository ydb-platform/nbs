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

}   // namespace NCloud::NBlockStore::NIcV2
