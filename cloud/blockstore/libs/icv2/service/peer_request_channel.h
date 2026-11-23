#pragma once

#include <cloud/storage/core/libs/icv2/peer/peer.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Pass a dedicated peer. Activate before sending I/O. Keep the scheduler and
// completion queue running until the channel and peer manager have stopped.
NCloud::NIcV2::IRequestChannelPtr CreatePeerRequestChannel(
    NCloud::NIcV2::IPeerPtr peer,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions);

}   // namespace NCloud::NBlockStore::NIcV2
