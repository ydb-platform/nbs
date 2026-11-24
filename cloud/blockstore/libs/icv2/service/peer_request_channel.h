#pragma once

#include <cloud/blockstore/libs/icv2/data/request_channel.h>

#include <cloud/storage/core/libs/icv2/peer/peer.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Pass a dedicated peer. Give the channel to a data endpoint or call Close()
// yourself. Await shutdown before stopping the peer manager, then stop the
// scheduler and completion queue.
IDataRequestChannelPtr CreatePeerRequestChannel(
    NCloud::NIcV2::IPeerPtr peer,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions);

}   // namespace NCloud::NBlockStore::NIcV2
