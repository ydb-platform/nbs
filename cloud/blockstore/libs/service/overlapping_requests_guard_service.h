
#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

/*
A service for dispatching write requests to its own OverlappingRequestsGuard
handler, individual for each disk.
A OverlappingRequestsGuard handler for a disk is created when the first request
for a new disk is received.
Dispatching requests between existing handlers is implemented lock-free. The
lock is taken only for the duration of the creation of a new handler and affects
only requests to new disks.
Only WriteBlocks, WriteBlocksLocal, ZeroBlocks, MountVolume requests are sent to
OverlappingRequestsGuard, the rest of the requests go through directly.

Diagram.
                      DurableClient
                            |
                            v
            TOverlappingRequestsGuardsService
                      |    |    |
                      |    |    |    ReadBlocks(any volume)
                      |    |    -----------------------------
                      |    |                                |
WriteBlocks(volume1)  |    | WriteBlocks(volume2)           |
              ---------    ------------------               |
              |                             |               |
              v                             v               |
  TOverlappingRequestsGuard     TOverlappingRequestsGuard   |
          (Volume1)                     (Volume2)           |
              |                             |               |
              ------------  -----------------               |
                         |  |  ------------------------------
                         |  |  |
                         v  v  v
                     TKikimrService


*/

IBlockStorePtr CreateOverlappingRequestsGuardsService(IBlockStorePtr service);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
