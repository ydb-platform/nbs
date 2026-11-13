#pragma once

#include "rdma_counters.h"
#include "rdma_tablet_registry.h"

#include <cloud/filestore/libs/rdma/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// True for the methods the handler forwards to the tablet: the
// FILESTORE_SERVICE_REQUESTS and FILESTORE_TABLET_REQUESTS requests except
// CreateSession.
bool IsRdmaTabletMethod(ui32 methodId);

// Parses the typed request on the RDMA poller thread and sends it to the tablet
// of the client's pipe server as if it came through that pipe; the tablet
// answers to the forwarder, which resolves the response.
IRdmaTabletRequestHandlerPtr CreateActorSystemRdmaRequestHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId forwarderId,
    TRdmaTabletRegistryPtr registry,
    TRdmaForwarderCountersPtr counters);

}   // namespace NCloud::NFileStore::NStorage
