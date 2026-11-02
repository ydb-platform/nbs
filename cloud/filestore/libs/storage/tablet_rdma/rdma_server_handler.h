#pragma once

#include "rdma_counters.h"

#include <cloud/filestore/libs/rdma/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// True for the methods the handler forwards to the tablet: the
// FILESTORE_SERVICE_REQUESTS and FILESTORE_TABLET_REQUESTS requests except
// CreateSession.
bool IsRdmaTabletMethod(ui32 methodId);

// Parses the typed request on the RDMA worker thread and hands it to the
// forwarder actor, which pipes it to the tablet and resolves the response.
IRdmaTabletRequestHandlerPtr CreateActorSystemRdmaRequestHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId forwarderId,
    TRdmaForwarderCountersPtr counters);

}   // namespace NCloud::NFileStore::NStorage
