#pragma once

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

/**
 * Creates the handler that the RDMA target calls on its poller threads. It
 * parses the typed request and sends it to the tablet of the client's pipe
 * server as if it came through that pipe.
 *
 * @param actorSystem - Actor system of the node, used to send the request to
 *  the tablet and the tracking event to the forwarder.
 * @param forwarderId - Forwarder actor (CreateRdmaTabletForwarderActor) that
 *  receives the tablet's response and resolves the returned future.
 * @param registry - Pipe servers of the node's tablets; finds the tablet of
 *  the pipe server named in the request.
 */
IRdmaTabletRequestHandlerPtr CreateActorSystemRdmaRequestHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId forwarderId,
    TRdmaTabletRegistryPtr registry);

}   // namespace NCloud::NFileStore::NStorage
