#pragma once

#include "rdma_tablet_registry.h"

#include <cloud/filestore/config/rdma.pb.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * Creates the component that serves the index tablets of the node over RDMA.
 * Start() registers the forwarder actor, starts the RDMA server and listens;
 * if listening fails, it reports a critical event and stops the RDMA server,
 * and the node keeps serving over the pipe. Stop() stops the RDMA server.
 *
 * @param config - Contents of the RDMA config file.
 * @param actorSystem - Actor system of the node; started before Start().
 * @param server - RDMA server to listen with; started and stopped here.
 * @param registry - Pipe servers of the node's tablets, the one passed to the
 *  tablets.
 */
IStartablePtr CreateRdmaTabletTarget(
    const NProto::TRdmaConfig& config,
    ILoggingServicePtr logging,
    IActorSystemPtr actorSystem,
    NCloud::NStorage::NRdma::IServerPtr server,
    TRdmaTabletRegistryPtr registry);

}   // namespace NCloud::NFileStore::NStorage
