#pragma once

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

ICellManagerPtr CreateCellManager(
    TCellsConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ITraceSerializerPtr traceSerializer,
    IServerStatsPtr serverStats,
    NRdma::IClientPtr rdmaClient);

}   // namespace NCloud::NBlockStore::NCells
