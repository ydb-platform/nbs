#pragma once

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::IClientPtr CreateRdmaClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NCloud::NStorage::NRdma::TClientConfigPtr config);

NCloud::NStorage::NRdma::IServerPtr CreateRdmaServer(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NCloud::NStorage::NRdma::TServerConfigPtr config);

}   // namespace NCloud::NBlockStore::NRdma
