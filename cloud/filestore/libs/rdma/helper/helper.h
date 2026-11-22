#pragma once

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>
#include <cloud/storage/core/protos/rdma.pb.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::IServerPtr CreateRdmaServer(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const NCloud::NProto::TRdmaServer& config);

}   // namespace NCloud::NFileStore::NStorage
