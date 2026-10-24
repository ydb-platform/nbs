#pragma once

#include <cloud/blockstore/config/rdma.pb.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

#include <util/generic/ptr.h>
#include <util/system/hostname.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlockstoreServerRdmaTargetConfig
{
    TString Host = "localhost";
    ui32 Port = 10088;
    ui32 WorkerThreads = 1;
    bool ConnectionMonitoringEnabled = false;

    explicit TBlockstoreServerRdmaTargetConfig(
        const NProto::TRdmaTarget& target)
    {
        ConnectionMonitoringEnabled = target.GetConnectionMonitoringEnabled();

        const auto& endpoint = target.GetEndpoint();

        if (const auto& host = endpoint.GetHost()) {
            Host = host;
        }

        if (auto port = endpoint.GetPort()) {
            Port = port;
        }

        if (auto threads = target.GetWorkerThreads()) {
            WorkerThreads = threads;
        }
    }
};

using TBlockstoreServerRdmaTargetConfigPtr =
    std::shared_ptr<TBlockstoreServerRdmaTargetConfig>;

////////////////////////////////////////////////////////////////////////////////

IStartablePtr CreateBlockstoreServerRdmaTarget(
    TBlockstoreServerRdmaTargetConfigPtr rdmaTargetConfig,
    ILoggingServicePtr logging,
    ITraceSerializerPtr traceSerializer,
    IMonitoringServicePtr monitoring,
    NCloud::NStorage::NRdma::IServerPtr server,
    IBlockStorePtr service);

}   // namespace NCloud::NBlockStore::NStorage
