#pragma once

#include <cloud/blockstore/config/rdma.pb.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaTargetConfig
{
    TString Host = "localhost";
    ui32 Port = 10088;
    ui32 WorkerThreads = 1;

    explicit TRdmaTargetConfig(const NProto::TRdmaTarget& target)
    {
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

using TRdmaTargetConfigPtr = std::shared_ptr<TRdmaTargetConfig>;

IStartablePtr CreateBlockstoreServerRdmaTarget(
    TRdmaTargetConfigPtr rdmaTargetConfig,
    ILoggingServicePtr logging,
    NRdma::IServerPtr server,
    IBlockStorePtr service,
    TExecutorPtr executor);

}   // namespace NCloud::NBlockStore::NStorage
