#include "helper.h"

#include <cloud/storage/core/libs/rdma/impl/client.h>
#include <cloud/storage/core/libs/rdma/impl/server.h>
#include <cloud/storage/core/libs/rdma/impl/verbs.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::IClientPtr CreateRdmaClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NCloud::NStorage::NRdma::TClientConfigPtr config)
{
    return NCloud::NStorage::NRdma::CreateClient(
        NCloud::NStorage::NRdma::NVerbs::CreateVerbs(),
        NCloud::NStorage::NRdma::TObservabilityProvider(
            std::move(logging),
            monitoring,
            "BLOCKSTORE_RDMA",
            "blockstore",
            "rdma_client"),
        std::move(config));
}

NCloud::NStorage::NRdma::IServerPtr CreateRdmaServer(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    NCloud::NStorage::NRdma::TServerConfigPtr config)
{
    return NCloud::NStorage::NRdma::CreateServer(
        NCloud::NStorage::NRdma::NVerbs::CreateVerbs(),
        NCloud::NStorage::NRdma::TObservabilityProvider(
            std::move(logging),
            monitoring,
            "BLOCKSTORE_RDMA",
            "blockstore",
            "rdma_server"),
        std::move(config));
}

}   // namespace NCloud::NBlockStore::NRdma
