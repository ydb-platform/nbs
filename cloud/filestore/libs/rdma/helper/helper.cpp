#include "helper.h"

#include <cloud/storage/core/libs/rdma/iface/config.h>
#include <cloud/storage/core/libs/rdma/impl/server.h>
#include <cloud/storage/core/libs/rdma/impl/verbs.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::IServerPtr CreateRdmaServer(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const NCloud::NProto::TRdmaServer& config)
{
    return NCloud::NStorage::NRdma::CreateServer(
        NCloud::NStorage::NRdma::NVerbs::CreateVerbs(),
        NCloud::NStorage::NRdma::TObservabilityProvider(
            std::move(logging),
            std::move(monitoring),
            "NFS_RDMA",
            "filestore",
            "rdma_server"),
        NCloud::NStorage::NRdma::CreateServerConfigPtr(config));
}

}   // namespace NCloud::NFileStore::NStorage
