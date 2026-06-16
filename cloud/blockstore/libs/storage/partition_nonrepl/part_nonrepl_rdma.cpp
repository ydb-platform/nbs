#include "part_nonrepl_rdma.h"

#include "part_nonrepl_rdma_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateNonreplicatedPartitionRdma(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TNonreplicatedPartitionConfigPtr partConfig,
    NCloud::NStorage::NRdma::IProxyPtr rdmaProxy,
    TActorId volumeActorId,
    TActorId statActorId)
{
    return std::make_unique<TNonreplicatedPartitionRdmaActor>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(partConfig),
        std::move(rdmaProxy),
        volumeActorId,
        statActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
