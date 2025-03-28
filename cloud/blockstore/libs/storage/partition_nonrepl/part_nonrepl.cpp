#include "part_nonrepl.h"

#include "part_nonrepl_actor.h"
#include "part_nonrepl_rdma.h"
#include "part_nonrepl_rdma_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateNonreplicatedPartition(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TNonreplicatedPartitionConfigPtr partConfig,
    TActorId statActorId,
    NRdma::IClientPtr rdmaClient)
{
    if (rdmaClient) {
        return CreateNonreplicatedPartitionRdma(
            std::move(config),
            std::move(diagnosticsConfig),
            std::move(partConfig),
            std::move(rdmaClient),
            statActorId);
    }

    return std::make_unique<TNonreplicatedPartitionActor>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(partConfig),
        statActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
