#include "part_nonrepl_rdma.h"

#include "part_nonrepl_rdma_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateNonreplicatedPartitionRdma(
    TStorageConfigPtr config,
    TNonreplicatedPartitionConfigPtr partConfig,
    NRdma::IClientPtr rdmaClient,
    TActorId statActorId)
{
    return std::make_unique<TNonreplicatedPartitionRdmaActor>(
        std::move(config),
        std::move(partConfig),
        std::move(rdmaClient),
        statActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
