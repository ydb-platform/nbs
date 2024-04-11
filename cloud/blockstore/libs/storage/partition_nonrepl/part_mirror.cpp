#include "part_mirror.h"

#include "part_mirror_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateMirrorPartition(
    TStorageConfigPtr config,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr digestGenerator,
    TString rwClientId,
    TNonreplicatedPartitionConfigPtr partConfig,
    TMigrations migrations,
    TVector<TDevices> replicas,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId statActorId,
    NActors::TActorId resyncActorId,
    bool dataScrubbingNeeded)
{
    return std::make_unique<TMirrorPartitionActor>(
        std::move(config),
        std::move(profileLog),
        std::move(digestGenerator),
        std::move(rwClientId),
        std::move(partConfig),
        std::move(migrations),
        std::move(replicas),
        std::move(rdmaClient),
        statActorId,
        resyncActorId,
        dataScrubbingNeeded);
}

}   // namespace NCloud::NBlockStore::NStorage
