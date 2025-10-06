#include "part_mirror.h"

#include "part_mirror_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateMirrorPartition(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr digestGenerator,
    TString rwClientId,
    TNonreplicatedPartitionConfigPtr partConfig,
    TMigrations migrations,
    TVector<TDevices> replicas,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId statActorId,
    NActors::TActorId volumeActorId,
    NActors::TActorId resyncActorId)
{
    return std::make_unique<TMirrorPartitionActor>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(digestGenerator),
        std::move(rwClientId),
        std::move(partConfig),
        std::move(migrations),
        std::move(replicas),
        std::move(rdmaClient),
        statActorId,
        resyncActorId,
        volumeActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
