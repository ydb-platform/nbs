#include "part_mirror_resync.h"

#include "part_mirror_resync_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateMirrorPartitionResync(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr digestGenerator,
    TString rwClientId,
    TNonreplicatedPartitionConfigPtr partConfig,
    TMigrations migrations,
    TVector<TDevices> replicaDevices,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId statActorId,
    ui64 initialResyncIndex,
    NProto::EResyncPolicy resyncPolicy,
    bool critOnChecksumMismatch,
    NActors::TActorId volumeActorId)
{
    return std::make_unique<TMirrorPartitionResyncActor>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(digestGenerator),
        std::move(rwClientId),
        std::move(partConfig),
        std::move(migrations),
        std::move(replicaDevices),
        std::move(rdmaClient),
        statActorId,
        initialResyncIndex,
        resyncPolicy,
        critOnChecksumMismatch,
        volumeActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
