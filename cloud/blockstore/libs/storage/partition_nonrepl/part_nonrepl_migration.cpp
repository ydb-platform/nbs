#include "part_nonrepl_migration.h"

#include "part_nonrepl_migration_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateNonreplicatedPartitionMigration(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr digestGenerator,
    ui64 initialMigrationIndex,
    TString rwClientId,
    TNonreplicatedPartitionConfigPtr partConfig,
    google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
    NRdma::IClientPtr rdmaClient,
    NActors::TActorId volumeActorId,
    NActors::TActorId statActorId,
    NActors::TActorId migrationSrcActorId)
{
    return std::make_unique<TNonreplicatedPartitionMigrationActor>(
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(digestGenerator),
        initialMigrationIndex,
        std::move(rwClientId),
        std::move(partConfig),
        std::move(migrations),
        std::move(rdmaClient),
        volumeActorId,
        statActorId,
        migrationSrcActorId);
}

}   // namespace NCloud::NBlockStore::NStorage
