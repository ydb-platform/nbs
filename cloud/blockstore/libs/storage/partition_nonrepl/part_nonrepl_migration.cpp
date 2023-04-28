#include "part_nonrepl_migration.h"

#include "part_nonrepl_migration_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateNonreplicatedPartitionMigration(
    TStorageConfigPtr config,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr digestGenerator,
    ui64 initialMigrationIndex,
    TString rwClientId,
    TNonreplicatedPartitionConfigPtr partConfig,
    google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
    NRdma::IClientPtr rdmaClient)
{
    return std::make_unique<TNonreplicatedPartitionMigrationActor>(
        std::move(config),
        std::move(profileLog),
        std::move(digestGenerator),
        initialMigrationIndex,
        std::move(rwClientId),
        std::move(partConfig),
        std::move(migrations),
        std::move(rdmaClient));
}

}   // namespace NCloud::NBlockStore::NStorage
