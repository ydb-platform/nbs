#include "part.h"

#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreatePartitionTablet(
    const TActorId& owner,
    TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 partitionIndex,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId)
{
    return std::make_unique<TPartitionActor>(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(partitionConfig),
        storageAccessMode,
        partitionIndex,
        siblingCount,
        volumeActorId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
