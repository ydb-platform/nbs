#include "part.h"

#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreatePartitionTablet(
    const TActorId& owner,
    TTabletStorageInfoPtr storage,
    TStorageConfigConstPtr config,
    TDiagnosticsConfigConstPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 partitionIndex,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId,
    ui64 volumeTabletId,
    std::optional<bool> freshBlocksWriterEnabled)
{
    // Reuse this boot's selected mode, or select it for standalone callers.
    const bool useFreshBlocksWriter =
        freshBlocksWriterEnabled
            ? *freshBlocksWriterEnabled
            : config->GetFreshBlocksWriterEnabled() ||
                  config->IsFreshBlocksWriterFeatureEnabled(
                      partitionConfig.GetCloudId(),
                      partitionConfig.GetFolderId(),
                      partitionConfig.GetDiskId());
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
        volumeActorId,
        volumeTabletId,
        useFreshBlocksWriter);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
