#include "test_runtime.h"

#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TStorageConfigPtr CreateTestStorageConfig(
    NProto::TStorageServiceConfig storageServiceConfig,
    NCloud::NProto::TFeaturesConfig featuresConfig)
{
    if (!storageServiceConfig.GetSchemeShardDir()) {
        storageServiceConfig.SetSchemeShardDir("/local/nbs");
    }
    storageServiceConfig.SetHDDSystemChannelPoolKind("pool-kind-1");
    storageServiceConfig.SetHDDLogChannelPoolKind("pool-kind-1");
    storageServiceConfig.SetHDDIndexChannelPoolKind("pool-kind-1");
    storageServiceConfig.SetHDDMixedChannelPoolKind("pool-kind-1");
    storageServiceConfig.SetHDDMergedChannelPoolKind("pool-kind-1");
    storageServiceConfig.SetHDDFreshChannelPoolKind("pool-kind-1");

    storageServiceConfig.SetSSDSystemChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetSSDLogChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetSSDIndexChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetSSDMixedChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetSSDMergedChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetSSDFreshChannelPoolKind("pool-kind-2");

    storageServiceConfig.SetHybridSystemChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetHybridLogChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetHybridIndexChannelPoolKind("pool-kind-2");
    storageServiceConfig.SetHybridMixedChannelPoolKind("pool-kind-1");
    storageServiceConfig.SetHybridMergedChannelPoolKind("pool-kind-1");
    storageServiceConfig.SetHybridFreshChannelPoolKind("pool-kind-2");

    return std::make_unique<TStorageConfig>(
        storageServiceConfig,
        std::make_shared<NFeatures::TFeaturesConfig>(featuresConfig));
}

}   // namespace NCloud::NBlockStore::NStorage
