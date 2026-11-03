#include "volume_actor.h"

#include <cloud/blockstore/libs/config/blockstore_config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleConfigSubscriptionResponse(
    const TEvConfigsManager::TEvSetConfigSubscriptionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ApplyDynamicConfigUpdate(ev->Get()->ConfigProvider, ctx);
}

void TVolumeActor::HandleDynamicConfigUpdate(
    const TEvConfigsManager::TEvConfigChanged::TPtr& ev,
    const TActorContext& ctx)
{
    ApplyDynamicConfigUpdate(ev->Get()->ConfigProvider, ctx);
}

void TVolumeActor::ApplyDynamicConfigUpdate(
    const IBlockstoreConfigProviderPtr& configProvider,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    // Save the latest global configuration even if the per-volume patch hides
    // its changes. Later patch updates must use these latest global values.
    GlobalStorageConfig = configProvider->Get()->GetStorageConfig();
    ApplyStorageConfig();
}

bool TVolumeActor::ApplyStorageConfig()
{
    // Apply the per-volume patch over the global configuration, retaining
    // shared live ICB controls. Compare the result with the current Config.
    auto effective =
        TStorageConfig::Merge(GlobalStorageConfig, StorageConfigPatch);
    HasStorageConfigPatch = effective != GlobalStorageConfig;
    if (Config->IsSame(*effective)) {
        return false;
    }

    // Replace configuration references without rebuilding Volume state or
    // settings already copied into policies and models.
    Config = std::move(effective);

    // Update the configuration of owned objects.
    if (State) {
        State->UpdateStorageConfig(Config);
    }
    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
