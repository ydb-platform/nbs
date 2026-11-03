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

// Refresh the global source and apply its per-volume patch.
void TVolumeActor::ApplyDynamicConfigUpdate(
    const IBlockstoreConfigProviderPtr& configProvider,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    GlobalStorageConfig = configProvider->Get()->GetStorageConfig();
    ApplyStorageConfig();
}

// Replace configuration references while preserving loaded Volume state.
bool TVolumeActor::ApplyStorageConfig()
{
    // Retain the newest global source even when the patch masks its changes.
    auto effective =
        TStorageConfig::Merge(GlobalStorageConfig, StorageConfigPatch);
    HasStorageConfigPatch = effective != GlobalStorageConfig;
    if (Config->IsSame(*effective)) {
        return false;
    }

    // Preserve clients, checkpoints and settings already copied into models.
    Config = std::move(effective);
    if (State) {
        State->UpdateStorageConfig(Config);
    }
    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
