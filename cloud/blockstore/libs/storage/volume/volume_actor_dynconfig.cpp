#include "volume_actor.h"

#include <cloud/blockstore/libs/config/blockstore_config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleConfigSubscriptionResponse(
    const TEvConfigsManager::TEvSetConfigSubscriptionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ApplyDynamicConfigUpdate(ev->Get()->Config, ctx);
}

void TVolumeActor::HandleDynamicConfigUpdate(
    const TEvConfigsManager::TEvConfigChanged::TPtr& ev,
    const TActorContext& ctx)
{
    ApplyDynamicConfigUpdate(ev->Get()->Config, ctx);
}

void TVolumeActor::ApplyDynamicConfigUpdate(
    const IBlockstoreConfigConstPtr& config,
    const TActorContext& ctx)
{
    // Save the received configuration even if the per-volume patch hides
    // its changes. Later patch updates must use this configuration.
    GlobalStorageConfig = config->GetStorageConfig();
    if (ApplyStorageConfig()) {
        DynamicConfigRevision++;
        SendDynamicConfigUpdateToChildActors(ctx);
    }
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

void TVolumeActor::SendDynamicConfigUpdateToChildActors(const TActorContext& ctx)
{
    // Send only after an effective dynamic update and once Volume is working.
    // Partition startup paths call this method again to update new child actors.
    if (!State || !DynamicConfigRevision || CurrentState != STATE_WORK) {
        return;
    }

    for (const auto actorId: State->GetDynamicConfigUpdateRecipients()) {
        ctx.Send(
            actorId,
            new TEvVolume::TEvDynamicConfigUpdate(
                Config, SelfId(), DynamicConfigRevision));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
