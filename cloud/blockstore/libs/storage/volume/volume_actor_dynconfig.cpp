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

// Refresh the global source, merge its patch and notify consumers on change.
void TVolumeActor::ApplyDynamicConfigUpdate(
    const IBlockstoreConfigProviderPtr& configProvider,
    const TActorContext& ctx)
{
    GlobalStorageConfig = configProvider->Get()->GetStorageConfig();
    if (ApplyStorageConfig()) {
        DynamicConfigRevision++;
        SendDynamicConfigUpdateToPartitions(ctx);
    }
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

// Deliver the effective snapshot to direct consumers and their owned children.
void TVolumeActor::SendDynamicConfigUpdateToPartitions(const TActorContext& ctx)
{
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
