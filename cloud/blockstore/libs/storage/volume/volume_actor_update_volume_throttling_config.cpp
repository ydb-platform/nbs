#include "volume_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/volume_throttling_manager/model/helpers.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateVolatileThrottlingConfig(
    const TEvVolumeThrottlingManager::TEvVolumeThrottlingConfigNotification::
        TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& throttlingConfig = ev->Get()->Config;
    if (!StateLoadFinished) {
        // we'll get another notification soon
        return;
    }

    if (throttlingConfig.GetVersion() <=
        State->GetThrottlingPolicy().GetVolatileThrottlingVersion())
    {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Got TVolumeThrottlingConfig version less than known: (%u) <= "
            "(%u): ",
            throttlingConfig.GetVersion(),
            State->GetThrottlingPolicy().GetVolatileThrottlingVersion());
        return;
    }

    auto error = ValidateThrottlingConfigRules(throttlingConfig.GetRules());
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            FormatError(error).c_str());
        return;
    }

    const auto& rules = throttlingConfig.GetRules();
    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();

    auto throttlingRule = GetThrottlingRuleForVolume(
        rules,
        volumeConfig.GetDiskId(),
        volumeConfig.GetCloudId(),
        volumeConfig.GetFolderId(),
        State->GetConfig().GetStorageMediaKind());

    State->ResetThrottlingPolicy(throttlingRule, throttlingConfig.GetVersion());
}

}   // namespace NCloud::NBlockStore::NStorage
