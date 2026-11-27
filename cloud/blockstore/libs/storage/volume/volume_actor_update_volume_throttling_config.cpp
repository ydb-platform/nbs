#include "volume_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>
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

    const auto incomingVersion = throttlingConfig.GetVersion();
    const auto currentVersion =
        State->GetThrottlingPolicy().GetVolatileThrottlingVersion();

    // Re-evaluate the rule for the previously accepted configuration version if
    // volume parameters used for rule selection have changed (e.g. FolderId).
    if (incomingVersion < currentVersion ||
        (incomingVersion == currentVersion &&
         !State->IsThrottlingRuleOutdated()))
    {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Ignore throttling config version %u (current version: %u).",
            LogTitle.GetWithTime().c_str(),
            incomingVersion,
            currentVersion);
        return;
    }

    auto error = ValidateThrottlingConfigRules(throttlingConfig.GetRules());
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Ignore invalid throttling config version %u: %s",
            LogTitle.GetWithTime().c_str(),
            incomingVersion,
            FormatError(error).c_str());
        return;
    }

    const auto& rules = throttlingConfig.GetRules();
    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();

    auto throttlingRule = GetThrottlingRuleForVolume(
        rules,
        GetLogicalDiskId(volumeConfig.GetDiskId()),
        volumeConfig.GetCloudId(),
        volumeConfig.GetFolderId(),
        State->GetConfig().GetStorageMediaKind());

    State->ResetThrottlingPolicy(throttlingRule, throttlingConfig.GetVersion());
}

}   // namespace NCloud::NBlockStore::NStorage
