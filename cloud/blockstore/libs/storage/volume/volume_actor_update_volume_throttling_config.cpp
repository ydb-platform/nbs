#include "volume_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/volume_throttling_manager/model/helpers.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>

#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool SpecificDiskFilterApplies(
    const NProto::TSpecificDiskFilter& filter,
    const TString& diskId)
{
    const auto& diskIds = filter.GetDiskIds();
    return Find(diskIds, diskId) != diskIds.end();
}

bool DiskFilterApplies(
    const NProto::TDiskFilter& filter,
    const TString& cloudId,
    const TString& folderId,
    const NProto::EStorageMediaKind& mediaKind)
{
    const auto& cloudIds = filter.GetCloudIds();
    if (!cloudIds.empty() && Find(cloudIds, cloudId) == cloudIds.end()) {
        return false;
    }

    const auto& folderIds = filter.GetFolderIds();
    if (!folderIds.empty() && Find(folderIds, folderId) == folderIds.end()) {
        return false;
    }

    const auto& mediaKinds = filter.GetMediaKinds();
    if (!mediaKinds.empty() && Find(mediaKinds, mediaKind) == mediaKinds.end())
    {
        return false;
    }

    return true;
}

using TVolumeThrottlingRule = ::google::protobuf::RepeatedPtrField<
    NCloud::NBlockStore::NProto::TVolumeThrottlingRule>;

auto GetThrottlingRuleForVolume(
    const TVolumeThrottlingRule& rules,
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    const NProto::EStorageMediaKind& mediaKind) -> NProto::TVolumeThrottlingRule
{
    const NProto::TVolumeThrottlingRule* resultPtr = nullptr;

    for (const auto& rule: rules) {
        switch (rule.GetSelectorCase()) {
            case NCloud::NBlockStore::NProto::TVolumeThrottlingRule::kDisks:
                if (SpecificDiskFilterApplies(rule.GetDisks(), diskId)) {
                    return rule;
                }
                break;
            case NCloud::NBlockStore::NProto::TVolumeThrottlingRule::kFilter:
                if (DiskFilterApplies(
                        rule.GetFilter(),
                        cloudId,
                        folderId,
                        mediaKind))
                {
                    // Pointer is used to avoid copying here
                    resultPtr = &rule;
                }
            default:
                break;
        }
    }

    return resultPtr ? *resultPtr : NProto::TVolumeThrottlingRule{};
}

}   // namespace

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
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Got TVolumeThrottlingConfig version less then known: (%lu) <= (%lu): ",
            throttlingConfig.GetVersion(),
            State->GetThrottlingPolicy().GetVolatileThrottlingVersion());
        return;
    }

    auto error = ValidateThrottlingConfig(throttlingConfig);
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
