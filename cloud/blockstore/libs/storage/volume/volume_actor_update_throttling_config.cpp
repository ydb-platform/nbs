#include "volume_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>

#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool SpecificDiskFilterApplies(
    const NProto::TSpecificDiskFilter& disks,
    const TString& diskId)
{
    const auto& diskIds = disks.GetDiskIds();
    return std::ranges::find(diskIds, diskId) != diskIds.end();
}

bool DiskFilterApplies(
    const NProto::TDiskFilter& filter,
    const TString& cloudId,
    const TString& folderId,
    const NProto::EStorageMediaKind& mediaKind)
{
    const auto& cloudIds = filter.GetCloudIds();
    if (!cloudIds.empty() &&
        std::ranges::find(cloudIds, cloudId) == cloudIds.end())
    {
        return false;
    }

    const auto& folderIds = filter.GetFolderIds();
    if (!folderIds.empty() &&
        std::ranges::find(folderIds, folderId) == folderIds.end())
    {
        return false;
    }

    const auto& mediaKinds = filter.GetMediaKinds();
    if (!mediaKinds.empty() &&
        std::ranges::find(mediaKinds, mediaKind) == mediaKinds.end())
    {
        return false;
    }

    return true;
}

using TThrottlingRule = ::google::protobuf::RepeatedPtrField<
    NCloud::NBlockStore::NProto::TThrottlingRule>;

auto GetPerformanceProfileCoefficientsForVolume(
    const TThrottlingRule& rules,
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId,
    const NProto::EStorageMediaKind& mediaKind)
    -> NProto::TVolumePerformanceProfileCoefficients
{
    NProto::TVolumePerformanceProfileCoefficients result;

    for (const auto& rule: rules) {
        switch (rule.GetSelectorCase()) {
            case NCloud::NBlockStore::NProto::TThrottlingRule::kDisks:
                if (SpecificDiskFilterApplies(rule.GetDisks(), diskId)) {
                    return rule.GetCoefficients();
                }
                break;
            case NCloud::NBlockStore::NProto::TThrottlingRule::kFilter:
                if (DiskFilterApplies(
                        rule.GetFilter(),
                        cloudId,
                        folderId,
                        mediaKind))
                {
                    result = rule.GetCoefficients();
                }
            default:
                break;
        }
    }

    return result;
}
}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleUpdateVolatileThrottlingConfig(
    const TEvThrottlingManager::TEvNotifyVolume::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& throttlingConfig = ev->Get()->Config;
    if (!StateLoadFinished) {
        // we'll get another notification soon
        return;
    }

    if (throttlingConfig.GetVersion() <=
        State->GetThrottlingPolicy().GetVolatileVersion())
    {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::VOLUME,
            "Got TThrottlingConfig version less then known: (%lu) <= (%lu): ",
            throttlingConfig.GetVersion(),
            State->GetThrottlingPolicy().GetVolatileVersion());
        return;
    }

    const auto& rules = throttlingConfig.GetRules();
    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();

    auto coefficients = GetPerformanceProfileCoefficientsForVolume(
        rules,
        volumeConfig.GetDiskId(),
        volumeConfig.GetCloudId(),
        volumeConfig.GetFolderId(),
        State->GetConfig().GetStorageMediaKind());
    auto throttlingConfigVersion = throttlingConfig.GetVersion();

    State->AccessThrottlingPolicy().Reset(
        coefficients,
        throttlingConfigVersion);
}

}   // namespace NCloud::NBlockStore::NStorage
