#include "volume_model.h"

#include "config.h"

#include <cloud/storage/core/libs/common/media.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/size_literals.h>
#include <util/generic/ymath.h>

#include <cmath>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define THROTTLING_PARAM(paramName)                                            \
    ui64 paramName(                                                            \
        const TStorageConfig& config,                                          \
        const NCloud::NProto::EStorageMediaKind mediaKind)                     \
    {                                                                          \
        switch (mediaKind) {                                                   \
            case NCloud::NProto::STORAGE_MEDIA_SSD:                            \
                return config.GetSSD ## paramName();                           \
            case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED:              \
                return config.GetNonReplicatedSSD ## paramName();              \
            case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED:              \
                return config.GetNonReplicatedHDD ## paramName();              \
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2:                    \
                return config.GetMirror2SSD ## paramName();                    \
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3:                    \
                return config.GetMirror3SSD ## paramName();                    \
            default:                                                           \
                return config.GetHDD ## paramName();                           \
        }                                                                      \
    }                                                                          \
// THROTTLING_PARAM

THROTTLING_PARAM(UnitReadBandwidth);
THROTTLING_PARAM(UnitWriteBandwidth);
THROTTLING_PARAM(UnitReadIops);
THROTTLING_PARAM(UnitWriteIops);
THROTTLING_PARAM(MaxReadBandwidth);
THROTTLING_PARAM(MaxWriteBandwidth);
THROTTLING_PARAM(MaxReadIops);
THROTTLING_PARAM(MaxWriteIops);

#undef THROTTLING_PARAM

ui64 ReadBandwidth(
    const TStorageConfig& config,
    const ui32 unitCount,
    const TVolumeParams& volumeParams)
{
    const auto unitBandwidth = UnitReadBandwidth(config, volumeParams.MediaKind);
    const auto maxBandwidth = MaxReadBandwidth(config, volumeParams.MediaKind);
    return Max(
        static_cast<ui64>(volumeParams.MaxReadBandwidth),
        Min(maxBandwidth, unitCount * unitBandwidth) * 1_MB
    );
}

ui64 WriteBandwidth(
    const TStorageConfig& config,
    const ui32 unitCount,
    const TVolumeParams& volumeParams)
{
    const auto unitBandwidth = UnitWriteBandwidth(config, volumeParams.MediaKind);
    const auto maxBandwidth = MaxWriteBandwidth(config, volumeParams.MediaKind);
    const auto volumeMaxWriteBandwidth = volumeParams.MaxWriteBandwidth
        ? volumeParams.MaxWriteBandwidth : volumeParams.MaxReadBandwidth;
    return Max(
        static_cast<ui64>(volumeMaxWriteBandwidth),
        Min(maxBandwidth, unitCount * unitBandwidth) * 1_MB
    );
}

ui32 ReadIops(
    const TStorageConfig& config,
    const ui32 unitCount,
    const TVolumeParams& volumeParams)
{
    const auto unitIops = UnitReadIops(config, volumeParams.MediaKind);
    const auto maxIops = MaxReadIops(config, volumeParams.MediaKind);
    return Max(
        static_cast<ui64>(volumeParams.MaxReadIops),
        Min(maxIops, unitCount * unitIops)
    );
}

ui32 WriteIops(
    const TStorageConfig& config,
    const ui32 unitCount,
    const TVolumeParams& volumeParams)
{
    const auto unitIops = UnitWriteIops(config, volumeParams.MediaKind);
    const auto maxIops = MaxWriteIops(config, volumeParams.MediaKind);
    const auto volumeMaxWriteIops = volumeParams.MaxWriteIops
        ? volumeParams.MaxWriteIops : volumeParams.MaxReadIops;
    return Max(
        static_cast<ui64>(volumeMaxWriteIops),
        Min(maxIops, unitCount * unitIops)
    );
}

bool GetThrottlingEnabled(
    const TStorageConfig& config,
    const TVolumeParams& volumeParams)
{
    switch (volumeParams.MediaKind) {
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_LOCAL:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD_LOCAL:
            return false;
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD:
            return config.GetThrottlingEnabledSSD();

        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD_NONREPLICATED:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_MIRROR2:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_MIRROR3:
            return true;

        default:
            return config.GetThrottlingEnabled();
    }
}

auto GetThrottlingBoostUnits(
    const TStorageConfig& config,
    const TVolumeParams& volumeParams)
{
    switch (volumeParams.MediaKind) {
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD:
            return config.GetThrottlingSSDBoostUnits();

        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_LOCAL:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD_LOCAL:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD_NONREPLICATED:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_MIRROR2:
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_MIRROR3:
            return 0u;

        default:
            return config.GetThrottlingHDDBoostUnits();
    }
}

void AddOrModifyChannel(
    const TString& poolKind,
    const ui32 channelId,
    const ui64 size,
    const EChannelDataKind dataKind,
    NKikimrBlockStore::TVolumeConfig& volumeConfig,
    const NPrivateProto::TVolumeChannelsToPoolsKinds& volumeChannelsToPoolsKinds)
{
    while (channelId >= volumeConfig.ExplicitChannelProfilesSize()) {
        volumeConfig.AddExplicitChannelProfiles();
    }
    auto* profile = volumeConfig.MutableExplicitChannelProfiles(channelId);

    const auto& data = volumeChannelsToPoolsKinds.GetData();
    const auto found = data.find(channelId);
    if (found != data.end()) {
        profile->SetPoolKind(found->second);
    } else if (profile->GetPoolKind().empty()) {
        profile->SetPoolKind(poolKind);
    }

    profile->SetDataKind(static_cast<ui32>(dataKind));
    profile->SetSize(size);
    profile->SetReadIops(volumeConfig.GetPerformanceProfileMaxReadIops());
    profile->SetReadBandwidth(volumeConfig.GetPerformanceProfileMaxReadBandwidth());
    profile->SetWriteIops(volumeConfig.GetPerformanceProfileMaxWriteIops());
    profile->SetWriteBandwidth(volumeConfig.GetPerformanceProfileMaxWriteBandwidth());
}

void SetupVolumeChannel(
    const TString& poolKind,
    const ui32 channelId,
    const ui64 size,
    const EChannelDataKind dataKind,
    NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    while (channelId >= volumeConfig.VolumeExplicitChannelProfilesSize()) {
        volumeConfig.AddVolumeExplicitChannelProfiles();
    }
    auto* profile = volumeConfig.MutableVolumeExplicitChannelProfiles(channelId);

    if (profile->GetPoolKind().empty()) {
        profile->SetPoolKind(poolKind);
    }
    profile->SetDataKind(static_cast<ui32>(dataKind));
    profile->SetSize(size);
    profile->SetReadIops(1);
    profile->SetReadBandwidth(1_MB);
    profile->SetWriteIops(1);
    profile->SetWriteBandwidth(1_MB);
}

struct TPoolKinds
{
    TString System;
    TString Log;
    TString Index;
    TString Mixed;
    TString Merged;
    TString Fresh;
};

TPoolKinds GetPoolKinds(
    const TStorageConfig& config,
    const NCloud::NProto::EStorageMediaKind mediaKind,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId)
{
    switch (mediaKind) {
        case NCloud::NProto::STORAGE_MEDIA_HDD: {
            return {
                config.GetHDDSystemChannelPoolKind(),
                config.GetHDDLogChannelPoolKind(),
                config.GetHDDIndexChannelPoolKind(),
                config.GetHDDMixedChannelPoolKind(),
                config.GetHDDMergedChannelPoolKind(),
                config.GetHDDFreshChannelPoolKind()
            };
        }

        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            auto systemChannelPoolKind =
                config.GetSSDSystemChannelPoolKindFeatureValue(
                    cloudId, folderId, diskId);
            if (systemChannelPoolKind.empty()) {
                systemChannelPoolKind = config.GetSSDSystemChannelPoolKind();
            }
            auto logChannelPoolKind =
                config.GetSSDLogChannelPoolKindFeatureValue(
                    cloudId, folderId, diskId);
            if (logChannelPoolKind.empty()) {
                logChannelPoolKind = config.GetSSDLogChannelPoolKind();
            }
            auto indexChannelPoolKind =
                config.GetSSDIndexChannelPoolKindFeatureValue(
                    cloudId, folderId, diskId);
            if (indexChannelPoolKind.empty()) {
                indexChannelPoolKind = config.GetSSDIndexChannelPoolKind();
            }
            auto freshChannelPoolKind =
                config.GetSSDFreshChannelPoolKindFeatureValue(
                    cloudId, folderId, diskId);
            if (freshChannelPoolKind.empty()) {
                freshChannelPoolKind = config.GetSSDFreshChannelPoolKind();
            }
            return {
                std::move(systemChannelPoolKind),
                std::move(logChannelPoolKind),
                std::move(indexChannelPoolKind),
                config.GetSSDMixedChannelPoolKind(),
                config.GetSSDMergedChannelPoolKind(),
                std::move(freshChannelPoolKind)
            };
        }

        default: {
            return {
                config.GetHybridSystemChannelPoolKind(),
                config.GetHybridLogChannelPoolKind(),
                config.GetHybridIndexChannelPoolKind(),
                config.GetHybridMixedChannelPoolKind(),
                config.GetHybridMergedChannelPoolKind(),
                config.GetHybridFreshChannelPoolKind()
            };
        }
    }
}

void SetExplicitChannelProfiles(
    const TStorageConfig& config,
    ui64 diskSize,
    int mergedChannels,
    int mixedChannels,
    int freshChannels,
    const NCloud::NProto::EStorageMediaKind mediaKind,
    const TVector<TChannelInfo>& existingDataChannels,
    NKikimrBlockStore::TVolumeConfig& volumeConfig,
    const NPrivateProto::TVolumeChannelsToPoolsKinds& volumeChannelsToPoolsKinds)
{
    const auto unit = GetAllocationUnit(config, mediaKind);
    const auto poolKinds = GetPoolKinds(
        config,
        mediaKind,
        volumeConfig.GetCloudId(),
        volumeConfig.GetFolderId(),
        volumeConfig.GetDiskId());
    const ui64 freshChannelSize = 128_MB;

    AddOrModifyChannel(
        poolKinds.System,
        0,
        128_MB,
        EChannelDataKind::System,
        volumeConfig,
        volumeChannelsToPoolsKinds
    );
    AddOrModifyChannel(
        poolKinds.Log,
        1,
        freshChannelSize,
        EChannelDataKind::Log,
        volumeConfig,
        volumeChannelsToPoolsKinds
    );
    AddOrModifyChannel(
        poolKinds.Index,
        2,
        Max(16_MB, diskSize / 1024),
        EChannelDataKind::Index,
        volumeConfig,
        volumeChannelsToPoolsKinds
    );

    ui32 c = 3;
    while (c - 3 < existingDataChannels.size()) {
        TString poolKind;
        ui64 channelSize = unit;
        const auto dataKind = existingDataChannels[c - 3].DataKind;
        switch (dataKind) {
            case EChannelDataKind::Merged: {
                poolKind = poolKinds.Merged;
                --mergedChannels;
                break;
            }

            case EChannelDataKind::Mixed: {
                poolKind = poolKinds.Mixed;
                --mixedChannels;
                break;
            }

            case EChannelDataKind::Fresh: {
                poolKind = poolKinds.Fresh;
                channelSize = freshChannelSize;
                --freshChannels;
                break;
            }

            default: {
                Y_DEBUG_ABORT_UNLESS(0);
            }
        }

        if (!config.GetPoolKindChangeAllowed()) {
            poolKind = existingDataChannels[c - 3].PoolKind;
        }

        AddOrModifyChannel(
            poolKind,
            c,
            channelSize,
            dataKind,
            volumeConfig,
            volumeChannelsToPoolsKinds
        );

        ++c;
    }

    ComputeChannelCountLimits(
        255 - c,
        &mergedChannels,
        &mixedChannels,
        &freshChannels);

    Y_DEBUG_ABORT_UNLESS(
        c + mergedChannels + mixedChannels + freshChannels <= 255);

    while (mergedChannels > 0) {
        AddOrModifyChannel(
            poolKinds.Merged,
            c,
            unit,
            EChannelDataKind::Merged,
            volumeConfig,
            volumeChannelsToPoolsKinds
        );

        --mergedChannels;
        ++c;
    }

    while (mixedChannels > 0) {
        AddOrModifyChannel(
            poolKinds.Mixed,
            c,
            unit,
            EChannelDataKind::Mixed,
            volumeConfig,
            volumeChannelsToPoolsKinds
        );

        --mixedChannels;
        ++c;
    }

    while (freshChannels > 0) {
        AddOrModifyChannel(
            poolKinds.Fresh,
            c,
            freshChannelSize,
            EChannelDataKind::Fresh,
            volumeConfig,
            volumeChannelsToPoolsKinds
        );

        --freshChannels;
        ++c;
    }
}

void SetVolumeExplicitChannelProfiles(
    const TStorageConfig& config,
    NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    const auto poolKinds = GetPoolKinds(
        config,
        NCloud::NProto::STORAGE_MEDIA_SSD,
        volumeConfig.GetCloudId(),
        volumeConfig.GetFolderId(),
        volumeConfig.GetDiskId());

    SetupVolumeChannel(
        poolKinds.System,
        0,
        1_MB,
        EChannelDataKind::System,
        volumeConfig
    );
    SetupVolumeChannel(
        poolKinds.Log,
        1,
        1_MB,
        EChannelDataKind::Log,
        volumeConfig
    );
    SetupVolumeChannel(
        poolKinds.Index,
        2,
        1_MB,
        EChannelDataKind::Index,
        volumeConfig
    );
}

ui32 ComputeAllocationUnitCount(
    const TStorageConfig& config,
    const TVolumeParams& volumeParams)
{
    if (!volumeParams.GetBlocksCount()) {
        return 1;
    }

    double volumeSize = volumeParams.GetBlocksCount() * volumeParams.BlockSize;

    const auto unit = GetAllocationUnit(config, volumeParams.MediaKind);

    ui32 unitCount = std::ceil(volumeSize / unit);
    Y_DEBUG_ABORT_UNLESS(unitCount >= 1);

    return unitCount;
}

ui32 GetExistingMergedChannelCount(
    const TVolumeParams& volumeParams)
{
    ui32 existingMergedChannelCount = 0;
    for (const auto& dc: volumeParams.DataChannels) {
        if (dc.DataKind == EChannelDataKind::Merged) {
            ++existingMergedChannelCount;
        }
    }
    return existingMergedChannelCount;
}

ui32 ComputeMergedChannelCount(
    const ui32 allocationUnitCount,
    const ui32 existingMergedChannelCount,
    const TStorageConfig& config,
    const TVolumeParams& volumeParams)
{
    const ui32 calculatedMergedChannelCount = static_cast<ui32>(
        ceil(allocationUnitCount / volumeParams.PartitionsCount));
    return Min(
        Max(calculatedMergedChannelCount,
            existingMergedChannelCount,
            config.GetMinChannelCount()),
        MaxMergedChannelCount);
}

void SetupChannels(
    ui32 allocationUnitCount,
    const TStorageConfig& config,
    const TVolumeParams& volumeParams,
    const NProto::TResizeVolumeRequestFlags& flags,
    NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    const auto existingMergedChannelCount =
        GetExistingMergedChannelCount(volumeParams);
    ui32 mergedChannelCount = ComputeMergedChannelCount(
        allocationUnitCount,
        existingMergedChannelCount,
        config,
        volumeParams);
    ui32 mixedChannelCount = 0;

    ui32 freshChannelCount = config.GetFreshChannelCount();
    const bool isFreshChannelEnabled =
        config.IsAllocateFreshChannelFeatureEnabled(
            volumeConfig.GetCloudId(),
            volumeConfig.GetFolderId(),
            volumeConfig.GetDiskId());

    if (isFreshChannelEnabled || volumeConfig.GetTabletVersion() == 2) {
        freshChannelCount = 1;
    }

    if (volumeParams.MediaKind == NCloud::NProto::STORAGE_MEDIA_HYBRID &&
        !flags.GetNoSeparateMixedChannelAllocation() &&
        config.GetAllocateSeparateMixedChannels())
    {
        if (const auto mixedPercentage = config.GetMixedChannelsPercentageFromMerged();
            mixedPercentage == 0)
        {
            auto iopsFactor = volumeConfig.GetPerformanceProfileMaxWriteIops() /
                              double(config.GetSSDUnitWriteIops()) /
                              double(volumeParams.PartitionsCount);
            auto bandwidthFactor =
                volumeConfig.GetPerformanceProfileMaxWriteBandwidth() /
                double(config.GetSSDUnitWriteBandwidth() * 1_MB) /
                double(volumeParams.PartitionsCount);

            mixedChannelCount = ceil(Max(iopsFactor, bandwidthFactor));
            if (!mixedChannelCount) {
                mixedChannelCount = 1;
            }
        } else {
            mixedChannelCount =
                ceil((mergedChannelCount * mixedPercentage) / 100.f);
        }
    }

    SetExplicitChannelProfiles(
        config,
        volumeParams.BlocksCountPerPartition * volumeParams.BlockSize,
        mergedChannelCount,
        mixedChannelCount,
        freshChannelCount,
        volumeParams.MediaKind,
        volumeParams.DataChannels,
        volumeConfig,
        volumeParams.VolumeChannelsToPoolsKinds
    );

    if (config.GetPoolKindChangeAllowed()) {
        volumeConfig.SetPoolKindChangeAllowed(true);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ui64 ComputeBlocksCountPerPartition(
    const ui64 newBlocksCountPerVolume,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount)
{
    if (!blocksPerStripe) {
        Y_ABORT_UNLESS(partitionsCount == 1);
        return newBlocksCountPerVolume;
    }

    if (partitionsCount == 1) {
        return newBlocksCountPerVolume;
    }

    const double stripes =
        ceil(double(newBlocksCountPerVolume) / blocksPerStripe);
    const double stripesPerPartition = ceil(stripes / partitionsCount);
    return stripesPerPartition * blocksPerStripe;
}

ui64 ComputeBlocksCountPerPartition(
    const ui64 newBlocksCountPerVolume,
    const ui32 bytesPerStripe,
    const ui32 blockSize,
    const ui32 partitionsCount)
{
    return ComputeBlocksCountPerPartition(
        newBlocksCountPerVolume,
        ceil(double(bytesPerStripe) / blockSize),
        partitionsCount
    );
}

TPartitionsInfo ComputePartitionsInfo(
    const TStorageConfig& config,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    NCloud::NProto::EStorageMediaKind mediaKind,
    ui64 blocksCount,
    ui32 blockSize,
    bool isSystem,
    bool isOverlayDisk)
{
    TPartitionsInfo info;

    const bool enabledForCloud =
        config.IsMultipartitionVolumesFeatureEnabled(cloudId, folderId, diskId);
    const ui64 bytesPerPartition = mediaKind == NCloud::NProto::STORAGE_MEDIA_SSD
        ? config.GetBytesPerPartitionSSD() : config.GetBytesPerPartition();
    const bool enabled = (enabledForCloud
        || config.GetMultipartitionVolumesEnabled())
        && bytesPerPartition;

    if (enabled && !isSystem && !isOverlayDisk) {
        const double blocksPerStripe =
            ceil(double(config.GetBytesPerStripe()) / blockSize);
        const double stripes = ceil(blocksCount / blocksPerStripe);
        const double blocksNeeded = stripes * blocksPerStripe;
        const double bytesNeeded = blocksNeeded * blockSize;

        ui64 partitionsCount = ceil(bytesNeeded / bytesPerPartition);

        if (partitionsCount > config.GetMaxPartitionsPerVolume()) {
            info.PartitionsCount = config.GetMaxPartitionsPerVolume();
        } else {
            info.PartitionsCount = partitionsCount;
        }

        if (info.PartitionsCount > 1) {
            info.BlocksCountPerPartition = ComputeBlocksCountPerPartition(
                blocksNeeded,
                blocksPerStripe,
                info.PartitionsCount
            );

            return info;
        }
    }

    info.PartitionsCount = 1;
    info.BlocksCountPerPartition = blocksCount;
    return info;
}

////////////////////////////////////////////////////////////////////////////////

void ResizeVolume(
    const TStorageConfig& config,
    const TVolumeParams& volumeParams,
    const NProto::TResizeVolumeRequestFlags& flags,
    const NProto::TVolumePerformanceProfile& profile,
    NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    const auto allocationUnitCount =
        ComputeAllocationUnitCount(config, volumeParams);

    for (ui32 i = 0; i < volumeParams.PartitionsCount; ++i) {
        if (volumeConfig.PartitionsSize() == i) {
            volumeConfig.AddPartitions();
        }

        auto& partition = *volumeConfig.MutablePartitions(i);
        if (IsDiskRegistryMediaKind(volumeParams.MediaKind)) {
            partition.SetType(NKikimrBlockStore::EPartitionType::NonReplicated);
        }

        partition.SetBlockCount(volumeParams.BlocksCountPerPartition);
    }

    const auto suggestedReadBandwidth =
        ReadBandwidth(config, allocationUnitCount, volumeParams);
    const auto suggestedWriteBandwidth =
        WriteBandwidth(config, allocationUnitCount, volumeParams);
    const auto suggestedReadIops =
        ReadIops(config, allocationUnitCount, volumeParams);
    const auto suggestedWriteIops =
        WriteIops(config, allocationUnitCount, volumeParams);
    const auto suggestedBoostPercentage = 100
        * GetThrottlingBoostUnits(config, volumeParams)
        / double(allocationUnitCount);

    if (profile.GetMaxReadBandwidth()) {
        volumeConfig.SetPerformanceProfileMaxReadBandwidth(
            profile.GetMaxReadBandwidth());
    } else if (GetThrottlingEnabled(config, volumeParams)) {
        volumeConfig.SetPerformanceProfileMaxReadBandwidth(
            suggestedReadBandwidth);
    }

    if (profile.GetMaxWriteBandwidth()) {
        volumeConfig.SetPerformanceProfileMaxWriteBandwidth(
            profile.GetMaxWriteBandwidth());
    } else if (GetThrottlingEnabled(config, volumeParams)) {
        volumeConfig.SetPerformanceProfileMaxWriteBandwidth(
            suggestedWriteBandwidth);
    }

    if (profile.GetMaxReadIops()) {
        volumeConfig.SetPerformanceProfileMaxReadIops(
            profile.GetMaxReadIops());
    } else if (GetThrottlingEnabled(config, volumeParams)) {
        volumeConfig.SetPerformanceProfileMaxReadIops(
            suggestedReadIops);
    }

    if (profile.GetMaxWriteIops()) {
        volumeConfig.SetPerformanceProfileMaxWriteIops(
            profile.GetMaxWriteIops());
    } else if (GetThrottlingEnabled(config, volumeParams)) {
        volumeConfig.SetPerformanceProfileMaxWriteIops(
            suggestedWriteIops);
    }

    if (profile.GetBurstPercentage()) {
        volumeConfig.SetPerformanceProfileBurstPercentage(
            profile.GetBurstPercentage());
    } else if (config.GetThrottlingBurstPercentage()) {
        volumeConfig.SetPerformanceProfileBurstPercentage(
            config.GetThrottlingBurstPercentage());
    }

    if (profile.GetMaxPostponedWeight()) {
        volumeConfig.SetPerformanceProfileMaxPostponedWeight(
            profile.GetMaxPostponedWeight());
    } else if (config.GetThrottlingMaxPostponedWeight()) {
        volumeConfig.SetPerformanceProfileMaxPostponedWeight(
            config.GetThrottlingMaxPostponedWeight());
    }

    if (profile.GetBoostTime()) {
        volumeConfig.SetPerformanceProfileBoostTime(profile.GetBoostTime());
    } else if (config.GetThrottlingBoostTime().GetValue()) {
        volumeConfig.SetPerformanceProfileBoostTime(
            config.GetThrottlingBoostTime().MilliSeconds());
    }

    if (profile.GetBoostRefillTime()) {
        volumeConfig.SetPerformanceProfileBoostRefillTime(
            profile.GetBoostRefillTime());
    } else if (config.GetThrottlingBoostRefillTime().GetValue()) {
        volumeConfig.SetPerformanceProfileBoostRefillTime(
            config.GetThrottlingBoostRefillTime().MilliSeconds());
    }

    if (profile.GetBoostPercentage()) {
        volumeConfig.SetPerformanceProfileBoostPercentage(
            profile.GetBoostPercentage());
    } else {
        volumeConfig.SetPerformanceProfileBoostPercentage(
            suggestedBoostPercentage);
    }

    const bool throttlingSetUpForVolume =
        volumeConfig.GetPerformanceProfileMaxReadIops();
    const bool throttlingExplicitlyDisabled =
        profile.GetMaxReadIops() && !profile.GetThrottlingEnabled();
    if (throttlingSetUpForVolume
            && GetThrottlingEnabled(config, volumeParams)
            && !throttlingExplicitlyDisabled)
    {
        volumeConfig.SetPerformanceProfileThrottlingEnabled(true);
    } else {
        volumeConfig.SetPerformanceProfileThrottlingEnabled(
            profile.GetThrottlingEnabled());
    }

    if (!IsDiskRegistryMediaKind(volumeParams.MediaKind)) {
        SetupChannels(
            allocationUnitCount,
            config,
            volumeParams,
            flags,
            volumeConfig
        );
    }

    SetVolumeExplicitChannelProfiles(config, volumeConfig);
}

////////////////////////////////////////////////////////////////////////////////

bool SetMissingParams(
    const TVolumeParams& volumeParams,
    const NKikimrBlockStore::TVolumeConfig& prevConfig,
    NKikimrBlockStore::TVolumeConfig& update)
{
    Y_UNUSED(volumeParams);
    Y_UNUSED(prevConfig);
    Y_UNUSED(update);

    return false;
}

////////////////////////////////////////////////////////////////////////////////

ui64 ComputeMaxBlocks(
    const TStorageConfig& config,
    const NCloud::NProto::EStorageMediaKind mediaKind,
    ui32 currentPartitions)
{
    if (IsDiskRegistryMediaKind(mediaKind)) {
        return MaxVolumeBlocksCount;
    }

    if (currentPartitions) {
        if (currentPartitions == 1) {
            return MaxPartitionBlocksCount;
        }

        return MaxPartitionBlocksCountForMultipartitionVolume
            * currentPartitions;
    }

    if (config.GetMultipartitionVolumesEnabled()) {
        return MaxPartitionBlocksCountForMultipartitionVolume
            * config.GetMaxPartitionsPerVolume();
    }

    return MaxPartitionBlocksCount;
}

////////////////////////////////////////////////////////////////////////////////

void ComputeChannelCountLimits(
    int freeChannelCount,
    int* wantToAddMerged,
    int* wantToAddMixed,
    int* wantToAddFresh)
{
    int* wantToAddByKind[] = {
        wantToAddMerged,
        wantToAddMixed,
        wantToAddFresh,
    };

    if (!freeChannelCount) {
        // There are no free channels.
        for (auto& wantToAdd: wantToAddByKind) {
            *wantToAdd = 0;
        }
        return;
    }

    auto totalWantToAddCalculator = [&]() -> int
    {
        return Accumulate(
            wantToAddByKind,
            int{},
            [](int acc, const int* wantToAdd) { return acc + *wantToAdd; });
    };

    auto totalWantToAdd = totalWantToAddCalculator();
    if (totalWantToAdd <= freeChannelCount) {
        // There are enough free channels to satisfy everyone.
        return;
    }

    // Distribute free channels among all channel kinds.
    for (auto& wantToAdd: wantToAddByKind) {
        double trimmedWantToAdd =
            static_cast<double>(*wantToAdd) * freeChannelCount / totalWantToAdd;
        if (trimmedWantToAdd > 0.0 && trimmedWantToAdd < 1.0) {
            *wantToAdd = 1;
        } else {
            *wantToAdd = std::round(trimmedWantToAdd);
        }
    }

    // If there are still more channels being created than there are free ones,
    // then we take them away from the most greedy channel kind.
    while (totalWantToAddCalculator() > freeChannelCount) {
        Sort(
            std::begin(wantToAddByKind),
            std::end(wantToAddByKind),
            [](const int* lhs, const int* rhs) { return *lhs > *rhs; });
        --(*wantToAddByKind[0]);
        Y_DEBUG_ABORT_UNLESS(*wantToAddByKind[0] >= 0);
    }
}

TVolumeParams ComputeVolumeParams(
    const TStorageConfig& config,
    ui32 blockSize,
    ui64 blocksCount,
    NCloud::NProto::EStorageMediaKind mediaKind,
    ui32 partitionsCount,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    bool isSystem,
    bool isOverlayDisk)
{
    TVolumeParams volumeParams;
    volumeParams.BlockSize = blockSize;
    volumeParams.MediaKind = mediaKind;
    if (!IsDiskRegistryMediaKind(volumeParams.MediaKind)) {
        TPartitionsInfo partitionsInfo;
        if (partitionsCount) {
            partitionsInfo.PartitionsCount = partitionsCount;
            partitionsInfo.BlocksCountPerPartition =
                ComputeBlocksCountPerPartition(
                    blocksCount,
                    config.GetBytesPerStripe(),
                    blockSize,
                    partitionsInfo.PartitionsCount);
        } else {
            partitionsInfo = ComputePartitionsInfo(
                config,
                cloudId,
                folderId,
                diskId,
                mediaKind,
                blocksCount,
                blockSize,
                isSystem,
                isOverlayDisk);
        }
        volumeParams.PartitionsCount = partitionsInfo.PartitionsCount;
        volumeParams.BlocksCountPerPartition =
            partitionsInfo.BlocksCountPerPartition;
    } else {
        volumeParams.BlocksCountPerPartition = blocksCount;
        volumeParams.PartitionsCount = 1;
    }
    return volumeParams;
}

}   // namespace NCloud::NBlockStore::NStorage
