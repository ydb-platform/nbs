#include "model.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/model/channel_data_kind.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <util/generic/ymath.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define THROTTLING_PARAM(paramName, returnType)                                \
    returnType paramName(                                                      \
        const TStorageConfig& config,                                          \
        const ui32 mediaKind)                                                  \
    {                                                                          \
        switch (mediaKind) {                                                   \
            case NCloud::NProto::STORAGE_MEDIA_SSD:                            \
                return config.GetSSD ## paramName();                           \
            default:                                                           \
                return config.GetHDD ## paramName();                           \
        }                                                                      \
    }                                                                          \
// THROTTLING_PARAM

THROTTLING_PARAM(ThrottlingEnabled, bool);
THROTTLING_PARAM(UnitReadBandwidth, ui64);
THROTTLING_PARAM(UnitWriteBandwidth, ui64);
THROTTLING_PARAM(UnitReadIops, ui64);
THROTTLING_PARAM(UnitWriteIops, ui64);
THROTTLING_PARAM(MaxReadBandwidth, ui64);
THROTTLING_PARAM(MaxWriteBandwidth, ui64);
THROTTLING_PARAM(MaxReadIops, ui64);
THROTTLING_PARAM(MaxWriteIops, ui64);
THROTTLING_PARAM(BoostTime, TDuration);
THROTTLING_PARAM(BoostRefillTime, TDuration);
THROTTLING_PARAM(UnitBoost, ui64);
THROTTLING_PARAM(BurstPercentage, ui64);
THROTTLING_PARAM(DefaultPostponedRequestWeight, ui64);
THROTTLING_PARAM(MaxPostponedWeight, ui64);
THROTTLING_PARAM(MaxWriteCostMultiplier, ui64);
THROTTLING_PARAM(MaxPostponedTime, TDuration);
THROTTLING_PARAM(MaxPostponedCount, ui64);

#undef THROTTLING_PARAM

ui64 MaxReadBandwidth(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const ui32 unitCount)
{
    const auto unitBandwidth = UnitReadBandwidth(
        config,
        fileStore.GetStorageMediaKind());
    const auto maxBandwidth = MaxReadBandwidth(
        config,
        fileStore.GetStorageMediaKind());

    return Max(
        static_cast<ui64>(fileStore.GetPerformanceProfileMaxReadBandwidth()),
        Min(maxBandwidth, unitCount * unitBandwidth) * 1_MB
    );
}

ui64 MaxWriteBandwidth(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const ui32 unitCount)
{
    const auto unitBandwidth = UnitWriteBandwidth(
        config,
        fileStore.GetStorageMediaKind());
    const auto maxBandwidth = MaxWriteBandwidth(
        config,
        fileStore.GetStorageMediaKind());

    auto fileStoreMaxWriteBandwidth =
        fileStore.GetPerformanceProfileMaxWriteBandwidth();
    if (!fileStoreMaxWriteBandwidth) {
        fileStoreMaxWriteBandwidth =
            fileStore.GetPerformanceProfileMaxReadBandwidth();
    }

   return Max(
        static_cast<ui64>(fileStoreMaxWriteBandwidth),
        Min(maxBandwidth, unitCount * unitBandwidth) * 1_MB
    );
}

ui32 MaxReadIops(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const ui32 unitCount)
{
    const auto unitIops = UnitReadIops(
        config,
        fileStore.GetStorageMediaKind());
    const auto maxIops = MaxReadIops(
        config,
        fileStore.GetStorageMediaKind());

    return Max(
        static_cast<ui64>(fileStore.GetPerformanceProfileMaxReadIops()),
        Min(maxIops, unitCount * unitIops)
    );
}

ui32 MaxWriteIops(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const ui32 unitCount)
{
    const auto unitIops = UnitWriteIops(
        config,
        fileStore.GetStorageMediaKind());
    const auto maxIops = MaxWriteIops(
        config,
        fileStore.GetStorageMediaKind());

    auto fileStoreMaxWriteIops = fileStore.GetPerformanceProfileMaxWriteIops();
    if (!fileStoreMaxWriteIops) {
        fileStoreMaxWriteIops = fileStore.GetPerformanceProfileMaxReadIops();
    }

    return Max(
        static_cast<ui64>(fileStoreMaxWriteIops),
        Min(maxIops, unitCount * unitIops)
    );
}

bool ThrottlingEnabled(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return ThrottlingEnabled(config, fileStore.GetStorageMediaKind());
}

ui32 BoostTime(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return BoostTime(config, fileStore.GetStorageMediaKind()).MilliSeconds();
}

ui32 BoostRefillTime(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return BoostRefillTime(
        config,
        fileStore.GetStorageMediaKind()).MilliSeconds();
}

ui32 BoostPercentage(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const ui32 unitCount)
{
    const auto unitBoost = UnitBoost(config, fileStore.GetStorageMediaKind());
    return static_cast<ui32>(100.0 * static_cast<double>(unitBoost) /
        static_cast<double>(unitCount));
}

ui32 BurstPercentage(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return BurstPercentage(config, fileStore.GetStorageMediaKind());
}

ui64 DefaultPostponedRequestWeight(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return DefaultPostponedRequestWeight(
        config,
        fileStore.GetStorageMediaKind());
}

ui64 MaxPostponedWeight(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return MaxPostponedWeight(config, fileStore.GetStorageMediaKind());
}

ui32 MaxWriteCostMultiplier(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return MaxWriteCostMultiplier(config, fileStore.GetStorageMediaKind());
}

ui32 MaxPostponedTime(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return MaxPostponedTime(
        config,
        fileStore.GetStorageMediaKind()).MilliSeconds();
}

ui32 MaxPostponedCount(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    return MaxPostponedCount(
        config,
        fileStore.GetStorageMediaKind());
}

auto GetAllocationUnit(
    const TStorageConfig& config,
    ui32 mediaKind)
{
    ui64 unit = 0;
    switch (mediaKind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD:
            unit = config.GetAllocationUnitSSD();
            break;

        default:
            unit = config.GetAllocationUnitHDD();
            break;
    }

    Y_ABORT_UNLESS(unit != 0);
    return unit;
}

////////////////////////////////////////////////////////////////////////////////

struct TPoolKinds
{
    TString System;
    TString Log;
    TString Index;
    TString Fresh;
    TString Mixed;
};

TPoolKinds GetPoolKinds(
    const TStorageConfig& config,
    ui32 mediaKind)
{
    switch (mediaKind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD:
            return {
                config.GetSSDSystemChannelPoolKind(),
                config.GetSSDLogChannelPoolKind(),
                config.GetSSDIndexChannelPoolKind(),
                config.GetSSDFreshChannelPoolKind(),
                config.GetSSDMixedChannelPoolKind(),
            };
        case NCloud::NProto::STORAGE_MEDIA_HYBRID:
            return {
                config.GetHybridSystemChannelPoolKind(),
                config.GetHybridLogChannelPoolKind(),
                config.GetHybridIndexChannelPoolKind(),
                config.GetHybridFreshChannelPoolKind(),
                config.GetHybridMixedChannelPoolKind(),
            };
        case NCloud::NProto::STORAGE_MEDIA_HDD:
        default:
            return {
                config.GetHDDSystemChannelPoolKind(),
                config.GetHDDLogChannelPoolKind(),
                config.GetHDDIndexChannelPoolKind(),
                config.GetHDDFreshChannelPoolKind(),
                config.GetHDDMixedChannelPoolKind(),
            };
    }
}

////////////////////////////////////////////////////////////////////////////////

ui32 ComputeAllocationUnitCount(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore)
{
    if (!fileStore.GetBlocksCount()) {
        return 1;
    }

    const double fileStoreSize =
        fileStore.GetBlocksCount() * fileStore.GetBlockSize() / double(1_GB);

    const auto unit = GetAllocationUnit(
        config,
        fileStore.GetStorageMediaKind());

    ui32 unitCount = std::ceil(fileStoreSize / unit);
    Y_DEBUG_ABORT_UNLESS(unitCount >= 1, "size %f unit %lu", fileStoreSize, unit);

    return unitCount;
}

ui32 ComputeMixedChannelCount(
    const TStorageConfig& config,
    const ui32 allocationUnitCount,
    const NKikimrFileStore::TConfig& fileStore)
{
    ui32 mixed = 0;
    for (const auto& channel: fileStore.GetExplicitChannelProfiles()) {
        if (channel.GetDataKind() == static_cast<ui32>(EChannelDataKind::Mixed)) {
            ++mixed;
        }
    }

    return Min(
        Max(
            allocationUnitCount,
            mixed,
            config.GetMinChannelCount()
        ),
        MaxChannelsCount
    );

}

void AddOrModifyChannel(
    const TString& poolKind,
    const ui32 channelId,
    const ui64 size,
    const EChannelDataKind dataKind,
    NKikimrFileStore::TConfig& config)
{
    while (channelId >= config.ExplicitChannelProfilesSize()) {
        config.AddExplicitChannelProfiles();
    }

    auto* profile = config.MutableExplicitChannelProfiles(channelId);
    if (profile->GetPoolKind().empty()) {
        profile->SetPoolKind(poolKind);
    }

    profile->SetDataKind(static_cast<ui32>(dataKind));
    profile->SetSize(size);
    profile->SetReadIops(config.GetPerformanceProfileMaxReadIops());
    profile->SetWriteIops(config.GetPerformanceProfileMaxWriteIops());
    profile->SetReadBandwidth(config.GetPerformanceProfileMaxReadBandwidth());
    profile->SetWriteBandwidth(config.GetPerformanceProfileMaxWriteBandwidth());
}

void SetupChannels(
    ui32 unitCount,
    bool allocateMixed0Channel,
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore)
{
    const auto unit = GetAllocationUnit(
        config,
        fileStore.GetStorageMediaKind());
    const auto poolKinds = GetPoolKinds(
        config,
        fileStore.GetStorageMediaKind());

    AddOrModifyChannel(
        poolKinds.System,
        0,
        128_MB,
        EChannelDataKind::System,
        fileStore
    );

    AddOrModifyChannel(
        poolKinds.Index,
        1,
        16_MB,
        EChannelDataKind::Index,
        fileStore
    );
    AddOrModifyChannel(
        poolKinds.Fresh,
        2,
        128_MB,
        EChannelDataKind::Fresh,
        fileStore
    );

    const ui32 mixed = ComputeMixedChannelCount(config, unitCount, fileStore);
    ui32 mixedChannelStart = 3;

    if (allocateMixed0Channel) {
        AddOrModifyChannel(
            poolKinds.Mixed,
            mixedChannelStart,
            unit * 1_GB,
            EChannelDataKind::Mixed0,
            fileStore
        );

        ++mixedChannelStart;
    }

    for (ui32 i = 0; i < mixed; ++i) {
        AddOrModifyChannel(
            poolKinds.Mixed,
            mixedChannelStart + i,
            unit * 1_GB,
            EChannelDataKind::Mixed,
            fileStore);
    }
}

void OverrideStorageMediaKind(
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore)
{
    using namespace ::NCloud::NProto;
    if (fileStore.GetStorageMediaKind() == STORAGE_MEDIA_HDD) {
        switch(static_cast<EStorageMediaKind>(config.GetHDDMediaKindOverride())) {
            case STORAGE_MEDIA_HYBRID:
                fileStore.SetStorageMediaKind(STORAGE_MEDIA_HYBRID);
                break;
            case STORAGE_MEDIA_SSD:
                fileStore.SetStorageMediaKind(STORAGE_MEDIA_SSD);
                break;
            default:
                break; // pass
        }
    }
}

ui32 NodesLimit(
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore)
{
    ui64 size = fileStore.GetBlocksCount() * fileStore.GetBlockSize();
    ui64 limit = Min(
        static_cast<ui64>(Max<ui32>()),
        size / config.GetSizeToNodesRatio());

    return Max(limit, static_cast<ui64>(config.GetDefaultNodesLimit()));
}

#define PERFORMANCE_PROFILE_PARAMETERS_SIMPLE(xxx, ...)                        \
    xxx(ThrottlingEnabled,                      __VA_ARGS__)                   \
    xxx(BoostTime,                              __VA_ARGS__)                   \
    xxx(BoostRefillTime,                        __VA_ARGS__)                   \
    xxx(BurstPercentage,                        __VA_ARGS__)                   \
    xxx(DefaultPostponedRequestWeight,          __VA_ARGS__)                   \
    xxx(MaxPostponedWeight,                     __VA_ARGS__)                   \
    xxx(MaxWriteCostMultiplier,                 __VA_ARGS__)                   \
    xxx(MaxPostponedTime,                       __VA_ARGS__)                   \
    xxx(MaxPostponedCount,                      __VA_ARGS__)                   \
// PERFORMANCE_PROFILE_PARAMETERS_SIMPLE

#define PERFORMANCE_PROFILE_PARAMETERS_AU(xxx, ...)                            \
    xxx(MaxReadIops,                            __VA_ARGS__)                   \
    xxx(MaxReadBandwidth,                       __VA_ARGS__)                   \
    xxx(MaxWriteIops,                           __VA_ARGS__)                   \
    xxx(MaxWriteBandwidth,                      __VA_ARGS__)                   \
    xxx(BoostPercentage,                        __VA_ARGS__)                   \
// PERFORMANCE_PROFILE_PARAMETERS_AU

}   // namespace

void SetupFileStorePerformanceAndChannels(
    bool allocateMixed0Channel,
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore,
    const NProto::TFileStorePerformanceProfile& clientProfile)
{
    ui32 allocationUnitCount = ComputeAllocationUnitCount(config, fileStore);
    OverrideStorageMediaKind(config, fileStore);

#define SETUP_PARAMETER_SIMPLE(name, ...)                                      \
    fileStore.SetPerformanceProfile##name(                                     \
        clientProfile.Get##name()                                              \
            ? clientProfile.Get##name()                                        \
            : name(config, fileStore));                                        \
// SETUP_PARAMETER_AU

#define SETUP_PARAMETER_AU(name, ...)                                          \
    fileStore.SetPerformanceProfile##name(                                     \
        clientProfile.Get##name()                                              \
            ? clientProfile.Get##name()                                        \
            : name(config, fileStore, allocationUnitCount));                   \
// SETUP_PARAMETER_SIMPLE

    PERFORMANCE_PROFILE_PARAMETERS_SIMPLE(SETUP_PARAMETER_SIMPLE);
    PERFORMANCE_PROFILE_PARAMETERS_AU(SETUP_PARAMETER_AU);

#undef SETUP_PARAMETER_SIMPLE
#undef SETUP_PARAMETER_AU

    fileStore.SetNodesCount(NodesLimit(config, fileStore));

    SetupChannels(
        allocationUnitCount,
        allocateMixed0Channel,
        config,
        fileStore);
}

////////////////////////////////////////////////////////////////////////////////

ui32 ComputeShardCount(
    const ui64 blocksCount,
    const ui64 shardAllocationUnitBlocks)
{
    if (blocksCount < shardAllocationUnitBlocks) {
        // No need in using sharding for small enough filesystems
        return 0;
    }

    const ui32 shardCount = CeilDiv(blocksCount, shardAllocationUnitBlocks);
    return Min(shardCount, MaxShardCount);
}

TMultiShardFileStoreConfig SetupMultiShardFileStorePerformanceAndChannels(
    const TStorageConfig& config,
    const NKikimrFileStore::TConfig& fileStore,
    const NProto::TFileStorePerformanceProfile& clientProfile,
    ui32 explicitShardCount)
{
    TMultiShardFileStoreConfig result;
    result.MainFileSystemConfig = fileStore;
    SetupFileStorePerformanceAndChannels(
        false, // allocateMixed0Channel
        config,
        result.MainFileSystemConfig,
        clientProfile);

    const auto shardCount = explicitShardCount
        ? explicitShardCount
        : ComputeShardCount(
                fileStore.GetBlocksCount(),
                config.GetShardAllocationUnitBlocks());
    result.ShardConfigs.resize(shardCount);
    for (ui32 i = 0; i < shardCount; ++i) {
        result.ShardConfigs[i] = fileStore;
        result.ShardConfigs[i].ClearVersion();
        result.ShardConfigs[i].SetBlocksCount(
            config.GetAutomaticallyCreatedShardBlocks());
        result.ShardConfigs[i].SetFileSystemId(
            Sprintf("%s_s%u", fileStore.GetFileSystemId().c_str(), i + 1));
        SetupFileStorePerformanceAndChannels(
            false, // allocateMixed0Channel
            config,
            result.ShardConfigs[i],
            clientProfile);
        result.ShardConfigs[i].SetIsSystem(true);
    }

    return result;
}

}   // namespace NCloud::NFileStore::NStorage
