#include "shard_resize_companion.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TShardResizeCompanion::TShardResizeCompanion(
        TStorageConfigPtr storageConfig,
        NProto::TFileStorePerformanceProfile performanceProfile)
    : StorageConfig(std::move(storageConfig))
    , PerformanceProfile(std::move(performanceProfile))
    , ShardDescription(
          std::bind_front(&TShardResizeCompanion::DescribeShard, this),
          StorageConfig->GetMaxShardManagementRequestsInFlight())
    , ShardResize(
          std::bind_front(&TShardResizeCompanion::ResizeShard, this),
          StorageConfig->GetMaxShardManagementRequestsInFlight())
{}

void TShardResizeCompanion::InitializeShardResize(
    TVector<NKikimrFileStore::TConfig> shardConfigs)
{
    ShardConfigs = std::move(shardConfigs);
}

////////////////////////////////////////////////////////////////////////////////

void TShardResizeCompanion::StartShardDescription(const TActorContext& ctx)
{
    ShardDescription.StartRequests(ctx, 0, ShardConfigs.size());
}

void TShardResizeCompanion::DescribeShard(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        ShardConfigs[shardIndex].GetFileSystemId());

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), shardIndex);
}

NProto::TError TShardResizeCompanion::OnShardDescribed(
    const TActorContext& ctx,
    ui32 shardIndex,
    const NKikimrFileStore::TConfig& config)
{
    if (shardIndex >= ShardConfigs.size() || !HasShardsToDescribe()) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "unexpected description of shard " << shardIndex);
    }

    // The described config has the real channels, their calculated number may
    // be less.
    auto& shardConfig = ShardConfigs[shardIndex];
    const ui64 blocksCount = shardConfig.GetBlocksCount();
    shardConfig = config;
    shardConfig.SetBlocksCount(blocksCount);
    SetupFileStorePerformanceAndChannels(
        false /* allocateMixed0Channel */,
        *StorageConfig,
        shardConfig,
        PerformanceProfile);

    ShardDescription.OnResponse(ctx);
    return {};
}

bool TShardResizeCompanion::HasShardsToDescribe() const
{
    return ShardDescription.HasShardsLeft();
}

////////////////////////////////////////////////////////////////////////////////

void TShardResizeCompanion::StartShardResize(const TActorContext& ctx)
{
    ShardResize.StartRequests(ctx, 0, ShardConfigs.size());
}

void TShardResizeCompanion::ResizeShard(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    auto& shardConfig = ShardConfigs[shardIndex];
    shardConfig.ClearBlockSize();
    shardConfig.SetAlterTs(ctx.Now().MicroSeconds());
    auto request =
        std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(shardConfig);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), shardIndex);
}

NProto::TError TShardResizeCompanion::OnShardResized(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    if (shardIndex >= ShardConfigs.size() || !HasShardsToResize()) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "unexpected resize of shard " << shardIndex);
    }

    ShardResize.OnResponse(ctx);
    return {};
}

bool TShardResizeCompanion::HasShardsToResize() const
{
    return ShardResize.HasShardsLeft();
}

}   // namespace NCloud::NFileStore::NStorage
