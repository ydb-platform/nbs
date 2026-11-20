#include "shard_configuration_companion.h"

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TShardConfigurationCompanion::TShardConfigurationCompanion(
        ui32 maxRequestsInFlight)
    : NewShardConfiguration(
          std::bind_front(&TShardConfigurationCompanion::ConfigureShard, this),
          maxRequestsInFlight)
    , ExistingShardConfiguration(
          std::bind_front(&TShardConfigurationCompanion::ConfigureShard, this),
          maxRequestsInFlight)
{}

void TShardConfigurationCompanion::InitializeShardConfiguration(
    NProtoPrivate::TConfigureShardsRequest request,
    ui32 existingShardCount)
{
    MainFileStoreRequest = std::move(request);
    ExistingShardCount = existingShardCount;
}

////////////////////////////////////////////////////////////////////////////////

void TShardConfigurationCompanion::StartShardConfiguration(
    const TActorContext& ctx)
{
    // An existing shard gets the new shard list after the new shards are
    // configured, it may send them requests at once.
    NewShardConfiguration.StartRequests(
        ctx,
        ExistingShardCount,
        MainFileStoreRequest.ShardFileSystemIdsSize());
    if (!NewShardConfiguration.HasShardsLeft()) {
        ExistingShardConfiguration.StartRequests(ctx, 0, ExistingShardCount);
    }
}

void TShardConfigurationCompanion::ConfigureShard(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureAsShardRequest>();
    auto& record = request->Record;
    record.SetFileSystemId(
        MainFileStoreRequest.GetShardFileSystemIds(shardIndex));
    record.SetShardNo(shardIndex + 1);
    record.SetMainFileSystemId(MainFileStoreRequest.GetFileSystemId());
    record.SetDirectoryCreationInShardsEnabled(
        MainFileStoreRequest.GetDirectoryCreationInShardsEnabled());
    record.SetForceDirectoryCreationInShards(
        MainFileStoreRequest.GetForceDirectoryCreationInShards());
    record.SetStrictFileSystemSizeEnforcementEnabled(
        MainFileStoreRequest.GetStrictFileSystemSizeEnforcementEnabled());

    if (record.GetDirectoryCreationInShardsEnabled() ||
        record.GetStrictFileSystemSizeEnforcementEnabled())
    {
        *record.MutableShardFileSystemIds() =
            MainFileStoreRequest.GetShardFileSystemIds();
        *record.MutableShardTopology() =
            MainFileStoreRequest.GetShardTopology();
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring shard %s",
        MainFileStoreRequest.GetFileSystemId().c_str(),
        record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request),
        shardIndex   // cookie
    );
}

NProto::TError TShardConfigurationCompanion::OnShardConfigured(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    if (shardIndex < ExistingShardCount &&
        ExistingShardConfiguration.HasShardsLeft())
    {
        ExistingShardConfiguration.OnResponse(ctx);
        return {};
    }

    if (shardIndex >= ExistingShardCount &&
        shardIndex < MainFileStoreRequest.ShardFileSystemIdsSize() &&
        NewShardConfiguration.HasShardsLeft())
    {
        NewShardConfiguration.OnResponse(ctx);
        if (!NewShardConfiguration.HasShardsLeft()) {
            ExistingShardConfiguration.StartRequests(
                ctx,
                0,
                ExistingShardCount);
        }
        return {};
    }

    return MakeError(
        E_INVALID_STATE,
        TStringBuilder() << "unexpected configuration of shard " << shardIndex);
}

bool TShardConfigurationCompanion::HasShardsToConfigure() const
{
    return NewShardConfiguration.HasShardsLeft() ||
           ExistingShardConfiguration.HasShardsLeft();
}

////////////////////////////////////////////////////////////////////////////////

void TShardConfigurationCompanion::ConfigureMainFileStore(
    const TActorContext& ctx) const
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureShardsRequest>();
    request->Record = MainFileStoreRequest;

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring main filesystem %s",
        MainFileStoreRequest.GetFileSystemId().c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

}   // namespace NCloud::NFileStore::NStorage
