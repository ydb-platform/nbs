#include "service_actor_fastshard.h"

#include "service_private.h"

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/disk_registry_proxy/api/service.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/string/builder.h>

#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAllocateFastShardsActor final
    : public TActorBootstrapped<TAllocateFastShardsActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TActorId Owner;
    const TString LogTag;
    const TVector<TString> ShardIds;
    const NProto::EFastShardStorageKind StorageKind;
    const TString CloudId;
    const TString FolderId;
    const ui32 BlockSize;

    NProtoPrivate::TShardTopology Topology;

    ui32 NextShard = 0;
    ui32 AllocatedShardCount = 0;

public:
    TAllocateFastShardsActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TString logTag,
        TVector<TString> shardIds,
        NProto::EFastShardStorageKind storageKind,
        TString cloudId,
        TString folderId,
        ui32 blockSize);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeNextShard(const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAllocateDevicesResponse(
        const TEvDiskRegistryProxy::TEvAllocateDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TAllocateFastShardsActor::TAllocateFastShardsActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TString logTag,
        TVector<TString> shardIds,
        NProto::EFastShardStorageKind storageKind,
        TString cloudId,
        TString folderId,
        ui32 blockSize)
    : StorageConfig(std::move(storageConfig))
    , Owner(owner)
    , LogTag(std::move(logTag))
    , ShardIds(std::move(shardIds))
    , StorageKind(storageKind)
    , CloudId(std::move(cloudId))
    , FolderId(std::move(folderId))
    , BlockSize(blockSize)
{}

void TAllocateFastShardsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (const auto& shardId: ShardIds) {
        AddFastShard(shardId, Topology);
    }

    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    while (NextShard < ShardIds.size() && (!limit || NextShard < limit)) {
        DescribeNextShard(ctx);
    }
}

void TAllocateFastShardsActor::DescribeNextShard(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
            ShardIds[NextShard]),
        NextShard   // cookie
    );

    ++NextShard;
}

void TAllocateFastShardsActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (ev->Cookie >= ShardIds.size()) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "invalid cookie: " << ev->Cookie));
        return;
    }

    const auto& shardId = ShardIds[ev->Cookie];
    const ui64 tabletId =
        msg->PathDescription.GetFileStoreDescription().GetIndexTabletId();
    if (!tabletId) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "no tablet id of " << shardId));
        return;
    }
    Topology.MutableShards(ev->Cookie)->SetTabletId(tabletId);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Allocating devices for %s",
        LogTag.c_str(),
        shardId.c_str());

    // A mirror writes two copies, a quorum needs three. The replica count
    // leaves out the main copy.
    auto mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR2;
    ui32 replicaCount = 1;
    if (StorageKind == NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR) {
        mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3;
        replicaCount = 2;
    }

    NCloud::Send(
        ctx,
        MakeFileStoreDiskRegistryProxyId(),
        std::make_unique<TEvDiskRegistryProxy::TEvAllocateDevicesRequest>(
            shardId,
            tabletId,
            CloudId,
            FolderId,
            mediaKind,
            replicaCount,
            StorageConfig->GetFastShardAllocationSize() / DefaultBlockSize,
            StorageConfig->GetFastShardDevicePoolName()),
        ev->Cookie);
}

void TAllocateFastShardsActor::HandleAllocateDevicesResponse(
    const TEvDiskRegistryProxy::TEvAllocateDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (ev->Cookie >= ShardIds.size()) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "invalid cookie: " << ev->Cookie));

        return;
    }

    auto& config = *Topology.MutableShards(ev->Cookie)
        ->MutableFastShardConfig()
        ->MutablePersistentConfig();
    const ui64 capacity = GetFastShardCapacity(*StorageConfig);
    config.SetExpectedGroupCapacity(capacity);
    config.SetNodesPerGroup(capacity / StorageConfig->GetSizeToNodesRatio());
    config.SetPageSize(BlockSize);
    auto& group = *config.AddStorageGroups();
    group.SetType(StorageKind);
    for (const auto& replica: msg->Layout.Replicas) {
        // A device that is not journalled has no host.
        if (replica.size() != 1 || !replica[0].GetHost()) {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "invalid device layout of "
                        << ShardIds[ev->Cookie]));
            return;
        }
        *group.AddDevices() = replica[0];
    }

    if (++AllocatedShardCount == ShardIds.size()) {
        ReplyAndDie(ctx, {});
        return;
    }

    if (NextShard < ShardIds.size()) {
        DescribeNextShard(ctx);
    }
}

void TAllocateFastShardsActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Allocating fast shards failed: %s",
            LogTag.c_str(),
            FormatError(error).c_str());
    }

    auto response = std::make_unique<TEvServicePrivate::TEvFastShardsAllocated>(
        std::move(error));
    response->Topology = std::move(Topology);
    NCloud::Send(ctx, Owner, std::move(response));
    Die(ctx);
}

STFUNC(TAllocateFastShardsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreResponse);
        HFunc(
            TEvDiskRegistryProxy::TEvAllocateDevicesResponse,
            HandleAllocateDevicesResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDeallocateFastShardsActor final
    : public TActorBootstrapped<TDeallocateFastShardsActor>
{
private:
    const TActorId Owner;
    const TString LogTag;
    const NProtoPrivate::TShardTopology Topology;

    ui32 ShardIndex = 0;

public:
    TDeallocateFastShardsActor(
        TActorId owner,
        TString logTag,
        NProtoPrivate::TShardTopology topology);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void MarkShard(const TActorContext& ctx);
    void MarkNextShard(const TActorContext& ctx);

    void HandleMarkForCleanupResponse(
        const TEvDiskRegistryProxy::TEvMarkForCleanupResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDestroyFileStoreResponse(
        const TEvSSProxy::TEvDestroyFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeallocateDevicesResponse(
        const TEvDiskRegistryProxy::TEvDeallocateDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TDeallocateFastShardsActor::TDeallocateFastShardsActor(
        TActorId owner,
        TString logTag,
        NProtoPrivate::TShardTopology topology)
    : Owner(owner)
    , LogTag(std::move(logTag))
    , Topology(std::move(topology))
{}

void TDeallocateFastShardsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    MarkShard(ctx);
}

void TDeallocateFastShardsActor::MarkShard(const TActorContext& ctx)
{
    if (ShardIndex == Topology.ShardsSize()) {
        ReplyAndDie(ctx, {});
        return;
    }

    const auto& shard = Topology.GetShards(ShardIndex);
    NCloud::Send(
        ctx,
        MakeFileStoreDiskRegistryProxyId(),
        std::make_unique<TEvDiskRegistryProxy::TEvMarkForCleanupRequest>(
            shard.GetShardId(),
            shard.GetTabletId()));
}

void TDeallocateFastShardsActor::MarkNextShard(const TActorContext& ctx)
{
    ++ShardIndex;
    MarkShard(ctx);
}

void TDeallocateFastShardsActor::HandleMarkForCleanupResponse(
    const TEvDiskRegistryProxy::TEvMarkForCleanupResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    // The shard has no devices.
    if (msg->GetStatus() == E_NOT_FOUND) {
        MarkNextShard(ctx);
        return;
    }

    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDestroyFileStoreRequest>(
            Topology.GetShards(ShardIndex).GetShardId()));
}

void TDeallocateFastShardsActor::HandleDestroyFileStoreResponse(
    const TEvSSProxy::TEvDestroyFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& shard = Topology.GetShards(ShardIndex);
    NCloud::Send(
        ctx,
        MakeFileStoreDiskRegistryProxyId(),
        std::make_unique<TEvDiskRegistryProxy::TEvDeallocateDevicesRequest>(
            shard.GetShardId(),
            shard.GetTabletId()));
}

void TDeallocateFastShardsActor::HandleDeallocateDevicesResponse(
    const TEvDiskRegistryProxy::TEvDeallocateDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    MarkNextShard(ctx);
}

void TDeallocateFastShardsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Deallocating fast shards failed: %s",
            LogTag.c_str(),
            FormatError(error).c_str());
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvServicePrivate::TEvFastShardsDeallocated>(error));
    Die(ctx);
}

STFUNC(TDeallocateFastShardsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistryProxy::TEvMarkForCleanupResponse,
            HandleMarkForCleanupResponse);
        HFunc(
            TEvSSProxy::TEvDestroyFileStoreResponse,
            HandleDestroyFileStoreResponse);
        HFunc(
            TEvDiskRegistryProxy::TEvDeallocateDevicesResponse,
            HandleDeallocateDevicesResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ui64 GetFastShardCapacity(const TStorageConfig& config)
{
    return config.GetFastShardAllocationSize() -
           config.GetFastShardReservedSize();
}

ui64 CalculateFastShardCount(
    const TStorageConfig& config,
    ui64 blocksCount,
    ui32 blockSize)
{
    const ui64 capacity = GetFastShardCapacity(config) / blockSize;
    ui64 count = blocksCount / capacity;
    if (blocksCount % capacity) {
        ++count;
    }

    return count;
}

IActorPtr CreateAllocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    TActorId owner,
    TString logTag,
    const NKikimrFileStore::TConfig& mainConfig,
    const TVector<NKikimrFileStore::TConfig>& shardConfigs,
    ui32 fastShardCount,
    NProto::EFastShardStorageKind storageKind)
{
    const ui32 fastShardsBegin = shardConfigs.size() - fastShardCount;
    TVector<TString> shardIds;
    for (ui32 i = fastShardsBegin; i < shardConfigs.size(); ++i) {
        shardIds.push_back(shardConfigs[i].GetFileSystemId());
    }

    return std::make_unique<TAllocateFastShardsActor>(
        std::move(storageConfig),
        owner,
        std::move(logTag),
        std::move(shardIds),
        storageKind,
        mainConfig.GetCloudId(),
        mainConfig.GetFolderId(),
        mainConfig.GetBlockSize());
}

IActorPtr CreateDeallocateFastShardsActor(
    TActorId owner,
    TString logTag,
    NProtoPrivate::TShardTopology topology)
{
    return std::make_unique<TDeallocateFastShardsActor>(
        owner,
        std::move(logTag),
        std::move(topology));
}

}   // namespace NCloud::NFileStore::NStorage
