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

auto MakeAllocateDevicesRequest(
    const TStorageConfig& config,
    const TString& shardId,
    ui64 tabletId,
    const TString& cloudId,
    const TString& folderId,
    NProto::EFastShardStorageKind storageKind)
    -> std::unique_ptr<TEvDiskRegistryProxy::TEvAllocateDevicesRequest>
{
    // A mirror writes two copies, a quorum needs three. The replica count
    // leaves out the main copy.
    auto mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR2;
    ui32 replicaCount = 1;
    if (storageKind == NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR) {
        mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3;
        replicaCount = 2;
    }

    return std::make_unique<TEvDiskRegistryProxy::TEvAllocateDevicesRequest>(
        shardId,
        tabletId,
        cloudId,
        folderId,
        mediaKind,
        replicaCount,
        config.GetFastShardAllocationSize() / DefaultBlockSize,
        config.GetFastShardDevicePoolName());
}

TResultOrError<NProtoPrivate::TFastShardConfig> MakeFastShardConfig(
    const TStorageConfig& config,
    const TString& shardId,
    const TEvDiskRegistryProxy::TDeviceLayout& layout,
    NProto::EFastShardStorageKind storageKind,
    ui32 blockSize)
{
    NProtoPrivate::TFastShardConfig result;
    auto& persistent = *result.MutablePersistentConfig();
    const ui64 capacity = GetFastShardCapacity(config);
    persistent.SetExpectedGroupCapacity(capacity);
    persistent.SetNodesPerGroup(capacity / config.GetSizeToNodesRatio());
    persistent.SetPageSize(blockSize);
    auto& group = *persistent.AddStorageGroups();
    group.SetType(storageKind);
    for (const auto& replica: layout.Replicas) {
        // A device that is not journalled has no host.
        if (replica.size() != 1 || !replica[0].GetHost()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "invalid device layout of " << shardId);
        }
        *group.AddDevices() = replica[0];
    }

    return result;
}

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
