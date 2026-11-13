#include "service_actor_fastshard.h"

#include "service_private.h"

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/disk_registry_proxy/api/service.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
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
    const ui64 FileSystemSize;

    THashMap<TString, TEvServicePrivate::TAllocatedFastShardConfig> FastShards;

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
        ui64 fileSystemSize);

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
        ui64 fileSystemSize)
    : StorageConfig(std::move(storageConfig))
    , Owner(owner)
    , LogTag(std::move(logTag))
    , ShardIds(std::move(shardIds))
    , StorageKind(storageKind)
    , CloudId(std::move(cloudId))
    , FolderId(std::move(folderId))
    , FileSystemSize(fileSystemSize)
{}

void TAllocateFastShardsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

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
        ReplyAndDie(ctx, MakeError(E_INVALID_STATE, TStringBuilder()
            << "invalid cookie: " << ev->Cookie));
        return;
    }

    const auto& shardId = ShardIds[ev->Cookie];
    const ui64 tabletId = msg->PathDescription.GetFileStoreDescription().GetIndexTabletId();
    FastShards[shardId].TabletId = tabletId;

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Allocating devices for %s",
        LogTag.c_str(),
        shardId.c_str());

    // A mirror writes two copies, a quorum needs three. The replica count
    // leaves out the main copy.
    auto mediaKind = NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2;
    ui32 replicaCount = 1;
    if (StorageKind == NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR) {
        mediaKind = NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3;
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

    auto& config = FastShards[ShardIds[ev->Cookie]].Config;
    config.SetExpectedGroupCapacity(FileSystemSize);
    config.SetNodesPerGroup(FileSystemSize / StorageConfig->GetSizeToNodesRatio());
    auto& group = *config.AddStorageGroups();
    group.SetType(StorageKind);
    for (const auto& replica: msg->Layout.Replicas) {
        // A device that is not journalled has no host.
        if (replica.size() != 1 || !replica[0].GetHost()) {
            ReplyAndDie(ctx, MakeError(E_INVALID_STATE, TStringBuilder()
                << "invalid device layout of " << ShardIds[ev->Cookie]));
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
    response->FastShards = std::move(FastShards);
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

// The devices are deallocated only after their shard is destroyed. The tablet ids come from the main tablet: a retry
// has them when the shard is gone.
class TDeallocateFastShardsActor final
    : public TActorBootstrapped<TDeallocateFastShardsActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TActorId Owner;
    const TString LogTag;
    const NProtoPrivate::TFastShardTopology Topology;

    ui32 ShardIndex = 0;

public:
    TDeallocateFastShardsActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TString logTag,
        NProtoPrivate::TFastShardTopology topology);

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
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TString logTag,
        NProtoPrivate::TFastShardTopology topology)
    : StorageConfig(std::move(storageConfig))
    , Owner(owner)
    , LogTag(std::move(logTag))
    , Topology(std::move(topology))
{}

void TDeallocateFastShardsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    // The disk registry proxy exists only with the runtime.
    if (!StorageConfig->GetFastShardRuntimeEnabled()) {
        ReplyAndDie(ctx, MakeError(E_PRECONDITION_FAILED, "fast shards are disabled"));
        return;
    }

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
        std::make_unique<TEvDiskRegistryProxy::TEvMarkForCleanupRequest>(shard.GetShardId(), shard.GetTabletId()));
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
        std::make_unique<TEvSSProxy::TEvDestroyFileStoreRequest>(Topology.GetShards(ShardIndex).GetShardId()));
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
        std::make_unique<TEvDiskRegistryProxy::TEvDeallocateDevicesRequest>(shard.GetShardId(), shard.GetTabletId()));
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

NCloud::NProto::TError ValidateFastShardsConfig(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    const ui32 fastShardCount = request.GetFastShardCount();
    if (!fastShardCount) {
        return {};
    }

    if (!config.GetFastShardRuntimeEnabled()) {
        return MakeError(E_PRECONDITION_FAILED, "fast shards are disabled");
    }

    if (!config.GetDirectoryCreationInShardsEnabled()) {
        return MakeError(E_PRECONDITION_FAILED, "fast shards need directory creation in shards");
    }

    if (request.GetShardCount() > config.GetMaxShardCount()) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "shard count " << request.GetShardCount()
            << " should not be bigger than " << config.GetMaxShardCount());
    }

    if (request.GetShardCount() <= fastShardCount) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "shard count " << request.GetShardCount()
            << " should be bigger than fast shard count " << fastShardCount);
    }

    const auto storageKind = request.GetFastShardStorageKind();
    if (!NProto::EFastShardStorageKind_IsValid(storageKind)) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid fast shard storage kind: " << static_cast<int>(storageKind));
    }

    return ValidateFastShardCapacity(config, request.GetBlocksCount() * request.GetBlockSize());
}

NCloud::NProto::TError ValidateFastShardCapacity(const TStorageConfig& config, ui64 fileSystemSize)
{
    if (config.GetFastShardAllocationSize() <= config.GetFastShardReservedSize()) {
        return MakeError(E_PRECONDITION_FAILED, "fast shard allocation size is not configured");
    }

    const ui64 capacity = config.GetFastShardAllocationSize() - config.GetFastShardReservedSize();
    if (capacity < fileSystemSize) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "filesystem size " << FormatByteSize(fileSystemSize)
            << " exceeds fast shard capacity " << FormatByteSize(capacity));
    }

    return {};
}

IActorPtr CreateAllocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    TActorId owner,
    TString logTag,
    TVector<TString> shardIds,
    NProto::EFastShardStorageKind storageKind,
    TString cloudId,
    TString folderId,
    ui64 fileSystemSize)
{
    return std::make_unique<TAllocateFastShardsActor>(
        std::move(storageConfig),
        owner,
        std::move(logTag),
        std::move(shardIds),
        storageKind,
        std::move(cloudId),
        std::move(folderId),
        fileSystemSize);
}

IActorPtr CreateDeallocateFastShardsActor(
    TStorageConfigPtr storageConfig,
    TActorId owner,
    TString logTag,
    NProtoPrivate::TFastShardTopology topology)
{
    return std::make_unique<TDeallocateFastShardsActor>(
        std::move(storageConfig),
        owner,
        std::move(logTag),
        std::move(topology));
}

}   // namespace NCloud::NFileStore::NStorage
