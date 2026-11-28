#include "service_actor_multishard_create.h"

#include "service_actor_fastshard.h"
#include "service_private.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/disk_registry_proxy/api/service.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/compressed_bitmap_serialization.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/digest/city.h>
#include <util/string/builder.h>

#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 ShardConfigsHashSeed = 0x6f4c9ad921f7b55dULL;

// A retry gives the same hash: the fields that change between retries are
// left out.
ui64 GetShardConfigsHash(
    const TVector<TMultiShardCreateConfig::TShardConfig>& shards)
{
    ui64 hash = ShardConfigsHashSeed;
    for (const auto& shard: shards) {
        auto config = shard.Config;
        config.ClearVersion();
        config.ClearCreationTs();
        config.ClearAlterTs();
        hash = CityHash64WithSeed(config.SerializeAsString(), hash);

        ui32 storageKind = 0;
        if (shard.FastShardStorageKind) {
            storageKind = *shard.FastShardStorageKind;
        }

        const TString form = TStringBuilder()
            << static_cast<ui32>(shard.Type)
            << ' ' << shard.RoleMask << ' '
            << storageKind;

        hash = CityHash64WithSeed(form, hash);
    }

    return hash;
}

NProto::TError ValidateShardConfig(
    const TMultiShardCreateConfig::TShardConfig& shard)
{
    using TShardTopology = NProtoPrivate::TShardTopology;

    const auto& shardId = shard.Config.GetFileSystemId();
    if (shard.Type == TShardTopology::E_ORDINARY) {
        return {};
    }

    if (shard.Type != TShardTopology::E_FASTSHARD) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "no shard type of " << shardId);
    }

    if (!shard.FastShardStorageKind) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "no storage kind of " << shardId);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TMultiShardCreateActor final
    : public TActorBootstrapped<TMultiShardCreateActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TActorId Owner;
    const TMultiShardCreateConfig Config;

    // The created shards.
    NProtoPrivate::TShardTopology Topology;
    NProtoPrivate::TFileSystemShardCreationState State;
    std::unique_ptr<TCompressedBitmap> Bitmap;

    bool UpdateInFlight = false;

    ui32 Next = 0;
    ui32 InFlight = 0;
    ui32 Done = 0;

    TProtoMessagePrinter ProtoMessagePrinter;

public:
    TMultiShardCreateActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TMultiShardCreateConfig config);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SaveState(const TActorContext& ctx);

    void CreateShards(const TActorContext& ctx);
    void CreateShard(const TActorContext& ctx, ui32 shardIndex);
    void OnShardCreated(const TActorContext& ctx, ui32 shardIndex);
    void FinishShard();
    void Advance(const TActorContext& ctx);
    void ReplyIfAllDone(const TActorContext& ctx);

    void HandleSaveStateResponse(
        const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAllocateDevicesResponse(
        const TEvDiskRegistryProxy::TEvAllocateDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    NProto::TError CheckCookie(ui64 cookie) const;
    const TString& GetShardId(ui32 shardIndex) const;

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TMultiShardCreateActor::TMultiShardCreateActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TMultiShardCreateConfig config)
    : StorageConfig(std::move(storageConfig))
    , Owner(owner)
    , Config(std::move(config))
{
    if (Config.State) {
        State = *Config.State;
    }
}

void TMultiShardCreateActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (const auto& shardConfig: Config.ShardsToCreate) {
        auto error = ValidateShardConfig(shardConfig);
        if (HasError(error)) {
            ReplyAndDie(ctx, error);
            return;
        }

        auto& shard = *Topology.AddShards();
        shard.SetShardId(shardConfig.Config.GetFileSystemId());
        shard.SetType(shardConfig.Type);
        shard.SetRoleMask(shardConfig.RoleMask);
    }

    auto bitmap = LoadCompressedBitmap(
        State.GetCreatedShardBitmap(),
        Config.ExistingShardCount + Config.ShardsToCreate.size(),
        MaxShardCount);

    if (HasError(bitmap)) {
        ReplyAndDie(ctx, bitmap.GetError());
        return;
    }

    Bitmap = bitmap.ExtractResult();

    Advance(ctx);
}

void TMultiShardCreateActor::Advance(const TActorContext& ctx)
{
    CreateShards(ctx);
    ReplyIfAllDone(ctx);
}

void TMultiShardCreateActor::CreateShards(const TActorContext& ctx)
{
    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    while (Next < Config.ShardsToCreate.size() && (!limit || InFlight < limit)) {
        ++InFlight;
        CreateShard(ctx, Next++);
    }
}

void TMultiShardCreateActor::CreateShard(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    if (Bitmap->Test(Config.ExistingShardCount + shardIndex)) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard already created: %s",
            Config.FileSystemId.c_str(),
            GetShardId(shardIndex).c_str());

        OnShardCreated(ctx, shardIndex);
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating shard %s",
        Config.FileSystemId.c_str(),
        GetShardId(shardIndex).c_str());

    const auto& shardConfig = Config.ShardsToCreate[shardIndex];

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Shard %s config: %s",
        Config.FileSystemId.c_str(),
        GetShardId(shardIndex).c_str(),
        ProtoMessagePrinter.ToString(shardConfig.Config).c_str());

    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        shardConfig.Config);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), shardIndex);
}

void TMultiShardCreateActor::HandleCreateFileStoreResponse(
    const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (const auto error = CheckCookie(ev->Cookie); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Created shard %s",
        Config.FileSystemId.c_str(),
        GetShardId(ev->Cookie).c_str());

    const ui32 shardIndex = Config.ExistingShardCount + ev->Cookie;
    Bitmap->Set(shardIndex, shardIndex + 1);
    if (Config.State && !UpdateInFlight) {
        SaveState(ctx);
    }

    OnShardCreated(ctx, ev->Cookie);
    Advance(ctx);
}

void TMultiShardCreateActor::OnShardCreated(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    if (Config.ShardsToCreate[shardIndex].Type !=
        NProtoPrivate::TShardTopology::E_FASTSHARD)
    {
        FinishShard();
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Describing shard %s",
        Config.FileSystemId.c_str(),
        GetShardId(shardIndex).c_str());

    // The devices are allocated for the shard tablet.
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        GetShardId(shardIndex));
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), shardIndex);
}

void TMultiShardCreateActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (const auto error = CheckCookie(ev->Cookie); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    const auto& shardId = GetShardId(ev->Cookie);
    const ui64 tabletId = msg->PathDescription.GetFileStoreDescription()
        .GetIndexTabletId();

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Described shard %s",
        Config.FileSystemId.c_str(),
        shardId.c_str());

    if (!tabletId) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "no tablet id of " << shardId));
        return;
    }

    Topology.MutableShards(ev->Cookie)->SetTabletId(tabletId);

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Allocating devices for %s:%lu",
        Config.FileSystemId.c_str(),
        shardId.c_str(),
        tabletId);

    const auto& shardConfig = Config.ShardsToCreate[ev->Cookie];
    auto request = MakeAllocateDevicesRequest(
        *StorageConfig,
        shardId,
        tabletId,
        shardConfig.Config.GetCloudId(),
        shardConfig.Config.GetFolderId(),
        *shardConfig.FastShardStorageKind);

    NCloud::Send(
        ctx,
        MakeFileStoreDiskRegistryProxyId(),
        std::move(request),
        ev->Cookie);
}

void TMultiShardCreateActor::HandleAllocateDevicesResponse(
    const TEvDiskRegistryProxy::TEvAllocateDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (const auto error = CheckCookie(ev->Cookie); HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    const auto& shardConfig = Config.ShardsToCreate[ev->Cookie];

    auto fastShardConfig = MakeFastShardConfig(
        *StorageConfig,
        GetShardId(ev->Cookie),
        msg->Layout,
        *shardConfig.FastShardStorageKind,
        shardConfig.Config.GetBlockSize());

    if (HasError(fastShardConfig)) {
        ReplyAndDie(ctx, fastShardConfig.GetError());
        return;
    }

    auto& shard = *Topology.MutableShards(ev->Cookie);
    *shard.MutableFastShardConfig() = fastShardConfig.ExtractResult();

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Allocated devices for %s",
        Config.FileSystemId.c_str(),
        shard.GetShardId().c_str());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Fast shard: %s",
        Config.FileSystemId.c_str(),
        ProtoMessagePrinter.ToString(shard).c_str());

    FinishShard();
    Advance(ctx);
}

void TMultiShardCreateActor::SaveState(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    request->Record.SetFileSystemId(Config.FileSystemId);

    auto& state = *request->Record.MutableShardCreationState();
    state = State;
    state.SetVersion(State.GetVersion());
    SaveCompressedBitmap(
        *Bitmap,
        Config.ExistingShardCount + Config.ShardsToCreate.size(),
        *state.MutableCreatedShardBitmap());

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Saving shard creation state, version %u",
        Config.FileSystemId.c_str(),
        state.GetVersion());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Shard creation state: %s",
        Config.FileSystemId.c_str(),
        ProtoMessagePrinter.ToString(state).c_str());

    UpdateInFlight = true;
    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TMultiShardCreateActor::HandleSaveStateResponse(
    const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    if (HasError(record.GetError())) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "failed to update shard creation state: "
                    << FormatError(record.GetError())));
        return;
    }

    if (!record.HasShardCreationState()) {
        ReportShardCreationStateResponseMissing(
            TStringBuilder()
                << "Filesystem " << Config.FileSystemId.Quote()
                << ": response is missing shard creation state"
                << " after persistent state support was confirmed");
        ReplyAndDie(
            ctx,
            MakeError(E_REJECTED, "shard creation state response is missing"));
        return;
    }

    const auto& stored = record.GetShardCreationState();
    if (State.GetTopologyShardCount() != stored.GetTopologyShardCount() ||
        State.GetTargetShardCount() != stored.GetTargetShardCount() ||
        State.GetTargetShardConfigHash() != stored.GetTargetShardConfigHash())
    {
        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Shard creation/resize plan does not match the current "
                    << "request for filesystem "
                    << Config.FileSystemId.Quote() << ": request topology="
                    << State.GetTopologyShardCount()
                    << ", target=" << State.GetTargetShardCount()
                    << ", hash=" << State.GetTargetShardConfigHash()
                    << "; stored topology=" << stored.GetTopologyShardCount()
                    << ", target=" << stored.GetTargetShardCount()
                    << ", hash=" << stored.GetTargetShardConfigHash()
                    << ". Check the stored shard creation state before "
                    << "retrying"));
        return;
    }

    MergeCompressedBitmap(*Bitmap, stored.GetCreatedShardBitmap());
    State = stored;

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Saved shard creation state, version %u",
        Config.FileSystemId.c_str(),
        State.GetVersion());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Saved shard creation state: %s",
        Config.FileSystemId.c_str(),
        ProtoMessagePrinter.ToString(State).c_str());

    const ui32 target = Config.ExistingShardCount + Config.ShardsToCreate.size();
    const auto persisted = LoadCompressedBitmap(
        stored.GetCreatedShardBitmap(),
        target,
        MaxShardCount);
    if (HasError(persisted)) {
        ReplyAndDie(ctx, persisted.GetError());
        return;
    }

    for (ui32 i = 0; i < target; ++i) {
        if (Bitmap->Test(i) && !persisted.GetResult()->Test(i)) {
            SaveState(ctx);
            return;
        }
    }

    UpdateInFlight = false;
    ReplyIfAllDone(ctx);
}

NProto::TError TMultiShardCreateActor::CheckCookie(ui64 cookie) const
{
    if (cookie < Config.ShardsToCreate.size()) {
        return {};
    }

    return MakeError(
        E_INVALID_STATE,
        TStringBuilder() << "invalid cookie: " << cookie);
}

void TMultiShardCreateActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TMultiShardCreateActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Creating shards failed: %s",
            Config.FileSystemId.c_str(),
            FormatError(error).c_str());
    }

    using TResponse = TEvServicePrivate::TEvMultiShardCreateResponse;

    auto response = std::make_unique<TResponse>(error);
    response->Topology = std::move(Topology);

    NCloud::Send(ctx, Owner, std::move(response));
    Die(ctx);
}

void TMultiShardCreateActor::FinishShard()
{
    --InFlight;
    ++Done;
}

void TMultiShardCreateActor::ReplyIfAllDone(const TActorContext& ctx)
{
    if (Done == Config.ShardsToCreate.size() && !UpdateInFlight) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Created %lu shards",
            Config.FileSystemId.c_str(),
            Config.ShardsToCreate.size());

        ReplyAndDie(ctx, {});
    }
}

const TString& TMultiShardCreateActor::GetShardId(ui32 shardIndex) const
{
    return Config.ShardsToCreate[shardIndex].Config.GetFileSystemId();
}

STFUNC(TMultiShardCreateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvUnsafeChangeTabletStateResponse,
            HandleSaveStateResponse);
        HFunc(
            TEvSSProxy::TEvCreateFileStoreResponse,
            HandleCreateFileStoreResponse);
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

auto InitShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& saved,
    const TMultiShardCreateConfig& config)
    -> TResultOrError<NProtoPrivate::TFileSystemShardCreationState>
{
    ui32 existingShardCount = config.ExistingShardCount;
    ui32 target = existingShardCount + config.ShardsToCreate.size();
    ui64 hash = GetShardConfigsHash(config.ShardsToCreate);

    ui64 savedShardCount = Max<ui64>(
        saved.GetCreatedShardBitmap().GetBitCount(),
        saved.GetTargetShardCount());

    auto [bitmap, error] = LoadCompressedBitmap(
        saved.GetCreatedShardBitmap(),
        Max<ui64>(savedShardCount, target),
        MaxShardCount);

    if (HasError(error)) {
        return error;
    }

    bool unfinished = existingShardCount < savedShardCount &&
        bitmap->Count(existingShardCount, savedShardCount) != 0;

    bool sameShards = saved.GetTopologyShardCount() == existingShardCount &&
        saved.GetTargetShardCount() == target &&
        saved.GetTargetShardConfigHash() == hash;

    if (unfinished && !sameShards) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Unfinished shard creation state conflicts with current "
                << "plan for filesystem " << config.FileSystemId.Quote()
                << ": stored topology=" << saved.GetTopologyShardCount()
                << ", target=" << saved.GetTargetShardCount()
                << ", hash=" << saved.GetTargetShardConfigHash()
                << "; current topology=" << existingShardCount
                << ", target=" << target
                << ", hash=" << hash
                << ". Retry the previous create/resize request with the same"
                << " parameters or ask devops to reconcile already-created "
                << "shards");
    }

    auto state = saved;
    if (!unfinished) {
        state.MutableCreatedShardBitmap()->Clear();
        state.MutableCreatedShardBitmap()->SetBitCount(target);
        state.SetTopologyShardCount(existingShardCount);
        state.SetTargetShardCount(target);
        state.SetTargetShardConfigHash(hash);
    }

    return state;
}

IActorPtr CreateMultiShardCreateActor(
    TStorageConfigPtr storageConfig,
    TActorId owner,
    TMultiShardCreateConfig config)
{
    return std::make_unique<TMultiShardCreateActor>(
        std::move(storageConfig),
        owner,
        std::move(config));
}

}   // namespace NCloud::NFileStore::NStorage
