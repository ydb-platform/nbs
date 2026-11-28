#include "service_actor.h"

#include "service_actor_fastshard.h"
#include "service_actor_multishard_configure.h"
#include "service_actor_multishard_create.h"
#include "service_private.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/bitops.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Without an explicit count a filesystem gets the fast shards its size needs.
ui64 GetFastShardCount(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    if (!request.HasFastShardStorageKind()) {
        return 0;
    }

    if (request.GetFastShardCount()) {
        return request.GetFastShardCount();
    }

    return CalculateFastShardCount(
        config,
        request.GetBlocksCount(),
        request.GetBlockSize());
}

NProto::TError ValidateFastShards(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    if (request.GetFastShardCount() && !request.HasFastShardStorageKind()) {
        return MakeError(E_ARGUMENT, "fast shard count needs a storage kind");
    }

    if (!request.HasFastShardStorageKind()) {
        return {};
    }

    if (!config.GetDirectoryCreationInShardsEnabled()) {
        return MakeError(
            E_PRECONDITION_FAILED,
            "fast shards need directory creation in shards");
    }

    // The directories are not created in the fast shards.
    if (!request.GetShardCount()) {
        return MakeError(
            E_ARGUMENT,
            "fast shards need an explicit shard count");
    }

    const auto storageKind = request.GetFastShardStorageKind();
    if (!NProto::EFastShardStorageKind_IsValid(storageKind)) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "invalid fast shard storage kind: "
                << static_cast<int>(storageKind));
    }

    const ui64 requiredFastShardCount = CalculateFastShardCount(
        config,
        request.GetBlocksCount(),
        request.GetBlockSize());
    if (request.GetFastShardCount() &&
        request.GetFastShardCount() < requiredFastShardCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "fast shard count " << request.GetFastShardCount()
                << " cannot hold the filesystem, " << requiredFastShardCount
                << " needed");
    }

    return {};
}

NProto::TError ValidateTotalShards(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    ui64 shardCount = request.GetShardCount();
    shardCount += GetFastShardCount(config, request);
    if (shardCount > config.GetMaxShardCount()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "shard count " << shardCount << " should not be bigger than "
                << config.GetMaxShardCount());
    }

    return {};
}

NProto::TError ValidateCreateFileSystemRequest(
    const TStorageConfig& config,
    const NProto::TCreateFileStoreRequest& request)
{
    const auto& fileSystemId = request.GetFileSystemId();
    if (!fileSystemId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "missing file system identifier");
    }

    NProto::TError fileSystemIdError = ValidateFilesystemId(fileSystemId);
    if (HasError(fileSystemIdError)) {
        return fileSystemIdError;
    }

    const auto& cloudId = request.GetCloudId();
    if (!cloudId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "missing cloud identifier");
    }

    const auto& folderId = request.GetFolderId();
    if (!folderId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "missing folder identifier");
    }

    // The block arithmetic all over the tablet and the client masks by the
    // block size, so it has to be a power of two.
    ui32 blockSize = request.GetBlockSize();
    if (!IsPowerOf2(blockSize)
            || blockSize < 4_KB
            || blockSize > 128_KB)
    {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid block size: " << blockSize);
    }

    ui64 blocksCount = request.GetBlocksCount();
    if (!blocksCount || blockSize * blocksCount < 1_MB) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "invalid blocks count: " << blocksCount);
    }

    if (auto error = ValidateFastShards(config, request); HasError(error)) {
        return error;
    }

    return ValidateTotalShards(config, request);
}

////////////////////////////////////////////////////////////////////////////////

class TCreateFileStoreActor final
    : public TActorBootstrapped<TCreateFileStoreActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TCreateFileStoreRequest Request;

    TMultiShardCreateConfig ShardsConfig;
    // The step actor in flight.
    TActorId Worker;

public:
    TCreateFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TCreateFileStoreRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateMainFileStore(const TActorContext& ctx);
    void CreateShards(const TActorContext& ctx);
    void ConfigureShards(
        const TActorContext& ctx,
        NProtoPrivate::TShardTopology topology);

    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleShardCreationStateResponse(
        const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleMultiShardCreateResponse(
        const TEvServicePrivate::TEvMultiShardCreateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleMultiShardConfigureResponse(
        const TEvServicePrivate::TEvMultiShardConfigureResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TCreateFileStoreActor::TCreateFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TCreateFileStoreRequest request)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TCreateFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    CreateMainFileStore(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::CreateMainFileStore(const TActorContext& ctx)
{
    NKikimrFileStore::TConfig config;
    config.SetFileSystemId(Request.GetFileSystemId());
    config.SetProjectId(Request.GetProjectId());
    config.SetFolderId(Request.GetFolderId());
    config.SetCloudId(Request.GetCloudId());
    config.SetBlockSize(Request.GetBlockSize());
    config.SetBlocksCount(Request.GetBlocksCount());
    config.SetStorageMediaKind(Request.GetStorageMediaKind());
    config.SetRangeIdHasherType(1);

    if (StorageConfig->GetAutomaticShardCreationEnabled() ||
        Request.GetShardCount() > 0)
    {
        const ui32 fastShardCount = GetFastShardCount(*StorageConfig, Request);
        auto fileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            config,
            Request.GetPerformanceProfile(),
            Request.GetShardCount() + fastShardCount);
        config = fileStoreConfig.MainFileSystemConfig;

        using TShardTopology = NProtoPrivate::TShardTopology;

        // The fast shards are the last ones.
        auto& shardConfigs = fileStoreConfig.ShardConfigs;
        const ui32 fastShardsBegin = shardConfigs.size() - fastShardCount;
        for (ui32 i = 0; i < shardConfigs.size(); ++i) {
            auto& shard = ShardsConfig.ShardsToCreate.emplace_back();
            shard.Config = std::move(shardConfigs[i]);
            if (i < fastShardsBegin) {
                shard.Type = TShardTopology::E_ORDINARY;
                shard.RoleMask = ProtoFlag(TShardTopology::E_FILE) |
                    ProtoFlag(TShardTopology::E_DIRECTORY);
            } else {
                shard.Type = TShardTopology::E_FASTSHARD;
                shard.RoleMask = ProtoFlag(TShardTopology::E_FILE);
                shard.FastShardStorageKind = Request.GetFastShardStorageKind();
            }
        }

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Will create filesystem with %lu shards",
            Request.GetFileSystemId().c_str(),
            ShardsConfig.ShardsToCreate.size());
    } else {
        SetupFileStorePerformanceAndChannels(
            false,  // do not allocate mixed0 channel
            *StorageConfig,
            config,
            Request.GetPerformanceProfile());
    }

    ShardsConfig.FileSystemId = Request.GetFileSystemId();

    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        std::move(config));

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TCreateFileStoreActor::CreateShards(const TActorContext& ctx)
{
    auto actor = CreateMultiShardCreateActor(
        StorageConfig,
        SelfId(),
        std::move(ShardsConfig));
    Worker = NCloud::Register(ctx, std::move(actor));
}

void TCreateFileStoreActor::ConfigureShards(
    const TActorContext& ctx,
    NProtoPrivate::TShardTopology topology)
{
    TMultiShardConfigureConfig config;
    config.FileSystemId = Request.GetFileSystemId();
    config.Topology = std::move(topology);
    config.DirectoryCreationInShardsEnabled =
        StorageConfig->GetDirectoryCreationInShardsEnabled();
    config.StrictFileSystemSizeEnforcementEnabled =
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled();

    auto actor = CreateMultiShardConfigureActor(
        StorageConfig,
        SelfId(),
        std::move(config));
    Worker = NCloud::Register(ctx, std::move(actor));
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::HandleCreateFileStoreResponse(
    const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Created main filesystem",
        Request.GetFileSystemId().c_str());

    if (ShardsConfig.ShardsToCreate.empty()) {
        // A filesystem without shards still gets the flags.
        if (StorageConfig->GetDirectoryCreationInShardsEnabled() ||
            StorageConfig->GetStrictFileSystemSizeEnforcementEnabled())
        {
            ConfigureShards(ctx, {});
            return;
        }

        // TODO: fill filestore info
        ReplyAndDie(ctx);
        return;
    }

    if (!StorageConfig->GetMaxShardManagementRequestsInFlight()) {
        CreateShards(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Reading shard creation state",
        Request.GetFileSystemId().c_str());

    auto request =
        std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    request->Record.SetFileSystemId(Request.GetFileSystemId());
    // Without a version the tablet only returns the state.
    request->Record.MutableShardCreationState();
    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TCreateFileStoreActor::HandleShardCreationStateResponse(
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
                    << "failed to read shard creation state: "
                    << FormatError(record.GetError())));
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Read shard creation state",
        Request.GetFileSystemId().c_str());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Shard creation state: %s",
        Request.GetFileSystemId().c_str(),
        TProtoMessagePrinter().ToString(record).c_str());

    // A tablet of the previous version returns no state.
    if (record.HasShardCreationState()) {
        auto state = InitShardCreationState(
            record.GetShardCreationState(),
            ShardsConfig);
        if (HasError(state)) {
            ReplyAndDie(ctx, state.GetError());
            return;
        }
        ShardsConfig.State = state.ExtractResult();
    }

    CreateShards(ctx);
}

void TCreateFileStoreActor::HandleMultiShardCreateResponse(
    const TEvServicePrivate::TEvMultiShardCreateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    ConfigureShards(ctx, msg->Topology);
}

void TCreateFileStoreActor::HandleMultiShardConfigureResponse(
    const TEvServicePrivate::TEvMultiShardConfigureResponse::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO: fill filestore info
    ReplyAndDie(ctx, ev->Get()->GetError());
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    NCloud::Send(ctx, Worker, std::make_unique<TEvents::TEvPoisonPill>());
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TCreateFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Creating filesystem failed: %s",
            Request.GetFileSystemId().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Created filesystem",
            Request.GetFileSystemId().c_str());
    }

    auto response =
        std::make_unique<TEvService::TEvCreateFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TCreateFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvCreateFileStoreResponse,
            HandleCreateFileStoreResponse);
        HFunc(
            TEvIndexTablet::TEvUnsafeChangeTabletStateResponse,
            HandleShardCreationStateResponse);
        HFunc(
            TEvServicePrivate::TEvMultiShardCreateResponse,
            HandleMultiShardCreateResponse);
        HFunc(
            TEvServicePrivate::TEvMultiShardConfigureResponse,
            HandleMultiShardConfigureResponse);

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

void TStorageServiceActor::HandleCreateFileStore(
    const TEvService::TEvCreateFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->AccessProfileLogRequest(), msg->Record);

    auto error = ValidateCreateFileSystemRequest(*StorageConfig, msg->Record);
    if (HasError(error)) {
        auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>(
            error);

        InFlightRequests->CompleteAndErase(
            ctx.Now(),
            error,
            *inflight,
            cookie);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TCreateFileStoreActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
