#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateCreateFileSystemRequest(
    const NProto::TCreateFileStoreRequest& request)
{
    const auto& fileSystemId = request.GetFileSystemId();
    if (!fileSystemId) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "missing file system identifier");
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

    ui32 blockSize = request.GetBlockSize();
    if (!blockSize
            || !IsAligned(blockSize, 4_KB)
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

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TCreateFileStoreActor final
    : public TActorBootstrapped<TCreateFileStoreActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TCreateFileStoreRequest Request;
    const TString& LogTag;
    TMultiShardFileStoreConfig FileStoreConfig;

    bool MainFileSystemCreated = false;
    ui32 ShardsToCreate = 0;
    ui32 ShardsToConfigure = 0;

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
    void ConfigureShards(const TActorContext& ctx);
    void ConfigureMainFileStore(const TActorContext& ctx);

    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigureShardResponse(
        const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigureMainFileStoreResponse(
        const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvCreateFileStoreResponse> response);
};

////////////////////////////////////////////////////////////////////////////////

TCreateFileStoreActor::TCreateFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TCreateFileStoreRequest request)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , LogTag(Request.GetFileSystemId())
{}

void TCreateFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    CreateMainFileStore(ctx);
    Become(&TThis::StateWork);
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

    if (StorageConfig->GetAutomaticShardCreationEnabled()) {
        FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            config,
            Request.GetPerformanceProfile(),
            Request.GetShardCount());
        ShardsToCreate = FileStoreConfig.ShardConfigs.size();
        ShardsToConfigure = ShardsToCreate;
        config = FileStoreConfig.MainFileSystemConfig;

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Will create filesystem with %u shards",
            LogTag.c_str(),
            FileStoreConfig.ShardConfigs.size());
    } else {
        SetupFileStorePerformanceAndChannels(
            false,  // do not allocate mixed0 channel
            *StorageConfig,
            config,
            Request.GetPerformanceProfile());

        FileStoreConfig.MainFileSystemConfig = config;
    }

    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        std::move(config));

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TCreateFileStoreActor::CreateShards(const TActorContext& ctx)
{
    for (ui32 i = 0; i < FileStoreConfig.ShardConfigs.size(); ++i) {
        auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
            FileStoreConfig.ShardConfigs[i]);

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Creating shard %s",
            LogTag.c_str(),
            request->Config.GetFileSystemId().c_str());

        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(request),
            i // cookie
        );
    }
}

void TCreateFileStoreActor::ConfigureShards(const TActorContext& ctx)
{
    for (ui32 i = 0; i < FileStoreConfig.ShardConfigs.size(); ++i) {
        auto request =
            std::make_unique<TEvIndexTablet::TEvConfigureAsShardRequest>();
        request->Record.SetFileSystemId(
            FileStoreConfig.ShardConfigs[i].GetFileSystemId());
        request->Record.SetShardNo(i + 1);
        request->Record.SetMainFileSystemId(Request.GetFileSystemId());
        request->Record.SetDirectoryCreationInShardsEnabled(
            StorageConfig->GetDirectoryCreationInShardsEnabled());
        request->Record.SetStrictFileSystemSizeEnforcementEnabled(
            StorageConfig->GetStrictFileSystemSizeEnforcementEnabled());

        if (StorageConfig->GetDirectoryCreationInShardsEnabled() ||
            StorageConfig->GetStrictFileSystemSizeEnforcementEnabled())
        {
            for (const auto& shard: FileStoreConfig.ShardConfigs) {
                request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
            }
        }

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring shard %s",
            LogTag.c_str(),
            request->Record.Utf8DebugString().Quote().c_str());

        NCloud::Send(
            ctx,
            MakeIndexTabletProxyServiceId(),
            std::move(request),
            i // cookie
        );
    }
}

void TCreateFileStoreActor::ConfigureMainFileStore(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureShardsRequest>();
    request->Record.SetFileSystemId(
        FileStoreConfig.MainFileSystemConfig.GetFileSystemId());
    request->Record.SetDirectoryCreationInShardsEnabled(
        StorageConfig->GetDirectoryCreationInShardsEnabled());
    request->Record.SetStrictFileSystemSizeEnforcementEnabled(
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled());

    for (const auto& shard: FileStoreConfig.ShardConfigs) {
        request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring main filesystem %s",
        LogTag.c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::HandleCreateFileStoreResponse(
    const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Filesystem creation error: %s",
            LogTag.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (MainFileSystemCreated) {
        Y_ABORT_UNLESS(ev->Cookie < FileStoreConfig.ShardConfigs.size());

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Created shard %s",
            LogTag.c_str(),
            FileStoreConfig.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

        Y_DEBUG_ABORT_UNLESS(ShardsToCreate);
        if (--ShardsToCreate == 0) {
            ConfigureShards(ctx);
        }

        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Created main filesystem",
        LogTag.c_str());

    MainFileSystemCreated = true;
    if (ShardsToCreate) {
        CreateShards(ctx);
        return;
    }
    if (StorageConfig->GetDirectoryCreationInShardsEnabled() ||
        StorageConfig->GetStrictFileSystemSizeEnforcementEnabled())
    {
        // If no shards are to be created, but directory sharding or allocation
        // of shards of filesystem size is enabled, we need to configure the
        // main filestore in order to set the DirectoryCreationInShardsEnabled
        // flag in the main filestore
        ConfigureMainFileStore(ctx);
        return;
    }

    auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>();
    // TODO: fill filestore info

    ReplyAndDie(ctx, std::move(response));
}

void TCreateFileStoreActor::HandleConfigureShardResponse(
    const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard configuration error: %s",
            LogTag.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Y_ABORT_UNLESS(ev->Cookie < FileStoreConfig.ShardConfigs.size());

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured shard %s",
        LogTag.c_str(),
        FileStoreConfig.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

    Y_DEBUG_ABORT_UNLESS(ShardsToConfigure);
    if (--ShardsToConfigure == 0) {
        ConfigureMainFileStore(ctx);
    }
}

void TCreateFileStoreActor::HandleConfigureMainFileStoreResponse(
    const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
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
        "[%s] Configured main filesystem",
        LogTag.c_str());

    auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>();
    // TODO: fill filestore info

    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TCreateFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TCreateFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvCreateFileStoreResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

void TCreateFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvCreateFileStoreResponse> response)
{
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
            TEvIndexTablet::TEvConfigureAsShardResponse,
            HandleConfigureShardResponse);
        HFunc(
            TEvIndexTablet::TEvConfigureShardsResponse,
            HandleConfigureMainFileStoreResponse);

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

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto error = ValidateCreateFileSystemRequest(msg->Record);
    if (HasError(error)) {
        auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>(error);
        inflight->Complete(ctx.Now(), error);
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
