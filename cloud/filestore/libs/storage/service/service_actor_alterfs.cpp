#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/channel_data_kind.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAlterFileStoreActor final
    : public TActorBootstrapped<TAlterFileStoreActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;
    const NProto::TFileStorePerformanceProfile PerformanceProfile;
    const bool Alter;
    const bool Force;
    const ui32 ExplicitShardCount;

    NKikimrFileStore::TConfig Config;

    TMultiShardFileStoreConfig FileStoreConfig;

    TVector<TString> ShardIds;
    ui32 ShardsToCreate = 0;
    ui32 ShardsToConfigure = 0;
    // These flags are set by HandleGetFileSystemTopologyResponse.
    bool DirectoryCreationInShardsEnabled = false;
    bool StrictFileSystemSizeEnforcementEnabled = false;

public:
    TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TAlterFileStoreRequest& request);

    TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TResizeFileStoreRequest& request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeFileStore(const TActorContext& ctx);
    void AlterFileStore(const TActorContext& ctx);
    void GetFileSystemTopology(const TActorContext& ctx);
    void CreateShards(const TActorContext& ctx);
    void ConfigureShards(const TActorContext& ctx);
    void ConfigureMainFileStore(const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetFileSystemTopologyResponse(
        const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigureShardResponse(
        const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigureMainFileStoreResponse(
        const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAlterFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    const char* GetOperationString() const
    {
        return Config.GetBlocksCount() > 0 ? "resize" : "alter";
    }
};

////////////////////////////////////////////////////////////////////////////////

TAlterFileStoreActor::TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TAlterFileStoreRequest& request)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , FileSystemId(request.GetFileSystemId())
    , Alter(true)
    , Force(false)
    , ExplicitShardCount(0)
{
    Config.SetCloudId(request.GetCloudId());
    Config.SetFolderId(request.GetFolderId());
    Config.SetProjectId(request.GetProjectId());
    Config.SetVersion(request.GetConfigVersion());
}

TAlterFileStoreActor::TAlterFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        const NProto::TResizeFileStoreRequest& request)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , FileSystemId(request.GetFileSystemId())
    , PerformanceProfile(request.GetPerformanceProfile())
    , Alter(false)
    , Force(request.GetForce())
    , ExplicitShardCount(request.GetShardCount())
{
    Config.SetBlocksCount(request.GetBlocksCount());
    Config.SetVersion(request.GetConfigVersion());
}

void TAlterFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    DescribeFileStore(ctx);
    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::DescribeFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        FileSystemId);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TAlterFileStoreActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();
    auto config = fileStore.GetConfig();

    // Allocate legacy mixed0 channel in case it was already present
    Y_ABORT_UNLESS(config.ExplicitChannelProfilesSize() >= 4);
    const auto thirdChannelDataKind = static_cast<EChannelDataKind>(config
        .GetExplicitChannelProfiles(3)
        .GetDataKind());
    const bool allocateMixed0 =
        thirdChannelDataKind == EChannelDataKind::Mixed0;

    if (!Alter) {
        if (config.GetBlocksCount() > Config.GetBlocksCount() && !Force) {
            ReplyAndDie(
                ctx,
                MakeError(E_ARGUMENT, "Cannot decrease filestore size"));
            return;
        }

        Y_ABORT_UNLESS(
            Config.GetCloudId().empty() &&
            Config.GetFolderId().empty() &&
            Config.GetProjectId().empty());

        config.SetBlocksCount(Config.GetBlocksCount());
        if (!allocateMixed0
                && StorageConfig->GetAutomaticShardCreationEnabled())
        {
            FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
                *StorageConfig,
                config,
                PerformanceProfile,
                ExplicitShardCount);
            ShardsToCreate = FileStoreConfig.ShardConfigs.size();
            config = FileStoreConfig.MainFileSystemConfig;
        } else {
            SetupFileStorePerformanceAndChannels(
                allocateMixed0,
                *StorageConfig,
                config,
                PerformanceProfile);
        }
    } else {
        if (const auto& cloud = Config.GetCloudId()) {
            config.SetCloudId(cloud);
        }
        if (const auto& project = Config.GetProjectId()) {
            config.SetProjectId(project);
        }
        if (const auto& folder = Config.GetFolderId()) {
            config.SetFolderId(folder);
        }
    }

    if (auto version = Config.GetVersion()) {
        config.SetVersion(version);
    }

    config.SetAlterTs(ctx.Now().MicroSeconds());
    config.ClearBlockSize();

    Config.Swap(&config);
    AlterFileStore(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::AlterFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(Config);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TAlterFileStoreActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();
    if (HasError(error)) {
        LOG_ERROR(ctx, TFileStoreComponents::SERVICE,
            "%s of filestore %s failed: %s",
            GetOperationString(),
            Config.GetFileSystemId().Quote().c_str(),
            msg->GetErrorReason().c_str());

        ReplyAndDie(ctx, error);
        return;
    }

    if (ShardsToCreate) {
        GetFileSystemTopology(ctx);
        return;
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::GetFileSystemTopology(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvGetFileSystemTopologyRequest>();
    request->Record.SetFileSystemId(FileSystemId);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

void TAlterFileStoreActor::HandleGetFileSystemTopologyResponse(
    const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] GetFileSystemTopology error: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] AlterFileStore GetFileSystemTopology response: %s",
        FileSystemId.c_str(),
        msg->Record.Utf8DebugString().Quote().c_str());

    for (auto& shardId: *msg->Record.MutableShardFileSystemIds()) {
        ShardIds.push_back(std::move(shardId));
    }
    DirectoryCreationInShardsEnabled =
        msg->Record.GetDirectoryCreationInShardsEnabled();
    StrictFileSystemSizeEnforcementEnabled =
        msg->Record.GetStrictFileSystemSizeEnforcementEnabled();

    ui32 shardsToCheck =
        msg->Record.GetShardNo()
            ? 0
            : Min<ui32>(ShardIds.size(), FileStoreConfig.ShardConfigs.size());
    for (ui32 i = 0; i < shardsToCheck; ++i) {
        if (ShardIds[i] != FileStoreConfig.ShardConfigs[i].GetFileSystemId()) {
            ReplyAndDie(ctx, MakeError(E_INVALID_STATE, TStringBuilder()
                << "Shard FileSystemId mismatch at pos " << i << ": "
                << ShardIds[i] << " != "
                << FileStoreConfig.ShardConfigs[i].GetFileSystemId()));
            return;
        }
    }

    if (msg->Record.GetShardNo()) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] GetFileSystemTopology - resized a shard (%u), no subshards"
            " will be added",
            FileSystemId.c_str(),
            msg->Record.GetShardNo());
        ShardsToCreate = 0;
        ShardsToConfigure = 0;
    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Will resize filesystem to have %u shards",
            FileSystemId.c_str(),
            FileStoreConfig.ShardConfigs.size());
        ShardsToCreate -= Min<ui32>(ShardsToCreate, ShardIds.size());
        ShardsToConfigure = FileStoreConfig.ShardConfigs.size();
    }

    if (ShardsToCreate) {
        CreateShards(ctx);
        return;
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::CreateShards(const TActorContext& ctx)
{
    for (ui32 i = ShardIds.size();
            i < FileStoreConfig.ShardConfigs.size(); ++i)
    {
        auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
            FileStoreConfig.ShardConfigs[i]);

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Creating shard %s",
            FileSystemId.c_str(),
            request->Config.GetFileSystemId().c_str());

        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(request),
            i // cookie
        );
    }
}

void TAlterFileStoreActor::HandleCreateFileStoreResponse(
    const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Filesystem creation error: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Y_ABORT_UNLESS(ev->Cookie < FileStoreConfig.ShardConfigs.size());

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Created shard %s",
        FileSystemId.c_str(),
        FileStoreConfig.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

    Y_DEBUG_ABORT_UNLESS(ShardsToCreate);
    if (--ShardsToCreate == 0) {
        ConfigureShards(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::ConfigureShards(const TActorContext& ctx)
{
    for (ui32 i = 0; i < FileStoreConfig.ShardConfigs.size(); ++i) {
        auto request =
            std::make_unique<TEvIndexTablet::TEvConfigureAsShardRequest>();
        request->Record.SetFileSystemId(
            FileStoreConfig.ShardConfigs[i].GetFileSystemId());
        request->Record.SetShardNo(i + 1);
        request->Record.SetMainFileSystemId(FileSystemId);
        request->Record.SetDirectoryCreationInShardsEnabled(
            DirectoryCreationInShardsEnabled);
        request->Record.SetStrictFileSystemSizeEnforcementEnabled(
            StrictFileSystemSizeEnforcementEnabled);

        if (DirectoryCreationInShardsEnabled ||
            StrictFileSystemSizeEnforcementEnabled)
        {
            for (const auto& shard: FileStoreConfig.ShardConfigs) {
                request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
            }
        }

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring shard %s",
            FileSystemId.c_str(),
            request->Record.Utf8DebugString().Quote().c_str());

        NCloud::Send(
            ctx,
            MakeIndexTabletProxyServiceId(),
            std::move(request),
            i // cookie
        );
    }
}

void TAlterFileStoreActor::HandleConfigureShardResponse(
    const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard configuration error: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Y_ABORT_UNLESS(ev->Cookie < FileStoreConfig.ShardConfigs.size());

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configured shard %s",
        FileSystemId.c_str(),
        FileStoreConfig.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

    Y_DEBUG_ABORT_UNLESS(ShardsToConfigure);
    if (--ShardsToConfigure == 0) {
        ConfigureMainFileStore(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::ConfigureMainFileStore(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvConfigureShardsRequest>();
    request->Record.SetFileSystemId(
        FileStoreConfig.MainFileSystemConfig.GetFileSystemId());
    request->Record.SetDirectoryCreationInShardsEnabled(
        DirectoryCreationInShardsEnabled);
    request->Record.SetStrictFileSystemSizeEnforcementEnabled(
        StrictFileSystemSizeEnforcementEnabled);

    for (const auto& shard: FileStoreConfig.ShardConfigs) {
        request->Record.AddShardFileSystemIds(shard.GetFileSystemId());
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring main filesystem %s",
        FileSystemId.c_str(),
        request->Record.Utf8DebugString().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

void TAlterFileStoreActor::HandleConfigureMainFileStoreResponse(
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
        FileSystemId.c_str());

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TAlterFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (Alter) {
        auto response =
            std::make_unique<TEvService::TEvAlterFileStoreResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    } else {
        auto response =
            std::make_unique<TEvService::TEvResizeFileStoreResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TAlterFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreResponse);
        HFunc(
            TEvSSProxy::TEvAlterFileStoreResponse,
            HandleAlterFileStoreResponse);
        HFunc(
            TEvIndexTablet::TEvGetFileSystemTopologyResponse,
            HandleGetFileSystemTopologyResponse);
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

void TStorageServiceActor::HandleAlterFileStore(
    const TEvService::TEvAlterFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TAlterFileStoreActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

void TStorageServiceActor::HandleResizeFileStore(
    const TEvService::TEvResizeFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TAlterFileStoreActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
