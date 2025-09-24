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
// This class actually performs two different actions: alter and resize
// In order not mix them we have two different state functions: AlterStateWork,
// ResizeStateWork. Alter mode is quite simple and consits of the following
// steps: Describe main filestore, Alter main filestore
// The steps of the resize mode:
// 1. Describe main filestore. Gets main file system size, config version,
// calculates desired number of shards
// 2. Get filesystem topology. Gets number of exsitingshards, calculates number
// of shards too be created.
// 3. Describe shards. We need this step to get config version of shards in case
// we need to resize them.
// 4. Alter (actually resize) main filestore.
// 5. Alter shards (if we resize them)
// 6. Create shards if nedded.
// 7. Configure shards if we created some new ones.
// 8. Configure main filestore if new shards were created.
// The end!

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

    NKikimrFileStore::TConfig DesiredConfig;

    TMultiShardFileStoreConfig FileStoreConfig;

    TVector<TString> ExistingShardIds;
    ui32 ShardsToCreate = 0;
    ui32 ShardsToConfigure = 0;
    ui32 ShardsToAlter = 0;
    ui32 ShardsToDescribe = 0;
    // These flags are set by HandleGetFileSystemTopologyResponse.
    bool DirectoryCreationInShardsEnabled = false;
    bool StrictFileSystemSizeEnforcementEnabled = false;
    bool EnableStrictFileSystemSizeEnforcement = false;
    ui32 BlockSize = 0;
    bool IsToConfigureMainFileStore = false;

    const ui64 MainFileStoreCookie = Max<ui64>();

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
    STFUNC(ResizeStateWork);
    STFUNC(AlterStateWork);

    void DescribeMainFileStore(const TActorContext& ctx);
    void DescribeShards(const TActorContext& ctx);
    void AlterFileStore(const TActorContext& ctx);
    void AlterShards(const TActorContext& ctx);
    void GetFileSystemTopology(const TActorContext& ctx);
    void CreateShards(const TActorContext& ctx);
    void ConfigureShards(const TActorContext& ctx);
    void ConfigureMainFileStore(const TActorContext& ctx);

    void HandleDescribeFileStoreForAlterResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

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

    void HandleAlterFileStoreForAlterResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleResizeFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ReportInvaildCookieAndDie(const TActorContext& ctx, ui64 cookie);

    const char* GetOperationString() const
    {
        return !Alter ? "resize" : "alter";
    }

    const TString& GetFileSytemIdByCookie(const ui64 cookie) const
    {
        if (cookie == MainFileStoreCookie) {
            return FileSystemId;
        } else {
            return FileStoreConfig.ShardConfigs[cookie].GetFileSystemId();
        }
    }

    bool IsCookieValid(const ui64 cookie) const
    {
        return cookie == MainFileStoreCookie ||
               cookie < FileStoreConfig.ShardConfigs.size();
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
    DesiredConfig.SetCloudId(request.GetCloudId());
    DesiredConfig.SetFolderId(request.GetFolderId());
    DesiredConfig.SetProjectId(request.GetProjectId());
    DesiredConfig.SetVersion(request.GetConfigVersion());
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
    , EnableStrictFileSystemSizeEnforcement(
          request.GetEnableStrictFileSystemSizeEnforcement())
{
    DesiredConfig.SetBlocksCount(request.GetBlocksCount());
    DesiredConfig.SetVersion(request.GetConfigVersion());
}

void TAlterFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    if (Alter) {
        Become(&TThis::AlterStateWork);
    } else {
        Become(&TThis::ResizeStateWork);
    }

    DescribeMainFileStore(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::DescribeMainFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        FileSystemId);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        MainFileStoreCookie);
}

void TAlterFileStoreActor::DescribeShards(const TActorContext& ctx)
{
    if (ShardsToDescribe == 0) {
        AlterFileStore(ctx);
        return;
    }

    for (ui32 i = 0; i < ShardsToDescribe; ++i) {
        auto request =
            std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
                FileStoreConfig.ShardConfigs[i].GetFileSystemId());

        NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), i);
    }
}

void TAlterFileStoreActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const bool isCookieValid = IsCookieValid(ev->Cookie);
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Describing filestore %s failed: %s",
            FileSystemId.c_str(),
            isCookieValid ? GetFileSytemIdByCookie(ev->Cookie).Quote().c_str()
                          : "\"UNKNOWN\"",
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (!isCookieValid) {
        ReportInvaildCookieAndDie(ctx, ev->Cookie);
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();

    // A shard is described
    if (ev->Cookie != MainFileStoreCookie) {
        FileStoreConfig.ShardConfigs[ev->Cookie].SetVersion(
            fileStore.GetConfig().GetVersion());

        Y_ABORT_UNLESS(ShardsToDescribe);
        if (--ShardsToDescribe == 0) {
            AlterFileStore(ctx);
        }
        return;
    }

    NKikimrFileStore::TConfig currentConfig = fileStore.GetConfig();

    // Allocate legacy mixed0 channel in case it was already present
    Y_ABORT_UNLESS(currentConfig.ExplicitChannelProfilesSize() >= 4);
    const auto thirdChannelDataKind = static_cast<EChannelDataKind>(
        currentConfig.GetExplicitChannelProfiles(3)
            .GetDataKind());
    const bool allocateMixed0 =
        thirdChannelDataKind == EChannelDataKind::Mixed0;

    if (currentConfig.GetBlocksCount() > DesiredConfig.GetBlocksCount() &&
        !Force)
    {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "Cannot decrease filestore size"));
        return;
    }

    Y_ABORT_UNLESS(
        DesiredConfig.GetCloudId().empty() &&
        DesiredConfig.GetFolderId().empty() &&
        DesiredConfig.GetProjectId().empty());

    currentConfig.SetBlocksCount(DesiredConfig.GetBlocksCount());
    if (!allocateMixed0
            && StorageConfig->GetAutomaticShardCreationEnabled())
    {
        FileStoreConfig = SetupMultiShardFileStorePerformanceAndChannels(
            *StorageConfig,
            currentConfig,
            PerformanceProfile,
            ExplicitShardCount);
    } else {
        SetupFileStorePerformanceAndChannels(
            allocateMixed0,
            *StorageConfig,
            currentConfig,
            PerformanceProfile);

        FileStoreConfig.MainFileSystemConfig = currentConfig;
    }

    if (const auto version = DesiredConfig.GetVersion()) {
        FileStoreConfig.MainFileSystemConfig.SetVersion(version);
    }

    BlockSize = currentConfig.GetBlockSize();
    FileStoreConfig.MainFileSystemConfig.ClearBlockSize();

    GetFileSystemTopology(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::AlterFileStore(const TActorContext& ctx)
{
    FileStoreConfig.MainFileSystemConfig.SetAlterTs(ctx.Now().MicroSeconds());
    auto request = std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(
        FileStoreConfig.MainFileSystemConfig);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        MainFileStoreCookie);
}

void TAlterFileStoreActor::AlterShards(const TActorContext& ctx)
{
    if (ShardsToAlter == 0) {
        CreateShards(ctx);
    }

    for (ui32 i = 0; i < ShardsToAlter; ++i) {
        FileStoreConfig.ShardConfigs[i].ClearBlockSize();
        FileStoreConfig.ShardConfigs[i].SetAlterTs(ctx.Now().MicroSeconds());
        auto request = std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(
            FileStoreConfig.ShardConfigs[i]);
        NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), i);
    }
}

void TAlterFileStoreActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const bool isCookieValid = IsCookieValid(ev->Cookie);
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altering of filestore %s failed: %s",
            FileSystemId.c_str(),
            isCookieValid ? GetFileSytemIdByCookie(ev->Cookie).Quote().c_str()
                          : "\"UNKNOWN\"",
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (!isCookieValid) {
        ReportInvaildCookieAndDie(ctx, ev->Cookie);
        return;
    }

    if (ev->Cookie == MainFileStoreCookie) {
        AlterShards(ctx);
        return;
    }

    Y_ABORT_UNLESS(ShardsToAlter);
    if (--ShardsToAlter == 0) {
        CreateShards(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::GetFileSystemTopology(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(!Alter);
 
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
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Getting file system topology failed: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    DirectoryCreationInShardsEnabled =
        msg->Record.GetDirectoryCreationInShardsEnabled();
    StrictFileSystemSizeEnforcementEnabled =
        EnableStrictFileSystemSizeEnforcement ||
        msg->Record.GetStrictFileSystemSizeEnforcementEnabled();

    for (auto& shardId: *msg->Record.MutableShardFileSystemIds()) {
        ExistingShardIds.push_back(std::move(shardId));
    }

    if (FileStoreConfig.ShardConfigs.size() < ExistingShardIds.size()) {
        if (!StrictFileSystemSizeEnforcementEnabled || ExplicitShardCount) {
            ReplyAndDie(
                ctx,
                MakeError(E_ARGUMENT, "Cannot decrease number of shards"));
            return;
        } else {
            // In strict mode we just resize all the shards
            const auto oldSize = FileStoreConfig.ShardConfigs.size();
            Y_ABORT_UNLESS(oldSize);
            FileStoreConfig.ShardConfigs.resize(ExistingShardIds.size());
            for (auto i = oldSize; i < FileStoreConfig.ShardConfigs.size(); ++i)
            {
                FileStoreConfig.ShardConfigs[i] =
                    FileStoreConfig.ShardConfigs[oldSize - 1];
                FileStoreConfig.ShardConfigs[i].SetFileSystemId(
                    ExistingShardIds[i]);
            }
        }
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Will resize filesystem to have %u shards",
        FileSystemId.c_str(),
        FileStoreConfig.ShardConfigs.size());

    const ui32 shardsToCheck = msg->Record.GetShardNo()
                                   ? 0
                                   : Min<ui32>(
                                         ExistingShardIds.size(),
                                         FileStoreConfig.ShardConfigs.size());
    for (ui32 i = 0; i < shardsToCheck; ++i) {
        if (ExistingShardIds[i] !=
            FileStoreConfig.ShardConfigs[i].GetFileSystemId())
        {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_INVALID_STATE,
                    TStringBuilder()
                        << "Shard FileSystemId mismatch at pos " << i << ": "
                        << ExistingShardIds[i] << " != "
                        << FileStoreConfig.ShardConfigs[i].GetFileSystemId()));
            return;
        }
    }

    // Set shards size equal to the file system size in case of
    // StrictFileSystemSizeEnforcement
    if (StrictFileSystemSizeEnforcementEnabled) {
        for (auto& shard: FileStoreConfig.ShardConfigs) {
            shard.SetBlocksCount(DesiredConfig.GetBlocksCount());
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
        Y_ABORT_UNLESS(
            ShardsToCreate == 0 && ShardsToConfigure == 0 &&
            ShardsToAlter == 0 && ShardsToDescribe == 0 &&
            !IsToConfigureMainFileStore);

        FileStoreConfig.ShardConfigs.clear();

    } else {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Will resize filesystem to have %u shards",
            FileSystemId.c_str(),
            FileStoreConfig.ShardConfigs.size());
        ShardsToCreate =
            FileStoreConfig.ShardConfigs.size() - ExistingShardIds.size();
        if (ShardsToCreate || EnableStrictFileSystemSizeEnforcement) {
            IsToConfigureMainFileStore = true;
            ShardsToConfigure = FileStoreConfig.ShardConfigs.size();
        }

        // The shards are resized only if
        // StrictFileSystemSizeEnforcementEnabled, otherwise the shards doesn't
        // change their size.
        if (StrictFileSystemSizeEnforcementEnabled) {
            ShardsToDescribe = ExistingShardIds.size();
            ShardsToAlter = ExistingShardIds.size();
        }
    }

    DescribeShards(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterFileStoreActor::CreateShards(const TActorContext& ctx)
{
    if (ShardsToCreate == 0) {
        ConfigureShards(ctx);
    }

    for (ui32 i = ExistingShardIds.size();
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

    const bool isCookieValid = ev->Cookie < FileStoreConfig.ShardConfigs.size();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard %s creation failed: %s",
            FileSystemId.c_str(),
            isCookieValid ? GetFileSytemIdByCookie(ev->Cookie).Quote().c_str()
                          : "\"UNKNOWN\"",
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

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
    if (ShardsToConfigure == 0) {
        ConfigureMainFileStore(ctx);
        return;
    }

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
    const bool isCookieValid = ev->Cookie < FileStoreConfig.ShardConfigs.size();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Shard %s configuration failed: %s",
            FileSystemId.c_str(),
            isCookieValid ? GetFileSytemIdByCookie(ev->Cookie).Quote().c_str()
                          : "\"UNKNOWN\"",
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (!isCookieValid) {
        ReportInvaildCookieAndDie(ctx, ev->Cookie);
        return;
    }

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
    // As a shard is being resized, we can't send ConfigureShardsRequest to it
    if (!IsToConfigureMainFileStore) {
        ReplyAndDie(ctx);
        return;
    }

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
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring main filesystem failed: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

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

void TAlterFileStoreActor::HandleDescribeFileStoreForAlterResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring main filesystem failed: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();
    auto config = fileStore.GetConfig();

    if (const auto& cloud = DesiredConfig.GetCloudId()) {
        config.SetCloudId(cloud);
    }
    if (const auto& project = DesiredConfig.GetProjectId()) {
        config.SetProjectId(project);
    }
    if (const auto& folder = DesiredConfig.GetFolderId()) {
        config.SetFolderId(folder);
    }
    if (auto version = DesiredConfig.GetVersion()) {
        config.SetVersion(version);
    }

    config.SetAlterTs(ctx.Now().MicroSeconds());
    config.ClearBlockSize();

    FileStoreConfig.MainFileSystemConfig.Swap(&config);

    AlterFileStore(ctx);
}

void TAlterFileStoreActor::HandleAlterFileStoreForAlterResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();
    if (HasError(error)) {
        LOG_ERROR(ctx, TFileStoreComponents::SERVICE,
            "[%s] Altering of main filestore failed: %s",
            GetOperationString(),
            FileSystemId.Quote().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        return;
    }

    ReplyAndDie(ctx, error);
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

void TAlterFileStoreActor::ReportInvaildCookieAndDie(const TActorContext& ctx, ui64 cookie)
{
    ReplyAndDie(
        ctx,
        MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "invalid coockie: " << cookie));
}

STFUNC(TAlterFileStoreActor::AlterStateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreForAlterResponse);
        HFunc(
            TEvSSProxy::TEvAlterFileStoreResponse,
            HandleAlterFileStoreForAlterResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TAlterFileStoreActor::ResizeStateWork)
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
