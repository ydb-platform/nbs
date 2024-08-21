#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/channel_data_kind.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

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

    NKikimrFileStore::TConfig Config;

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
    STFUNC(StateDescribe);
    STFUNC(StateAlter);

    void DescribeFileStore(const TActorContext& ctx);
    void AlterFileStore(const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
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
{
    Config.SetBlocksCount(request.GetBlocksCount());
    Config.SetVersion(request.GetConfigVersion());
}

void TAlterFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    DescribeFileStore(ctx);
    Become(&TThis::StateDescribe);
}

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

    if (FAILED(msg->GetStatus())) {
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
    const bool allocateMixed0 = thirdChannelDataKind == EChannelDataKind::Mixed0;

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
        SetupFileStorePerformanceAndChannels(
            allocateMixed0,
            *StorageConfig,
            config,
            PerformanceProfile);
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

void TAlterFileStoreActor::AlterFileStore(const TActorContext& ctx)
{
    Become(&TThis::StateAlter);

    auto request = std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(Config);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TAlterFileStoreActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();
    ui32 errorCode = error.GetCode();
    if (FAILED(errorCode)) {
        LOG_ERROR(ctx, TFileStoreComponents::SERVICE,
            "%s of filestore %s failed: %s",
            GetOperationString(),
            Config.GetFileSystemId().Quote().c_str(),
            msg->GetErrorReason().c_str());

        ReplyAndDie(ctx, MakeError(errorCode, error.GetMessage()));
        return;
    }

    ReplyAndDie(ctx);
}

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
        auto response = std::make_unique<TEvService::TEvAlterFileStoreResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    } else {
        auto response = std::make_unique<TEvService::TEvResizeFileStoreResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TAlterFileStoreActor::StateDescribe)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvSSProxy::TEvDescribeFileStoreResponse, HandleDescribeFileStoreResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

STFUNC(TAlterFileStoreActor::StateAlter)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvSSProxy::TEvAlterFileStoreResponse, HandleAlterFileStoreResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
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
