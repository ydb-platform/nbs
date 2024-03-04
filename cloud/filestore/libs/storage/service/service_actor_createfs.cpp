#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
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

public:
    TCreateFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TCreateFileStoreRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateFileStore(const TActorContext& ctx);
    void HandleCreateFileStoreResponse(
        const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
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
{}

void TCreateFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    CreateFileStore(ctx);
    Become(&TThis::StateWork);
}

void TCreateFileStoreActor::CreateFileStore(const TActorContext& ctx)
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

    SetupFileStorePerformanceAndChannels(
        false,  // do not allocate mixed0 channel
        *StorageConfig,
        config,
        Request.GetPerformanceProfile());

    auto request = std::make_unique<TEvSSProxy::TEvCreateFileStoreRequest>(
        std::move(config));

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TCreateFileStoreActor::HandleCreateFileStoreResponse(
    const TEvSSProxy::TEvCreateFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>();
    // TODO: fill filestore info

    ReplyAndDie(ctx, std::move(response));
}

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
    auto response = std::make_unique<TEvService::TEvCreateFileStoreResponse>(error);
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

        HFunc(TEvSSProxy::TEvCreateFileStoreResponse, HandleCreateFileStoreResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
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
    if (FAILED(error.GetCode())) {
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
