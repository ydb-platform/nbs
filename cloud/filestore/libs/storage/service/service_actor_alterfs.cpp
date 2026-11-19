#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Describes the filestore and alters its config with the ids and the version
// of the request.

class TAlterFileStoreActor final
    : public TActorBootstrapped<TAlterFileStoreActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NProto::TAlterFileStoreRequest Request;

public:
    TAlterFileStoreActor(
        TRequestInfoPtr requestInfo,
        NProto::TAlterFileStoreRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAlterFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TAlterFileStoreActor::TAlterFileStoreActor(
        TRequestInfoPtr requestInfo,
        NProto::TAlterFileStoreRequest request)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TAlterFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        Request.GetFileSystemId());
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TAlterFileStoreActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Describing filestore failed: %s",
            Request.GetFileSystemId().c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();
    auto config = fileStore.GetConfig();

    if (const auto& cloud = Request.GetCloudId()) {
        config.SetCloudId(cloud);
    }
    if (const auto& project = Request.GetProjectId()) {
        config.SetProjectId(project);
    }
    if (const auto& folder = Request.GetFolderId()) {
        config.SetFolderId(folder);
    }
    if (auto version = Request.GetConfigVersion()) {
        config.SetVersion(version);
    }

    config.SetAlterTs(ctx.Now().MicroSeconds());
    config.ClearBlockSize();

    auto request =
        std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(config);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TAlterFileStoreActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altering of main filestore failed: %s",
            Request.GetFileSystemId().Quote().c_str(),
            FormatError(msg->GetError()).Quote().c_str());
    }

    ReplyAndDie(ctx, msg->GetError());
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
    auto response =
        std::make_unique<TEvService::TEvAlterFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

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

    InitProfileLogRequestInfo(inflight->AccessProfileLogRequest(), msg->Record);

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    auto actor = std::make_unique<TAlterFileStoreActor>(
        std::move(requestInfo),
        msg->Record);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
