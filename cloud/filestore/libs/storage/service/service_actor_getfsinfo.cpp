#include "service_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetFileStoreInfoActor final
    : public TActorBootstrapped<TGetFileStoreInfoActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;

public:
    TGetFileStoreInfoActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeFileStore(const TActorContext& ctx);
    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvGetFileStoreInfoResponse> response);
};

////////////////////////////////////////////////////////////////////////////////

TGetFileStoreInfoActor::TGetFileStoreInfoActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId)
    : RequestInfo(std::move(requestInfo))
    , FileSystemId(std::move(fileSystemId))
{}

void TGetFileStoreInfoActor::Bootstrap(const TActorContext& ctx)
{
    DescribeFileStore(ctx);
    Become(&TThis::StateWork);
}

void TGetFileStoreInfoActor::DescribeFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        FileSystemId);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TGetFileStoreInfoActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();
    const auto& config = fileStore.GetConfig();

    auto response = std::make_unique<TEvService::TEvGetFileStoreInfoResponse>();

    auto* fs = response->Record.MutableFileStore();
    Convert(config, *fs);
    Convert(config, *fs->MutablePerformanceProfile());

    ReplyAndDie(ctx, std::move(response));
}

void TGetFileStoreInfoActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TGetFileStoreInfoActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvGetFileStoreInfoResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

void TGetFileStoreInfoActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvGetFileStoreInfoResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TGetFileStoreInfoActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvSSProxy::TEvDescribeFileStoreResponse, HandleDescribeFileStoreResponse);

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

void TStorageServiceActor::HandleGetFileStoreInfo(
    const TEvService::TEvGetFileStoreInfoRequest::TPtr& ev,
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

    auto actor = std::make_unique<TGetFileStoreInfoActor>(
        std::move(requestInfo),
        msg->Record.GetFileSystemId());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
