#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyFileStoreActor final
    : public TActorBootstrapped<TDestroyFileStoreActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;

public:
    TDestroyFileStoreActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DestroyFileStore(const TActorContext& ctx);
    void HandleDestroyFileStoreResponse(
        const TEvSSProxy::TEvDestroyFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TDestroyFileStoreActor::TDestroyFileStoreActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId)
    : RequestInfo(std::move(requestInfo))
    , FileSystemId(std::move(fileSystemId))
{
    ActivityType = TFileStoreActivities::SERVICE_WORKER;
}

void TDestroyFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    DestroyFileStore(ctx);
    Become(&TThis::StateWork);
}

void TDestroyFileStoreActor::DestroyFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDestroyFileStoreRequest>(
        FileSystemId);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TDestroyFileStoreActor::HandleDestroyFileStoreResponse(
    const TEvSSProxy::TEvDestroyFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TDestroyFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TDestroyFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvDestroyFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TDestroyFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvSSProxy::TEvDestroyFileStoreResponse, HandleDestroyFileStoreResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleDestroyFileStore(
    const TEvService::TEvDestroyFileStoreRequest::TPtr& ev,
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

    auto actor = std::make_unique<TDestroyFileStoreActor>(
        std::move(requestInfo),
        msg->Record.GetFileSystemId());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
