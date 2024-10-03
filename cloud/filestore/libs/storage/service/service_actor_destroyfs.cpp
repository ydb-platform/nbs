#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

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
    const bool ForceDestroy;

public:
    TDestroyFileStoreActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        bool forceDestroy);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeSessions(const TActorContext& ctx);
    void HandleDescribeSessionsResponse(
        const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

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
        TString fileSystemId,
        bool forceDestroy)
    : RequestInfo(std::move(requestInfo))
    , FileSystemId(std::move(fileSystemId))
    , ForceDestroy(forceDestroy)
{}

void TDestroyFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    if (ForceDestroy) {
        DestroyFileStore(ctx);
    } else {
        DescribeSessions(ctx);
    }
    Become(&TThis::StateWork);
}

void TDestroyFileStoreActor::DescribeSessions(const TActorContext& ctx)
{
    auto requestToTablet =
        std::make_unique<TEvIndexTablet::TEvDescribeSessionsRequest>();
    requestToTablet->Record.SetFileSystemId(FileSystemId);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TDestroyFileStoreActor::HandleDescribeSessionsResponse(
    const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        if (msg->GetStatus() ==
            MAKE_SCHEMESHARD_ERROR(
                NKikimrScheme::EStatus::StatusPathDoesNotExist))
        {
            ReplyAndDie(
                ctx,
                MakeError(S_FALSE, FileSystemId.Quote() + " does not exist"));
            return;
        }

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (msg->Record.SessionsSize() != 0) {
        ReplyAndDie(
            ctx,
            MakeError(E_REJECTED, "FileStore has active sessions"));
        return;
    }

    DestroyFileStore(ctx);
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

        HFunc(
            TEvIndexTablet::TEvDescribeSessionsResponse,
            HandleDescribeSessionsResponse);

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

    if (Count(
            StorageConfig->GetDestroyFilestoreDenyList(),
            (msg->Record.GetFileSystemId())))
    {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "FileStore %s is in deny list",
            msg->Record.GetFileSystemId().c_str());
        auto response =
            std::make_unique<TEvService::TEvDestroyFileStoreResponse>(MakeError(
                E_ARGUMENT,
                Sprintf(
                    "FileStore %s is in deny list",
                    msg->Record.GetFileSystemId().c_str())));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    bool forceDestroy = msg->Record.GetForceDestroy() &&
                        StorageConfig->GetAllowFileStoreForceDestroy();
    auto actor = std::make_unique<TDestroyFileStoreActor>(
        std::move(requestInfo),
        msg->Record.GetFileSystemId(),
        forceDestroy);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
