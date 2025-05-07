#include "hive_proxy_fallback_actor.h"

#include "tablet_boot_info_backup.h"

#include <contrib/ydb/core/base/appdata.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequestInfo
{
    NActors::TActorId Sender;
    ui64 Cookie = 0;

    TRequestInfo(NActors::TActorId sender, ui64 cookie)
        : Sender(sender)
        , Cookie(cookie)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TReadTabletBootInfoBackupActor final
    : public TActorBootstrapped<TReadTabletBootInfoBackupActor>
{
private:
    using TRequest =
        TEvHiveProxyPrivate::TEvReadTabletBootInfoBackupRequest;
    using TResponse =
        TEvHiveProxyPrivate::TEvReadTabletBootInfoBackupResponse;

    using TReply = std::function<
        void(const TActorContext&, std::unique_ptr<TResponse>)
    >;

    int LogComponent;
    TActorId TabletBootInfoBackup;
    ui64 TabletId;
    TReply Reply;

public:
    TReadTabletBootInfoBackupActor(
            int logComponent,
            TActorId tabletBootInfoCache,
            ui64 tabletId,
            TReply reply)
        : LogComponent(logComponent)
        , TabletBootInfoBackup(std::move(tabletBootInfoCache))
        , TabletId(tabletId)
        , Reply(std::move(reply))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateWork);
        Request(ctx);
    }

private:
    void Request(const TActorContext& ctx)
    {
        auto request = std::make_unique<TRequest>(TabletId);
        NCloud::Send(ctx, TabletBootInfoBackup, std::move(request));
    }

    void HandleResponse(
        const TResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        std::unique_ptr<TResponse> response;
        if (HasError(msg->Error)) {
            auto error = msg->Error;
            if (error.GetCode() == E_NOT_FOUND) {
                // should not return fatal error to client
                error = MakeError(
                    E_REJECTED,
                    "E_NOT_FOUND from TabletBootInfoBackup converted to E_REJECTED"
                );
            }

            response = std::make_unique<TResponse>(std::move(error));
        } else {
            response = std::make_unique<TResponse>(
                std::move(msg->StorageInfo), msg->SuggestedGeneration);
        }

        Reply(ctx, std::move(response));
        Die(ctx);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);

            default:
                HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
                break;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

THiveProxyFallbackActor::THiveProxyFallbackActor(THiveProxyConfig config)
    : Config(std::move(config))
{}

void THiveProxyFallbackActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    if (Config.TabletBootInfoBackupFilePath) {
        auto cache = std::make_unique<TTabletBootInfoBackup>(
            Config.LogComponent,
            Config.TabletBootInfoBackupFilePath,
            true /* readOnlyMode */
        );
        TabletBootInfoBackup = ctx.Register(
            cache.release(), TMailboxType::HTSwap, AppData()->IOPoolId);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool THiveProxyFallbackActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        STORAGE_HIVE_PROXY_REQUESTS(STORAGE_HANDLE_REQUEST, TEvHiveProxy)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THiveProxyFallbackActor::StateWork)
{
    if (!HandleRequests(ev)) {
        LogUnexpectedEvent(ev, Config.LogComponent);
    }
}

////////////////////////////////////////////////////////////////////////////////

void THiveProxyFallbackActor::HandleLockTablet(
    const TEvHiveProxy::TEvLockTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvHiveProxy::TEvLockTabletResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void THiveProxyFallbackActor::HandleUnlockTablet(
    const TEvHiveProxy::TEvUnlockTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvHiveProxy::TEvUnlockTabletResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void THiveProxyFallbackActor::HandleGetStorageInfo(
    const TEvHiveProxy::TEvGetStorageInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvHiveProxy::TEvGetStorageInfoResponse;

    if (!TabletBootInfoBackup) {
        // should not return fatal error to client
        auto error = MakeError(E_REJECTED, "TabletBootInfoBackup is not set");
        auto response = std::make_unique<TResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto* msg = ev->Get();

    auto requestInfo = TRequestInfo(ev->Sender, ev->Cookie);
    auto reply = [=](const auto& ctx, auto r) {
        if (HasError(r->Error)) {
            NCloud::Reply(
                ctx,
                requestInfo,
                std::make_unique<TResponse>(r->Error)
            );
            return;
        }

        auto response = std::make_unique<TResponse>(std::move(r->StorageInfo));
        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    NCloud::Register<TReadTabletBootInfoBackupActor>(
        ctx,
        Config.LogComponent,
        TabletBootInfoBackup,
        msg->TabletId,
        std::move(reply));
}

void THiveProxyFallbackActor::HandleBootExternal(
    const TEvHiveProxy::TEvBootExternalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvHiveProxy::TEvBootExternalResponse;

    if (!TabletBootInfoBackup) {
        // should not return fatal error to client
        auto error = MakeError(E_REJECTED, "TabletBootInfoBackup is not set");
        auto response = std::make_unique<TResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto* msg = ev->Get();

    auto requestInfo = TRequestInfo(ev->Sender, ev->Cookie);
    auto reply = [=](const auto& ctx, auto r) {
        if (HasError(r->Error)) {
            NCloud::Reply(
                ctx,
                requestInfo,
                std::make_unique<TResponse>(r->Error)
            );
            return;
        }

        // increment suggested generation to ensure that the tablet does not get
        // stuck with an outdated generation, no matter what
        auto request = std::make_unique<
            TEvHiveProxyPrivate::TEvUpdateTabletBootInfoBackupRequest>(
               r->StorageInfo,
               r->SuggestedGeneration + 1
            );
        NCloud::Send(ctx, TabletBootInfoBackup, std::move(request));

        auto response = std::make_unique<TResponse>(
            std::move(r->StorageInfo),
            r->SuggestedGeneration,
            TEvHiveProxy::TEvBootExternalResponse::EBootMode::MASTER,
            0  // SlaveId
        );
        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    NCloud::Register<TReadTabletBootInfoBackupActor>(
        ctx,
        Config.LogComponent,
        TabletBootInfoBackup,
        msg->TabletId,
        std::move(reply));
}

void THiveProxyFallbackActor::HandleReassignTablet(
    const TEvHiveProxy::TEvReassignTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvHiveProxy::TEvReassignTabletResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void THiveProxyFallbackActor::HandleCreateTablet(
    const TEvHiveProxy::TEvCreateTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvHiveProxy::TEvCreateTabletResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void THiveProxyFallbackActor::HandleLookupTablet(
    const TEvHiveProxy::TEvLookupTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvHiveProxy::TEvLookupTabletResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void THiveProxyFallbackActor::HandleDrainNode(
    const TEvHiveProxy::TEvDrainNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvHiveProxy::TEvDrainNodeResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void THiveProxyFallbackActor::HandleBackupTabletBootInfos(
    const TEvHiveProxy::TEvBackupTabletBootInfosRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (TabletBootInfoBackup) {
        ctx.Send(ev->Forward(TabletBootInfoBackup));
    } else {
        auto response =
            std::make_unique<TEvHiveProxy::TEvBackupTabletBootInfosResponse>(
                MakeError(S_FALSE));
        NCloud::Reply(ctx, *ev, std::move(response));
    }
}

void THiveProxyFallbackActor::HandleListTabletBootInfoBackups(
    const TEvHiveProxy::TEvListTabletBootInfoBackupsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (TabletBootInfoBackup) {
        ctx.Send(ev->Forward(TabletBootInfoBackup));
    } else {
        auto response = std::make_unique<
            TEvHiveProxy::TEvListTabletBootInfoBackupsResponse>(
            MakeError(S_FALSE));
        NCloud::Reply(ctx, *ev, std::move(response));
    }
}

}   // namespace NCloud::NStorage
