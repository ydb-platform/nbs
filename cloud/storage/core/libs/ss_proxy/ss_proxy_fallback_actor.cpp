#include "ss_proxy_fallback_actor.h"

#include "path_description_backup.h"
#include "ss_proxy_events_private.h"

#include <ydb/core/base/appdata.h>

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

class TDescribeSchemeActor final
    : public TActorBootstrapped<TDescribeSchemeActor>
{
private:
    using TReadBackupRequest =
        TEvSSProxyPrivate::TEvReadPathDescriptionBackupRequest;
    using TReadBackupResponse =
        TEvSSProxyPrivate::TEvReadPathDescriptionBackupResponse;

    const int LogComponent;
    const TRequestInfo RequestInfo;
    const TActorId PathDescriptionBackup;
    const TString Path;

public:
    TDescribeSchemeActor(
        int logComponent,
        TRequestInfo requestInfo,
        TActorId pathDescriptionBackup,
        TString path);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleReadBackupResponse(
        const TReadBackupResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDescribeSchemeResponse> response);

private:
    STFUNC(StateWork);
};

TDescribeSchemeActor::TDescribeSchemeActor(
        int logComponent,
        TRequestInfo requestInfo,
        TActorId pathDescriptionBackup,
        TString path)
    : LogComponent(logComponent)
    , RequestInfo(std::move(requestInfo))
    , PathDescriptionBackup(std::move(pathDescriptionBackup))
    , Path(std::move(path))
{}

void TDescribeSchemeActor::Bootstrap(
    const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<TReadBackupRequest>(Path);
    NCloud::Send(ctx, PathDescriptionBackup, std::move(request));
}

void TDescribeSchemeActor::HandleReadBackupResponse(
    const TReadBackupResponse::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxy::TEvDescribeSchemeResponse;

    const auto* msg = ev->Get();

    std::unique_ptr<TResponse> response;
    if (HasError(msg->Error)) {
        auto error = std::move(msg->Error);
        if (error.GetCode() == E_NOT_FOUND) {
            // should not return fatal error to client
            error = MakeError(
                E_REJECTED,
                "E_NOT_FOUND from PathDescriptionBackup converted to E_REJECTED"
            );
        }

        response = std::make_unique<TResponse>(std::move(error));
    } else {
        response = std::make_unique<TResponse>(
            std::move(msg->Path),
            std::move(msg->PathDescription));
    }

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeSchemeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvDescribeSchemeResponse> response)
{
    NCloud::Reply(ctx, RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeSchemeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TReadBackupResponse, HandleReadBackupResponse);

        default:
            HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSSProxyFallbackActor::TSSProxyFallbackActor(TSSProxyConfig config)
    : Config(std::move(config))
{}

void TSSProxyFallbackActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    if (Config.PathDescriptionBackupFilePath) {
        auto actor = std::make_unique<TPathDescriptionBackup>(
            Config.LogComponent,
            Config.PathDescriptionBackupFilePath,
            true    // readOnlyMode
        );
        PathDescriptionBackup = ctx.Register(
            actor.release(), TMailboxType::HTSwap, AppData()->IOPoolId);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TSSProxyFallbackActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        STORAGE_SS_PROXY_REQUESTS(STORAGE_HANDLE_REQUEST, TEvSSProxy)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSSProxyFallbackActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    Config.LogComponent,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyFallbackActor::HandleDescribeScheme(
    const TEvSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!PathDescriptionBackup) {
        // should not return fatal error to client
        auto error = MakeError(E_REJECTED, "PathDescriptionBackup is not set");
        auto response =
            std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto* msg = ev->Get();

    NCloud::Register<TDescribeSchemeActor>(
        ctx,
        Config.LogComponent,
        TRequestInfo(ev->Sender, ev->Cookie),
        PathDescriptionBackup,
        std::move(msg->Path));
}

void TSSProxyFallbackActor::HandleModifyScheme(
    const TEvSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvSSProxy::TEvModifySchemeResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TSSProxyFallbackActor::HandleWaitSchemeTx(
    const TEvSSProxy::TEvWaitSchemeTxRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvSSProxy::TEvWaitSchemeTxResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TSSProxyFallbackActor::HandleBackupPathDescriptions(
    const TEvSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxy::TEvBackupPathDescriptionsResponse;

    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response = std::make_unique<TResponse>(std::move(error));
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NStorage
