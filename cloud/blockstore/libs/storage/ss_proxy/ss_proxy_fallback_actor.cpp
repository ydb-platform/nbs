#include "ss_proxy_fallback_actor.h"

#include "path_description_backup.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy_events_private.h>

#include <cloud/storage/core/libs/ss_proxy/ss_proxy.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
class TReadPathDescriptionBackupActor final
    : public TActorBootstrapped<TReadPathDescriptionBackupActor<TResponse>>
{
private:
    using TSelf = TReadPathDescriptionBackupActor<TResponse>;
    using TReadCacheRequest =
        TEvSSProxyPrivate::TEvReadPathDescriptionBackupRequest;
    using TReadCacheResponse =
        TEvSSProxyPrivate::TEvReadPathDescriptionBackupResponse;

    const TRequestInfoPtr RequestInfo;
    const TActorId PathDescriptionBackup;
    const TVector<TString> Paths;
    size_t PathIndex = 0;

public:
    TReadPathDescriptionBackupActor(
        TRequestInfoPtr requestInfo,
        TActorId pathDescriptionCache,
        TVector<TString> paths);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReadCache(const TActorContext& ctx);
    void HandleReadCacheResponse(
        const TReadCacheResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TResponse> response);

private:
    STFUNC(StateWork);
};

template <class TResponse>
TReadPathDescriptionBackupActor<TResponse>::TReadPathDescriptionBackupActor(
        TRequestInfoPtr requestInfo,
        TActorId pathDescriptionCache,
        TVector<TString> paths)
    : RequestInfo(std::move(requestInfo))
    , PathDescriptionBackup(std::move(pathDescriptionCache))
    , Paths(std::move(paths))
{}

template <class TResponse>
void TReadPathDescriptionBackupActor<TResponse>::Bootstrap(
    const TActorContext& ctx)
{
    TSelf::Become(&TSelf::StateWork);
    ReadCache(ctx);
}

template <class TResponse>
void TReadPathDescriptionBackupActor<TResponse>::ReadCache(
    const TActorContext& ctx)
{
    auto& path = Paths[PathIndex];
    auto request = std::make_unique<TReadCacheRequest>(std::move(path));
    NCloud::Send(ctx, PathDescriptionBackup, std::move(request));

    ++PathIndex;
}

template <class TResponse>
void TReadPathDescriptionBackupActor<TResponse>::HandleReadCacheResponse(
    const TReadCacheResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    std::unique_ptr<TResponse> response;
    if (HasError(msg->Error)) {
        if (msg->Error.GetCode() == E_NOT_FOUND && PathIndex < Paths.size()) {
            ReadCache(ctx);
            return;
        }

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
            std::move(msg->Path), std::move(msg->PathDescription));
    }

    ReplyAndDie(ctx, std::move(response));
}

template <class TResponse>
void TReadPathDescriptionBackupActor<TResponse>::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    TSelf::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
STFUNC(TReadPathDescriptionBackupActor<TResponse>::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TReadCacheResponse, HandleReadCacheResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSSProxyFallbackActor::TSSProxyFallbackActor(TStorageConfigPtr config)
    : Config(std::move(config))
{}

void TSSProxyFallbackActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    const auto& filepath = Config->GetPathDescriptionBackupFilePath();
    if (filepath) {
        auto cache = std::make_unique<TPathDescriptionBackup>(
            TBlockStoreComponents::SS_PROXY,
            filepath,
            Config->GetUseBinaryFormatForPathDescriptionBackup(),
            /*readOnlyMode=*/true);

        PathDescriptionBackup = ctx.Register(
            cache.release(), TMailboxType::HTSwap, AppData()->IOPoolId);
    }

    auto actor = ::NCloud::NStorage::CreateSSProxy({
        .LogComponent = TBlockStoreComponents::SS_PROXY,
        .PipeClientRetryCount = Config->GetPipeClientRetryCount(),
        .PipeClientMinRetryTime = Config->GetPipeClientMinRetryTime(),
        .PipeClientMaxRetryTime = Config->GetPipeClientMaxRetryTime(),
        .SchemeShardDir = Config->GetSchemeShardDir(),
        .PathDescriptionBackupFilePath = Config->GetPathDescriptionBackupFilePath(),
    });
    StorageSSProxy = NCloud::Register(ctx, std::move(actor));
}

////////////////////////////////////////////////////////////////////////////////

bool TSSProxyFallbackActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStorageSSProxy::TEvDescribeSchemeRequest, HandleDescribeScheme);
        HFunc(TEvStorageSSProxy::TEvModifySchemeRequest, HandleModifyScheme);

        BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvSSProxy)

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
                    TBlockStoreComponents::SS_PROXY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyFallbackActor::HandleCreateVolume(
    const TEvSSProxy::TEvCreateVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvSSProxy::TEvCreateVolumeResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TSSProxyFallbackActor::HandleDescribeScheme(
    const TEvStorageSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvStorageSSProxy::TEvDescribeSchemeResponse;

    if (!PathDescriptionBackup) {
        // should not return fatal error to client
        auto error = MakeError(E_REJECTED, "PathDescriptionBackup is not set");
        auto response = std::make_unique<TResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>()); //TODO

    NCloud::Register<TReadPathDescriptionBackupActor<TResponse>>(
        ctx,
        std::move(requestInfo),
        PathDescriptionBackup,
        TVector<TString>{std::move(msg->Path)});
}

void TSSProxyFallbackActor::HandleDescribeVolume(
    const TEvSSProxy::TEvDescribeVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxy::TEvDescribeVolumeResponse;

    if (!PathDescriptionBackup) {
        // should not return fatal error to client
        auto error = MakeError(E_REJECTED, "PathDescriptionBackup is not set");
        auto response = std::make_unique<TResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TString dir = TStringBuilder() << Config->GetSchemeShardDir() << '/';
    TString path = TStringBuilder() << dir << DiskIdToPath(msg->DiskId);
    // path for volumes with old layout
    TString fallbackPath =
        TStringBuilder() << dir << DiskIdToPathDeprecated(msg->DiskId);

    NCloud::Register<TReadPathDescriptionBackupActor<TResponse>>(
        ctx,
        std::move(requestInfo),
        PathDescriptionBackup,
        TVector<TString>{std::move(path), std::move(fallbackPath)});
}

void TSSProxyFallbackActor::HandleModifyScheme(
    const TEvStorageSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxy));
}

void TSSProxyFallbackActor::HandleModifyVolume(
    const TEvSSProxy::TEvModifyVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvSSProxy::TEvModifyVolumeResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

// void TSSProxyFallbackActor::HandleWaitSchemeTx(
//     const TEvStorageSSProxy::TEvWaitSchemeTxRequest::TPtr& ev,
//     const TActorContext& ctx)
// {
//     ctx.Send(ev->Forward(StorageSSProxy));
// }

void TSSProxyFallbackActor::HandleBackupPathDescriptions(
    const TEvSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxy::TEvBackupPathDescriptionsResponse;

    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response = std::make_unique<TResponse>(std::move(error));
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
