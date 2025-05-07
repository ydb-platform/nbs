#include "ss_proxy_fallback_actor.h"

#include "path.h"

#include <cloud/filestore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/ss_proxy/ss_proxy.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeFileStoreActor final
    : public TActorBootstrapped<TDescribeFileStoreActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString SchemeShardDir;
    const TActorId StorageSSProxy;
    const TString FileSystemId;

public:
    TDescribeFileStoreActor(
        TRequestInfoPtr requestInfo,
        TString schemeShardDir,
        TActorId storageSSProxy,
        TString fileSystemId);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleDescribeSchemeResponse(
        const TEvStorageSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx);

private:
    STFUNC(StateWork);
};

TDescribeFileStoreActor::TDescribeFileStoreActor(
        TRequestInfoPtr requestInfo,
        TString schemeShardDir,
        TActorId storageSSProxy,
        TString fileSystemId)
    : RequestInfo(std::move(requestInfo))
    , SchemeShardDir(std::move(schemeShardDir))
    , StorageSSProxy(std::move(storageSSProxy))
    , FileSystemId(std::move(fileSystemId))
{}

void TDescribeFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto path = GetFileSystemPath(SchemeShardDir, FileSystemId);
    auto request =
        std::make_unique<TEvStorageSSProxy::TEvDescribeSchemeRequest>(
            std::move(path));
    NCloud::Send(ctx, StorageSSProxy, std::move(request));
}

void TDescribeFileStoreActor::HandleDescribeSchemeResponse(
    const TEvStorageSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response = std::make_unique<TEvSSProxy::TEvDescribeFileStoreResponse>(
        std::move(msg->Error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStorageSSProxy::TEvDescribeSchemeResponse, HandleDescribeSchemeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SS_PROXY,
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

    auto actor = ::NCloud::NStorage::CreateSSProxy({
        .LogComponent = TFileStoreComponents::SS_PROXY,
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

        FILESTORE_SS_PROXY_REQUESTS(FILESTORE_HANDLE_REQUEST, TEvSSProxy)

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
                    TFileStoreComponents::SS_PROXY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyFallbackActor::HandleDescribeScheme(
    const TEvStorageSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxy));
}

void TSSProxyFallbackActor::HandleModifyScheme(
    const TEvStorageSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxy));
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyFallbackActor::HandleDescribeFileStore(
    const TEvSSProxy::TEvDescribeFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    NCloud::Register<TDescribeFileStoreActor>(
        ctx,
        std::move(requestInfo),
        Config->GetSchemeShardDir(),
        StorageSSProxy,
        std::move(msg->FileSystemId));
}

void TSSProxyFallbackActor::HandleCreateFileStore(
    const TEvSSProxy::TEvCreateFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvSSProxy::TEvCreateFileStoreResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TSSProxyFallbackActor::HandleAlterFileStore(
    const TEvSSProxy::TEvAlterFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvSSProxy::TEvAlterFileStoreResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TSSProxyFallbackActor::HandleDestroyFileStore(
    const TEvSSProxy::TEvDestroyFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto error = MakeError(E_NOT_IMPLEMENTED);
    auto response =
        std::make_unique<TEvSSProxy::TEvDestroyFileStoreResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
