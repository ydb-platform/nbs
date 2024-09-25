#include "ss_proxy_actor.h"

#include <cloud/filestore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/ss_proxy/ss_proxy_actor.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

NTabletPipe::TClientConfig CreateTabletPipeClientConfig(
    const TStorageConfig& config)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = config.GetPipeClientRetryCount(),
        .MinRetryTime = config.GetPipeClientMinRetryTime(),
        .MaxRetryTime = config.GetPipeClientMaxRetryTime()
    };
    return clientConfig;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSSProxyActor::TSSProxyActor(TStorageConfigPtr config)
    : Config(std::move(config))
{}

void TSSProxyActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    auto actor = std::make_unique<::NCloud::NStorage::TSSProxyActor>(
        TFileStoreComponents::SS_PROXY,
        Config->GetSchemeShardDir(),
        CreateTabletPipeClientConfig(*Config)
    );
    StorageSSProxyActor = NCloud::Register(ctx, std::move(actor));
}

////////////////////////////////////////////////////////////////////////////////

bool TSSProxyActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_SS_PROXY_REQUESTS(FILESTORE_HANDLE_REQUEST, TEvSSProxy)

        default:
            return false;
    }

    return true;
}

STFUNC(TSSProxyActor::StateWork)
{
    if (!HandleRequests(ev)) {
        HandleUnexpectedEvent(ev, TFileStoreComponents::SS_PROXY);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDescribeScheme(
    const TEvSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxyActor));
}

void TSSProxyActor::HandleModifyScheme(
    const TEvSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxyActor));
}

void TSSProxyActor::HandleWaitSchemeTx(
    const TEvSSProxy::TEvWaitSchemeTxRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxyActor));
}

}   // namespace NCloud::NFileStore::NStorage
