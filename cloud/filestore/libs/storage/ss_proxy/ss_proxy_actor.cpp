#include "ss_proxy_actor.h"

#include <cloud/filestore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/ss_proxy/ss_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TSSProxyActor::TSSProxyActor(TStorageConfigPtr config)
    : Config(std::move(config))
{}

void TSSProxyActor::Bootstrap(const TActorContext& ctx)
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

bool TSSProxyActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStorageSSProxy::TEvDescribeSchemeRequest, HandleDescribeScheme);
        HFunc(TEvStorageSSProxy::TEvModifySchemeRequest, HandleModifyScheme);
        HFunc(TEvStorageSSProxy::TEvBackupPathDescriptionsRequest, HandleBackupPathDescriptions);

        FILESTORE_SS_PROXY_REQUESTS(FILESTORE_HANDLE_REQUEST, TEvSSProxy)

        default:
            return false;
    }

    return true;
}

STFUNC(TSSProxyActor::StateWork)
{
    if (!HandleRequests(ev)) {
        HandleUnexpectedEvent(
            ev,
            TFileStoreComponents::SS_PROXY,
            __PRETTY_FUNCTION__);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDescribeScheme(
    const TEvStorageSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxy));
}

void TSSProxyActor::HandleModifyScheme(
    const TEvStorageSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxy));
}

void TSSProxyActor::HandleBackupPathDescriptions(
    const TEvStorageSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxy));
}

}   // namespace NCloud::NFileStore::NStorage
