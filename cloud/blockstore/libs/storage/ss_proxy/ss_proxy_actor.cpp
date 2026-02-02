#include "ss_proxy_actor.h"

#include "path_description_backup.h"

#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/ss_proxy/ss_proxy.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/tx/tx_proxy/proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

const THashSet<ui32> RetriableTxProxyErrors {
    NKikimr::NTxProxy::TResultStatus::ProxyNotReady,
    NKikimr::NTxProxy::TResultStatus::ProxyShardNotAvailable,
    NKikimr::NTxProxy::TResultStatus::ProxyShardTryLater,
    NKikimr::NTxProxy::TResultStatus::ProxyShardOverloaded,
    NKikimr::NTxProxy::TResultStatus::ExecTimeout,
    NKikimr::NTxProxy::TResultStatus::ExecResultUnavailable
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSSProxyActor::TSSProxyActor(TStorageConfigPtr config)
    : Config(std::move(config))
{}

void TSSProxyActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    const auto& filepath = Config->GetPathDescriptionBackupFilePath();
    if (filepath) {
        auto cache = std::make_unique<TPathDescriptionBackup>(
            TBlockStoreComponents::SS_PROXY,
            filepath,
            Config->GetUseBinaryFormatForPathDescriptionBackup(),
            /*readOnlyMode=*/false);

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
    StorageSSProxyActor = NCloud::Register(ctx, std::move(actor));
}

////////////////////////////////////////////////////////////////////////////////

bool TSSProxyActor::HandleRequests(STFUNC_SIG)
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

STFUNC(TSSProxyActor::StateWork)
{
    if (!HandleRequests(ev)) {
        HandleUnexpectedEvent(
            ev,
            TBlockStoreComponents::SS_PROXY,
            __PRETTY_FUNCTION__);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDescribeScheme(
    const TEvStorageSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxyActor));
}

void TSSProxyActor::HandleModifyScheme(
    const TEvStorageSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxyActor));
}

void TSSProxyActor::HandleWaitSchemeTx(
    const TEvStorageSSProxy::TEvWaitSchemeTxRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(StorageSSProxyActor));
}


void TSSProxyActor::HandleBackupPathDescriptions(
    const TEvSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (PathDescriptionBackup) {
        ctx.Send(ev->Forward(PathDescriptionBackup));
    } else {
        auto response =
            std::make_unique<TEvSSProxy::TEvBackupPathDescriptionsResponse>(
                MakeError(S_FALSE));
        NCloud::Reply(ctx, *ev, std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError GetErrorFromPreconditionFailed(const NProto::TError& error)
{
    NProto::TError result = error;
    const auto& msg = error.GetMessage();

    if (msg.Contains("Wrong version in")) {
        // ConfigVersion is different from current one in SchemeShard
        // return E_ABORTED to client to read
        // updated config (StatVolume) and issue new request
        result.SetCode(E_ABORTED);
        result.SetMessage("Config version mismatch");
    } else if (msg.Contains("path version mistmach")) {
        // Just path version mismatch. Return E_REJECTED
        // so durable client will retry request
        result.SetCode(E_REJECTED);
    }
    return result;
}

NProto::TError TranslateTxProxyError(NProto::TError error)
{
    if (FACILITY_FROM_CODE(error.GetCode()) != FACILITY_TXPROXY) {
        return error;
    }

    auto status =
        static_cast<NKikimrScheme::EStatus>(STATUS_FROM_CODE(error.GetCode()));
    if (RetriableTxProxyErrors.count(status)) {
        error.SetCode(E_REJECTED);
    }
    return error;
}

}   // namespace NCloud::NBlockStore::NStorage
