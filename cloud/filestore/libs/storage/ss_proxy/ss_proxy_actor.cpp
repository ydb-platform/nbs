#include "ss_proxy_actor.h"

#include <cloud/filestore/libs/storage/core/config.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NTabletPipe::IClientCache> CreateTabletPipeClientCache(
    const TStorageConfig& config)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = config.GetPipeClientRetryCount(),
        .MinRetryTime = config.GetPipeClientMinRetryTime(),
        .MaxRetryTime = config.GetPipeClientMaxRetryTime()
    };

    return std::unique_ptr<NTabletPipe::IClientCache>(
        NTabletPipe::CreateUnboundedClientCache(clientConfig));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSSProxyActor::TSSProxyActor(TStorageConfigPtr config)
    : TActor(&TThis::StateWork)
    , Config(std::move(config))
    , ClientCache(CreateTabletPipeClientCache(*Config))
{}

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleConnect(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!ClientCache->OnConnect(ev)) {
        auto error = MakeKikimrError(msg->Status, TStringBuilder()
            << "Connect to schemeshard " << msg->TabletId << " failed");

        OnConnectionError(ctx, error, msg->TabletId);
    }
}

void TSSProxyActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ClientCache->OnDisconnect(ev);

    auto error = MakeError(E_REJECTED, TStringBuilder()
        << "Disconnected from schemeshard " << msg->TabletId);

    OnConnectionError(ctx, error, msg->TabletId);
}

void TSSProxyActor::OnConnectionError(
    const TActorContext& ctx,
    const NProto::TError& error,
    ui64 schemeShard)
{
    Y_UNUSED(error);

    // SchemeShard is a tablet, so it should eventually get up
    // Re-send all outstanding requests
    if (auto* state = SchemeShardStates.FindPtr(schemeShard)) {
        for (const auto& kv : state->TxToRequests) {
            ui64 txId = kv.first;
            SendWaitTxRequest(ctx, schemeShard, txId);
        }
    }
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
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);

        HFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered, HandleTxRegistered);
        HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, HandleTxResult);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, TFileStoreComponents::SS_PROXY);
            }
            break;
    }
}

}   // namespace NCloud::NFileStore::NStorage
