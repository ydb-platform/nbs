#include "ss_proxy_actor.h"

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

////////////////////////////////////////////////////////////////////////////////

TSSProxyActor::TSSProxyActor(
        int logComponent,
        TString schemeShardDir,
        NKikimr::NTabletPipe::TClientConfig pipeClientConfig)
    : TActor(&TThis::StateWork)
    , LogComponent(logComponent)
    , SchemeShardDir(schemeShardDir)
    , ClientCache(NTabletPipe::CreateUnboundedClientCache(pipeClientConfig))
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
        STORAGE_SS_PROXY_REQUESTS(STORAGE_HANDLE_REQUEST, TEvSSProxy)

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
                HandleUnexpectedEvent(ev, LogComponent);
            }
            break;
    }
}

}   // namespace NCloud::NStorage
