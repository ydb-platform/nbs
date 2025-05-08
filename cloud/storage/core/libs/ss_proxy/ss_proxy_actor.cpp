#include "ss_proxy_actor.h"

#include "path_description_backup.h"

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateTabletPipeClientCache(const TSSProxyConfig& config)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = config.PipeClientRetryCount,
        .MinRetryTime = config.PipeClientMinRetryTime,
        .MaxRetryTime = config.PipeClientMaxRetryTime
    };
    return NTabletPipe::CreateUnboundedClientCache(clientConfig);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSSProxyActor::TSSProxyActor(TSSProxyConfig config)
    : Config(std::move(config))
    , ClientCache(CreateTabletPipeClientCache(Config))
{}

void TSSProxyActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    if (Config.PathDescriptionBackupFilePath) {
        auto cache = std::make_unique<TPathDescriptionBackup>(
            Config.LogComponent,
            Config.PathDescriptionBackupFilePath,
            false); // readOnlyMode
        PathDescriptionBackup = ctx.Register(
            cache.release(),
            TMailboxType::HTSwap,
            AppData()->IOPoolId);
    }
}

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
                HandleUnexpectedEvent(
                    ev,
                    Config.LogComponent,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

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

}   // namespace NCloud::NStorage
