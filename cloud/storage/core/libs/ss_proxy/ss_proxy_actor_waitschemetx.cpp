#include "ss_proxy_actor.h"

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReplyProxyActor final
    : public TActor<TReplyProxyActor>
{
private:
    const int LogComponent;
    const TActorId Owner;
    const ui64 TabletId;

public:
    TReplyProxyActor(
            int logComponent,
            const TActorId& owner,
            const ui64 tabletId)
        : TActor(&TThis::StateWork)
        , LogComponent(logComponent)
        , Owner(owner)
        , TabletId(tabletId)
    {}

private:
    STFUNC(StateWork);

    void Handle(
        const TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev,
        const TActorContext& ctx);

    void Handle(
        const TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReplyProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);

        default:
            HandleUnexpectedEvent(ev, LogComponent);
            break;
    }
}

void TReplyProxyActor::Handle(
    const TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev,
    const TActorContext& ctx)
{
    // Send response to owner with the correct cookie
    ctx.Send(Owner, ev->Release().Release(), 0, TabletId);
}

void TReplyProxyActor::Handle(
    const TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
    const TActorContext& ctx)
{
    // Send response to owner with the correct cookie
    ctx.Send(Owner, ev->Release().Release(), 0, TabletId);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleWaitSchemeTx(
    const TEvSSProxy::TEvWaitSchemeTxRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto& state = SchemeShardStates[msg->SchemeShardTabletId];
    auto& requests = state.TxToRequests[msg->TxId];

    requests.emplace_back(TRequestInfo(ev->Sender, ev->Cookie));

    if (requests.size() == 1) {
        // This is the first request for this tabletId/txId
        SendWaitTxRequest(ctx, msg->SchemeShardTabletId, msg->TxId);
    }
}

void TSSProxyActor::SendWaitTxRequest(
    const TActorContext& ctx,
    ui64 schemeShard,
    ui64 txId)
{
    auto& state = SchemeShardStates[schemeShard];
    if (!state.ReplyProxy) {
        LOG_DEBUG(ctx, LogComponent,
            "Creating reply proxy actor for schemeshard %lu",
            schemeShard);

        state.ReplyProxy = NCloud::Register(
            ctx,
            std::make_unique<TReplyProxyActor>(
                LogComponent,
                ctx.SelfID,
                schemeShard));
    }

    LOG_DEBUG(ctx, LogComponent,
        "Sending NotifyTxCompletion to %lu for txId# %lu",
        schemeShard,
        txId);

    TActorId clientId = ClientCache->Prepare(ctx, schemeShard);
    NTabletPipe::SendData(
        ctx.MakeFor(state.ReplyProxy),
        clientId,
        new TEvSchemeShard::TEvNotifyTxCompletion(txId));
}

void TSSProxyActor::HandleTxRegistered(
    const TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev,
    const TActorContext& ctx)
{
    ui64 schemeShard = ev->Cookie;

    const auto* msg = ev->Get();
    ui64 txId = msg->Record.GetTxId();

    LOG_DEBUG(ctx, LogComponent,
        "Received NotifyTxCompletionRegistered from %lu for txId# %lu",
        schemeShard,
        txId);
}

void TSSProxyActor::HandleTxResult(
    const TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
    const TActorContext& ctx)
{
    ui64 schemeShard = ev->Cookie;
    auto& state = SchemeShardStates[schemeShard];

    const auto* msg = ev->Get();
    ui64 txId = msg->Record.GetTxId();

    LOG_DEBUG(ctx, LogComponent,
        "Received NotifyTxCompletionResult from %lu for txId# %lu",
        schemeShard,
        txId);

    auto it = state.TxToRequests.find(txId);
    if (it != state.TxToRequests.end()) {
        for (const auto& request : it->second) {
            NCloud::Reply(
                ctx,
                request,
                std::make_unique<TEvSSProxy::TEvWaitSchemeTxResponse>());
        }
        state.TxToRequests.erase(it);
    }
}

}   // namespace NCloud::NStorage
