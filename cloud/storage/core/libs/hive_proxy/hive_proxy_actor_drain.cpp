#include "hive_proxy_actor.h"

#include <contrib/ydb/core/base/hive.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDrainNodeRequestActor final
    : public TActorBootstrapped<TDrainNodeRequestActor>
{
private:
    const TActorId Owner;
    const bool KeepDown;
    const int LogComponent;
    const THiveProxyActor::TRequestInfo Request;
    TActorId ClientId;

public:
    TDrainNodeRequestActor(
            const TActorId& owner,
            const bool keepDown,
            const int logComponent,
            THiveProxyActor::TRequestInfo request,
            TActorId clientId)
        : Owner(owner)
        , KeepDown(keepDown)
        , LogComponent(logComponent)
        , Request(request)
        , ClientId(clientId)
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void HandleChangeTabletClient(
        const TEvHiveProxyPrivate::TEvChangeTabletClient::TPtr& ev,
        const TActorContext& ctx);

    void HandleDrainNodeResult(
        const NKikimr::TEvHive::TEvDrainNodeResult::TPtr& ev,
        const TActorContext& ctx);

    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

void TDrainNodeRequestActor::Bootstrap(const TActorContext& ctx)
{

    auto ev = std::make_unique<NKikimr::TEvHive::TEvDrainNode>(Owner.NodeId());
    ev->Record.SetKeepDown(KeepDown);
    NKikimr::NTabletPipe::SendData(
        ctx,
        ClientId,
        ev.release()
    );

    Become(&TThis::StateWork);
}

void TDrainNodeRequestActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvHiveProxy::TEvDrainNodeResponse>(
        std::move(error));
    NCloud::Reply(ctx, Request, std::move(response));
    NCloud::Send<TEvHiveProxyPrivate::TEvRequestFinished>(
        ctx, Owner, 0, 0);
    Die(ctx);
}

void TDrainNodeRequestActor::HandleChangeTabletClient(
    const TEvHiveProxyPrivate::TEvChangeTabletClient::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "pipe reset"));
}

void TDrainNodeRequestActor::HandleDrainNodeResult(
    const NKikimr::TEvHive::TEvDrainNodeResult::TPtr& ev,
    const TActorContext& ctx)
{
    NProto::TError error;

    const auto status = ev->Get()->Record.GetStatus();
    if (status != NKikimrProto::OK) {
        error = MakeError(E_FAIL, TStringBuilder()
            << "unexpected status: " << static_cast<ui32>(status));
    }

    ReplyAndDie(ctx, std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDrainNodeRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvHiveProxyPrivate::TEvChangeTabletClient, HandleChangeTabletClient);
        HFunc(NKikimr::TEvHive::TEvDrainNodeResult, HandleDrainNodeResult);

        default:
            HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleDrainNode(
    const TEvHiveProxy::TEvDrainNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ui64 hive = GetHive(ctx, 0);
    auto clientId = ClientCache->Prepare(ctx, hive);

    HiveStates[hive].Actors.insert(NCloud::Register<TDrainNodeRequestActor>(
        ctx,
        SelfId(),
        ev->Get()->KeepDown,
        LogComponent,
        TRequestInfo(ev->Sender, ev->Cookie),
        clientId
    ));
}

}   // namespace NCloud::NStorage
