#include "hive_proxy_actor.h"

#include <ydb/core/base/hive.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReassignRequestActor final
    : public TActorBootstrapped<TReassignRequestActor>
{
private:
    const TActorId Owner;
    const int LogComponent;
    const THiveProxyActor::TRequestInfo Request;
    const ui64 TabletId;
    TVector<ui32> Channels;
    TActorId ClientId;

public:
    TReassignRequestActor(
            const TActorId& owner,
            const int logComponent,
            THiveProxyActor::TRequestInfo request,
            ui64 tabletId,
            TVector<ui32> channels,
            TActorId clientId)
        : Owner(owner)
        , LogComponent(logComponent)
        , Request(std::move(request))
        , TabletId(tabletId)
        , Channels(std::move(channels))
        , ClientId(clientId)
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void HandleChangeTabletClient(
        const TEvHiveProxyPrivate::TEvChangeTabletClient::TPtr& ev,
        const TActorContext& ctx);

    void HandleTabletCreationResult(
        const NKikimr::TEvHive::TEvTabletCreationResult::TPtr& ev,
        const TActorContext& ctx);

    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

void TReassignRequestActor::Bootstrap(const TActorContext& ctx)
{
    NKikimr::NTabletPipe::SendData(
        ctx,
        ClientId,
        new NKikimr::TEvHive::TEvReassignTabletSpace(TabletId, Channels)
    );

    Become(&TThis::StateWork);
}

void TReassignRequestActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvHiveProxy::TEvReassignTabletResponse>(
        std::move(error));
    NCloud::Reply(ctx, Request, std::move(response));
    NCloud::Send<TEvHiveProxyPrivate::TEvRequestFinished>(
        ctx, Owner, TabletId, TabletId);
    Die(ctx);
}

void TReassignRequestActor::HandleChangeTabletClient(
    const TEvHiveProxyPrivate::TEvChangeTabletClient::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "pipe reset"));
}

void TReassignRequestActor::HandleTabletCreationResult(
    const NKikimr::TEvHive::TEvTabletCreationResult::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, {});
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReassignRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvHiveProxyPrivate::TEvChangeTabletClient, HandleChangeTabletClient);
        HFunc(NKikimr::TEvHive::TEvTabletCreationResult, HandleTabletCreationResult);

        default:
            HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleReassignTablet(
    const TEvHiveProxy::TEvReassignTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ui64 hive = GetHive(ctx, msg->TabletId);
    auto clientId = ClientCache->Prepare(ctx, hive);

    HiveStates[hive].Actors.insert(NCloud::Register<TReassignRequestActor>(
        ctx,
        SelfId(),
        LogComponent,
        TRequestInfo(ev->Sender, ev->Cookie),
        msg->TabletId,
        msg->Channels,
        clientId
    ));
}

}   // namespace NCloud::NStorage
