#include "hive_proxy_actor.h"

#include <contrib/ydb/core/base/hive.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NKikimrHive::EDrainDownPolicy ConvertDownPolicy(
    NProto::EDrainDownPolicy downPolicy)
{
    using TDownPolicy = NProto::EDrainDownPolicy;

    constexpr auto MinSentinel = static_cast<TDownPolicy>(
        std::numeric_limits<i32>::min());
    constexpr auto MaxSentinel = static_cast<TDownPolicy>(
        std::numeric_limits<i32>::max());

    switch (downPolicy) {
        case NProto::DRAIN_POLICY_NO_DOWN:
            return NKikimrHive::DRAIN_POLICY_NO_DOWN;
        case NProto::DRAIN_POLICY_KEEP_DOWN_UNTIL_RESTART:
            return NKikimrHive::DRAIN_POLICY_KEEP_DOWN_UNTIL_RESTART;
        case NProto::DRAIN_POLICY_KEEP_DOWN:
            return NKikimrHive::DRAIN_POLICY_KEEP_DOWN;
        case MinSentinel:
        case MaxSentinel:
            Y_ABORT_UNLESS(
                false,
                "Unknown drain down policy: %d",
                static_cast<int>(downPolicy));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDrainNodeRequestActor final
    : public TActorBootstrapped<TDrainNodeRequestActor>
{
private:
    const TActorId Owner;
    const NProto::EDrainDownPolicy DownPolicy;
    const int LogComponent;
    const THiveProxyActor::TRequestInfo Request;
    TActorId ClientId;

public:
    TDrainNodeRequestActor(
            const TActorId& owner,
            NProto::EDrainDownPolicy downPolicy,
            const int logComponent,
            THiveProxyActor::TRequestInfo request,
            TActorId clientId)
        : Owner(owner)
        , DownPolicy(downPolicy)
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
        const TEvHive::TEvDrainNodeResult::TPtr& ev,
        const TActorContext& ctx);

    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

void TDrainNodeRequestActor::Bootstrap(const TActorContext& ctx)
{

    auto ev = std::make_unique<TEvHive::TEvDrainNode>(Owner.NodeId());
    ev->Record.SetDownPolicy(ConvertDownPolicy(DownPolicy));
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
    const TEvHive::TEvDrainNodeResult::TPtr& ev,
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
        HFunc(TEvHive::TEvDrainNodeResult, HandleDrainNodeResult);
        IgnoreFunc(TEvHive::TEvDrainNodeAck);

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
    auto clientId = ClientCache->Prepare(ctx, HiveTabletId);

    HiveState.Actors.insert(NCloud::Register<TDrainNodeRequestActor>(
        ctx,
        SelfId(),
        ev->Get()->DownPolicy,
        LogComponent,
        TRequestInfo(ev->Sender, ev->Cookie),
        clientId
    ));
}

}   // namespace NCloud::NStorage
