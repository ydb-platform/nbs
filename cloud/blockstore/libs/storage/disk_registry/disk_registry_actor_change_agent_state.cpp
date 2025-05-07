#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NMonitoringUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TChangeAgentStateActor final
    : public TActorBootstrapped<TChangeAgentStateActor>
{
private:
    const TActorId Owner;
    const ui64 TabletID;
    const TRequestInfoPtr RequestInfo;
    const TString AgentID;
    const NProto::EAgentState NewState;
    const NProto::EAgentState OldState;

public:
    TChangeAgentStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString agentId,
        NProto::EAgentState newState,
        NProto::EAgentState oldState);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        TString message,
        const EAlertLevel alertLevel);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleChangeAgentStateResponse(
        const TEvDiskRegistry::TEvChangeAgentStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TChangeAgentStateActor::TChangeAgentStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString agentId,
        NProto::EAgentState newState,
        NProto::EAgentState oldState)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , AgentID(std::move(agentId))
    , NewState(newState)
    , OldState(oldState)
{}

void TChangeAgentStateActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvChangeAgentStateRequest>();

    request->Record.SetAgentId(AgentID);
    request->Record.SetAgentState(NewState);
    request->Record.SetReason("monpage action");

    NCloud::Send(ctx, Owner, std::move(request));

    Become(&TThis::StateWork);
}

void TChangeAgentStateActor::Notify(
    const TActorContext& ctx,
    TString message,
    const EAlertLevel alertLevel)
{
    TStringStream out;

    BuildNotifyPageWithRedirect(
        out,
        std::move(message),
        TStringBuilder() << "./app?action=agent&TabletID=" << TabletID
                         << "&AgentID=" << AgentID,
        alertLevel);

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TChangeAgentStateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (!HasError(error)) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder()
                << "failed to change agent[" << AgentID.Quote()
                << "] state from " << EAgentState_Name(OldState) << " to "
                << EAgentState_Name(NewState) << ": " << FormatError(error),
            EAlertLevel::DANGER);
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TChangeAgentStateActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TChangeAgentStateActor::HandleChangeAgentStateResponse(
    const TEvDiskRegistry::TEvChangeAgentStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    ReplyAndDie(ctx, response->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TChangeAgentStateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistry::TEvChangeAgentStateResponse,
            HandleChangeAgentStateResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_ChangeAgentState(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (!Config->GetEnableToChangeStatesFromDiskRegistryMonpage()) {
        RejectHttpRequest(ctx, *requestInfo, "Can't change state from monpage");
        return;
    }

    const auto& newStateRaw = params.Get("NewState");
    const auto& agentId = params.Get("AgentID");

    if (!newStateRaw) {
        RejectHttpRequest(ctx, *requestInfo, "No new state is given");
        return;
    }

    if (!agentId) {
        RejectHttpRequest(ctx, *requestInfo, "No agent id is given");
        return;
    }

    NProto::EAgentState newState;
    if (!EAgentState_Parse(newStateRaw, &newState)) {
        RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
        return;
    }

    switch (newState) {
        case NProto::AGENT_STATE_ONLINE:
        case NProto::AGENT_STATE_WARNING:
            break;
        default:
            RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
            return;
    }

    const auto agentState = State->GetAgentState(agentId);
    if (agentState.Empty()) {
        RejectHttpRequest(ctx, *requestInfo, "Unknown agent");
        return;
    }

    switch (newState) {
        case NProto::AGENT_STATE_ONLINE:
        case NProto::AGENT_STATE_WARNING:
            break;
        default:
            RejectHttpRequest(
                ctx,
                *requestInfo,
                "Can't change agent state from " +
                    EAgentState_Name(*agentState.Get()));
            return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Change state of agent[%s] on monitoring page from %s to %s",
        agentId.Quote().c_str(),
        EAgentState_Name(*agentState.Get()).c_str(),
        EAgentState_Name(newState).c_str());

    auto actor = NCloud::Register<TChangeAgentStateActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        agentId,
        newState,
        *agentState.Get());

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
