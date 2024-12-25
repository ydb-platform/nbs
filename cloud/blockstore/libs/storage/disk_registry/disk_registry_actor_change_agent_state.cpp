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

public:
    TChangeAgentStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString agentId,
        NProto::EAgentState newState);

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
        NProto::EAgentState newState)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , AgentID(std::move(agentId))
    , NewState(newState)
{}

void TChangeAgentStateActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvChangeAgentStateRequest>();

    request->Record.SetAgentId(AgentID);
    request->Record.SetAgentState(NewState);

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
        TStringBuilder() << "./app?action=agent&TabletId=" << TabletID
                         << "&AgentID=" << AgentID,
        alertLevel);

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TChangeAgentStateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (SUCCEEDED(error.GetCode())) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder()
                << "failed to change agent[" << AgentID.Quote() << "] state to "
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
    ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
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
                TBlockStoreComponents::DISK_REGISTRY_WORKER);
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

    static const THashSet<NProto::EAgentState> NewStateWhiteList = {
        NProto::EAgentState::AGENT_STATE_ONLINE,
        NProto::EAgentState::AGENT_STATE_WARNING,
    };

    if (!NewStateWhiteList.contains(newState)) {
        RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Change state of agent[%s] from monitoring page to %s",
        agentId.Quote().c_str(),
        EAgentState_Name(newState).c_str());

    auto actor = NCloud::Register<TChangeAgentStateActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        agentId,
        newState);

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
