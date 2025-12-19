#include "disk_registry_actor.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/string/join.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRestoreAgentsActor final: public TActorBootstrapped<TRestoreAgentsActor>
{
private:
    const TActorId DiskRegistryActorId;
    const TRequestInfoPtr RequestInfo;
    const TDuration RestoreAgentsDelay;

    NProto::TError LastError;
    size_t ResponsesCount = 0;
    size_t RequestsSent = 0;
    TVector<NProto::TAgentInfo> Agents;
    TVector<TString> AffectedAgents;

public:
    TRestoreAgentsActor(
        const TActorId& diskRegistryActorId,
        TRequestInfoPtr requestInfo,
        TDuration restoreAgentsDelay);

    void Bootstrap(const TActorContext& ctx);

private:
    void QueryAgentsList(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleQueryAgentsInfoResponse(
        const TEvService::TEvQueryAgentsInfoResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleQueryAgentsInfoUndelivery(
        const TEvService::TEvQueryAgentsInfoRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleChangeAgentStateResponse(
        const TEvDiskRegistry::TEvChangeAgentStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleChangeAgentStateUndelivery(
        const TEvDiskRegistry::TEvChangeAgentStateRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRestoreAgentsActor::TRestoreAgentsActor(
    const TActorId& diskRegistryActorId,
    TRequestInfoPtr requestInfo,
    TDuration restoreAgentsDelay)
    : DiskRegistryActorId(diskRegistryActorId)
    , RequestInfo(std::move(requestInfo))
    , RestoreAgentsDelay(restoreAgentsDelay)
{}

void TRestoreAgentsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Restoring agents with status \"back from unavailable\" and last state "
        "change more than %s ago",
        FormatDuration(RestoreAgentsDelay).c_str());

    QueryAgentsList(ctx);
}

void TRestoreAgentsActor::QueryAgentsList(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvQueryAgentsInfoRequest>();
    request->Record.MutableFilter()->AddStates(
        NProto::EAgentState::AGENT_STATE_WARNING);

    NCloud::SendWithUndeliveryTracking(
        ctx,
        DiskRegistryActorId,
        std::move(request));
}

void TRestoreAgentsActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvRestoreAgentsToOnlineResponse>(LastError);
    response->AffectedAgents = AffectedAgents;
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TRestoreAgentsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TRestoreAgentsActor::HandleQueryAgentsInfoResponse(
    const TEvService::TEvQueryAgentsInfoResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    Agents = {msg->Record.GetAgents().begin(), msg->Record.GetAgents().end()};

    for (size_t agentIdx = 0; agentIdx < Agents.size(); agentIdx++) {
        const auto& agentInfo = Agents[agentIdx];
        TDuration timeSinceLastChange =
            ctx.Now() - TInstant::MicroSeconds(agentInfo.GetStateTs());

        if (agentInfo.GetStateMessage() == BackFromUnavailableStateMessage &&
            timeSinceLastChange > RestoreAgentsDelay)
        {
            auto request =
                std::make_unique<TEvDiskRegistry::TEvChangeAgentStateRequest>();

            request->Record.SetAgentId(agentInfo.GetAgentId());
            request->Record.SetAgentState(NProto::AGENT_STATE_ONLINE);
            request->Record.SetReason(
                TString(AutomaticallyRestoredStateMessage));

            ++RequestsSent;

            NCloud::SendWithUndeliveryTracking(
                ctx,
                DiskRegistryActorId,
                std::move(request),
                agentIdx);
        }
    }

    if (RequestsSent == 0) {
        ReplyAndDie(ctx);
    }
}

void TRestoreAgentsActor::HandleQueryAgentsInfoUndelivery(
    const TEvService::TEvQueryAgentsInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LastError = MakeError(E_REJECTED, "Failed to fetch agents");

    ReplyAndDie(ctx);
}

void TRestoreAgentsActor::HandleChangeAgentStateResponse(
    const TEvDiskRegistry::TEvChangeAgentStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LastError = msg->GetError();
    } else {
        AffectedAgents.emplace_back(Agents[ev->Cookie].GetAgentId());
    }

    ++ResponsesCount;
    if (ResponsesCount == RequestsSent) {
        ReplyAndDie(ctx);
    }
}

void TRestoreAgentsActor::HandleChangeAgentStateUndelivery(
    const TEvDiskRegistry::TEvChangeAgentStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LastError = MakeError(
        E_REJECTED,
        TStringBuilder() << "ChangeAgentStateRequest to agent "
                         << msg->Record.GetAgentId() << " undelivered");

    ++ResponsesCount;
    if (ResponsesCount == RequestsSent) {
        ReplyAndDie(ctx);
    }
}

STFUNC(TRestoreAgentsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvService::TEvQueryAgentsInfoResponse,
            HandleQueryAgentsInfoResponse);
        HFunc(
            TEvService::TEvQueryAgentsInfoRequest,
            HandleQueryAgentsInfoUndelivery);
        HFunc(
            TEvDiskRegistry::TEvChangeAgentStateResponse,
            HandleChangeAgentStateResponse);
        HFunc(
            TEvDiskRegistry::TEvChangeAgentStateRequest,
            HandleChangeAgentStateUndelivery);

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

void TDiskRegistryActor::HandleRestoreAgentsToOnline(
    const TEvDiskRegistryPrivate::TEvRestoreAgentsToOnlineRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TActorId actorId = NCloud::Register<TRestoreAgentsActor>(
        ctx,
        ctx.SelfID,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        Config->GetAgentBackFromUnavailableToOnlineDelay());
    Actors.insert(actorId);
}

void TDiskRegistryActor::HandleRestoreAgentsToOnlineReadOnly(
    const TEvDiskRegistryPrivate::TEvRestoreAgentsToOnlineRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ScheduleRestoreAgentsToOnline(ctx);
}

void TDiskRegistryActor::HandleRestoreAgentsToOnlineResponse(
    const TEvDiskRegistryPrivate::TEvRestoreAgentsToOnlineResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->AffectedAgents) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Restored agents to online state: %s",
            JoinSeq(", ", msg->AffectedAgents).c_str());
    }

    Actors.erase(ev->Sender);
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Restoring agents from \"back from unavailable\" failed: %s",
            FormatError(msg->GetError()).c_str());
    }

    ScheduleRestoreAgentsToOnline(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
