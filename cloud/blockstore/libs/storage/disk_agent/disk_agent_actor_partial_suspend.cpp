#include "disk_agent_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandlePartiallySuspendAgent(
    const TEvDiskAgent::TEvPartiallySuspendAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT, "Received SuspendAgent.");

    if (PartiallySuspendAgentRequestInfo) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentResponse>(
                MakeError(E_REJECTED, "Already suspending.")));
        return;
    }

    if (State->GetPartiallySuspended()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentResponse>(
                MakeError(S_ALREADY, "Already partially suspended.")));
        return;
    }

    const auto* msg = ev->Get();
    if (!msg->Record.GetCancelSuspensionDelay()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentResponse>(
                MakeError(E_ARGUMENT, "Empty CancelSuspensionDelay.")));
        return;
    }

    const auto delay =
        TDuration::MilliSeconds(msg->Record.GetCancelSuspensionDelay());
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Schedule CancelSuspensionRequest at " << delay.ToDeadLine(ctx.Now()));
    ctx.Schedule(delay, new TEvDiskAgentPrivate::TEvCancelSuspensionRequest());
    State->SetPartiallySuspended(true);

    // There is no need in updating session cache for the primary agent since it
    // updates cache on every change anyway.
    if (SessionCacheActor && AgentConfig->GetTemporaryAgent()) {
        PartiallySuspendAgentRequestInfo =
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
        NCloud::Send<TEvDiskAgentPrivate::TEvUpdateSessionCacheRequest>(
            ctx,
            SessionCacheActor,
            0,   // cookie
            State->GetSessions());
    } else {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentResponse>());
    }
}

void TDiskAgentActor::HandleUpdateSessionCacheResponse(
    const TEvDiskAgentPrivate::TEvUpdateSessionCacheResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!PartiallySuspendAgentRequestInfo) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(State->GetPartiallySuspended());
    const auto* msg = ev->Get();
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Session cache update response: " << FormatError(msg->GetError()));

    NCloud::Reply(
        ctx,
        *PartiallySuspendAgentRequestInfo,
        std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentResponse>(
            msg->GetError()));
    PartiallySuspendAgentRequestInfo.Reset();
}

void TDiskAgentActor::HandleCancelSuspension(
    const TEvDiskAgentPrivate::TEvCancelSuspensionRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_WARN(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Partial suspension worn off. Going to register in the DR.");

    State->SetPartiallySuspended(false);
    if (PartiallySuspendAgentRequestInfo) {
        NCloud::Reply(
            ctx,
            *PartiallySuspendAgentRequestInfo,
            std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentResponse>(
                MakeError(E_TIMEOUT, "Couldn't update session cache.")));
        PartiallySuspendAgentRequestInfo.Reset();
    }

    SendRegisterRequest(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
