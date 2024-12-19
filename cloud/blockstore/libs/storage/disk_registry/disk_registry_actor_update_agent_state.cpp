#include "disk_registry_actor.h"
#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleChangeAgentState(
    const TEvDiskRegistry::TEvChangeAgentStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ChangeAgentState);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ChangeAgentState request: AgentId=%s, State=%u",
        TabletID(),
        msg->Record.GetAgentId().c_str(),
        static_cast<ui32>(msg->Record.GetAgentState()));

    ExecuteTx<TUpdateAgentState>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetAgentId(),
        msg->Record.GetAgentState(),
        ctx.Now(),
        msg->Record.GetReason(),
        false);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleDisableAgent(
    const TEvDiskRegistry::TEvDisableAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(DisableAgent);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received DisableAgent request: AgentId=%s, DeviceUUIDs=%s",
        TabletID(),
        msg->Record.GetAgentId().c_str(),
        [&] {
            TStringStream out;
            for (auto& d: msg->Record.GetDeviceUUIDs()) {
                if (out.Str()) {
                    out << " ";
                }
                out << d;
            }
            return out.Str();
        }().c_str());

    auto* agent = State->FindAgent(msg->Record.GetAgentId());
    if (!agent) {
        auto response = std::make_unique<TEvDiskRegistry::TEvDisableAgentResponse>(
            MakeError(E_NOT_FOUND, TStringBuilder() << "no such agent: "
                << msg->Record.GetAgentId()));
        NCloud::Reply(ctx, *requestInfo, std::move(response));

        return;
    }

    auto agentRequest =
        std::make_unique<TEvDiskAgent::TEvDisableConcreteAgentRequest>();
    for (const auto& deviceId: msg->Record.GetDeviceUUIDs()) {
        *agentRequest->Record.AddDeviceUUIDs() = deviceId;
    }

    NCloud::Send(
        ctx,
        MakeDiskAgentServiceId(agent->GetNodeId()),
        std::move(agentRequest));

    if (msg->Record.DeviceUUIDsSize()) {
        // in case of 'partial' DisableAgentRequest we don't want to mark agent
        // as unavailable

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvDiskRegistry::TEvDisableAgentResponse>());

        return;
    }

    ExecuteTx<TUpdateAgentState>(
        ctx,
        std::move(requestInfo),
        std::move(*msg->Record.MutableAgentId()),
        NProto::AGENT_STATE_UNAVAILABLE,
        ctx.Now(),
        "DisableAgent request",
        true);
}

void TDiskRegistryActor::SendEnableDevice(
    const TActorContext& ctx,
    const TString& deviceId)
{
    if (const auto* agent = State->FindDeviceAgent(deviceId)) {
        auto agentRequest =
            std::make_unique<TEvDiskAgent::TEvEnableAgentDeviceRequest>();
        agentRequest->Record.SetDeviceUUID(deviceId);
        NCloud::Send(
            ctx,
            MakeDiskAgentServiceId(agent->GetNodeId()),
            std::move(agentRequest));
    }
}

void TDiskRegistryActor::HandleEnableDeviceResponse(
    const TEvDiskAgent::TEvEnableAgentDeviceResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdateAgentState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateAgentState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdateAgentState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdateAgentState& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);

    args.Error = State->UpdateAgentState(
        db,
        args.AgentId,
        args.State,
        args.StateTs,
        args.Reason,
        args.AffectedDisks);
}

void TDiskRegistryActor::CompleteUpdateAgentState(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdateAgentState& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "UpdateAgentState error: %s",
            FormatError(args.Error).c_str());
    }

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);
    StartMigration(ctx);

    NActors::IEventBasePtr response;
    if (args.IsDisableAgentRequest) {
        response = std::make_unique<TEvDiskRegistry::TEvDisableAgentResponse>(
            std::move(args.Error));
    } else {
        response = std::make_unique<TEvDiskRegistry::TEvChangeAgentStateResponse>(
            std::move(args.Error));
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    TVector<TDiskId> failedAllocationDisks;
    if (args.State == NProto::AGENT_STATE_UNAVAILABLE) {
        ScheduleSwitchAgentDisksToReadOnly(ctx, args.AgentId);
        failedAllocationDisks = State->CheckPendingAllocations(args.AgentId);
    }

    for (const auto& diskId: failedAllocationDisks) {
        ReplyToPendingAllocations(
            ctx,
            diskId,
            MakeError(
                E_REJECTED,
                "Allocation failed due to disk agent problems"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
