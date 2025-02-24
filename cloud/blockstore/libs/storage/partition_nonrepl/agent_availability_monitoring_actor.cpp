#include "agent_availability_monitoring_actor.h"

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

///////////////////////////////////////////////////////////////////////////////

TAgentAvailabilityMonitoringActor::TAgentAvailabilityMonitoringActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId nonreplPartitionActorId,
        TActorId parentActorId,
        TString agentId)
    : Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , NonreplPartitionActorId(nonreplPartitionActorId)
    , ParentActorId(parentActorId)
    , AgentId(std::move(agentId))
{
    for (const auto& device: PartConfig->GetDevices()) {
        if (device.GetAgentId() == AgentId) {
            break;
        }

        ReadBlockIndex += device.GetBlocksCount();
    }
}

TAgentAvailabilityMonitoringActor::~TAgentAvailabilityMonitoringActor() =
    default;

void TAgentAvailabilityMonitoringActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ScheduleCheckAvailabilityRequest(ctx);
}

void TAgentAvailabilityMonitoringActor::ScheduleCheckAvailabilityRequest(
    const TActorContext& ctx)
{
    ctx.Schedule(
        Config->GetLaggingDevicePingInterval(),
        new TEvents::TEvWakeup());
}

void TAgentAvailabilityMonitoringActor::CheckAgentAvailability(
    const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Checking lagging agent %s availability by reading block %lu",
        PartConfig->GetName().c_str(),
        AgentId.c_str(),
        ReadBlockIndex);

    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(ReadBlockIndex);
    request->Record.SetBlocksCount(1);
    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(CheckHealthClientId));

    auto event = std::make_unique<IEventHandle>(
        NonreplPartitionActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );
    ctx.Send(event.release());
}

void TAgentAvailabilityMonitoringActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    CheckAgentAvailability(ctx);
    ScheduleCheckAvailabilityRequest(ctx);
}

void TAgentAvailabilityMonitoringActor::HandleReadBlocksUndelivery(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Couldn't read block from lagging agent %s due to undelivered "
        "request",
        PartConfig->GetName().c_str(),
        AgentId.c_str());
}

void TAgentAvailabilityMonitoringActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "[%s] Couldn't read block from lagging agent %s due to error: %s",
            PartConfig->GetName().c_str(),
            AgentId.c_str(),
            FormatError(msg->GetError()).c_str());

        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Successfully read one block from lagging agent %s",
        PartConfig->GetName().c_str(),
        AgentId.c_str());

    NCloud::Send<TEvNonreplPartitionPrivate::TEvAgentIsBackOnline>(
        ctx,
        ParentActorId,
        0,   // cookie
        AgentId);
}

void TAgentAvailabilityMonitoringActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    NCloud::Reply(ctx, *ev, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TAgentAvailabilityMonitoringActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocksUndelivery);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
