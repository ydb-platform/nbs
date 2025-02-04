#include "agent_availability_monitoring_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

// #include <cloud/blockstore/libs/storage/core/disk_counters.h> ?

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using TEvPartition = NPartition::TEvPartition;

TAgentAvailabilityMonitoringActor::TAgentAvailabilityMonitoringActor(
    TStorageConfigPtr config,
    TNonreplicatedPartitionConfigPtr partConfig,
    NActors::TActorId partNonreplActorId,
    NActors::TActorId parentActorId,
    TActorId statActorId,
    TString agentId)
    : Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , PartNonreplActorId(partNonreplActorId)
    , ParentActorId(parentActorId)
    , StatActorId(statActorId)
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

void TAgentAvailabilityMonitoringActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "TAgentAvailabilityMonitoringActor wakeup ",
        AgentId.c_str());

    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(ReadBlockIndex);
    request->Record.SetBlocksCount(1);

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(CheckHealthClientId));

    auto event = std::make_unique<IEventHandle>(
        PartNonreplActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );
    ctx.Send(event.release());

    ScheduleCheckAvailabilityRequest(ctx);
}

void TAgentAvailabilityMonitoringActor::ScheduleCheckAvailabilityRequest(
    const TActorContext& ctx)
{
    ctx.Schedule(
        Config->GetLaggingDevicePingInterval(),
        new TEvents::TEvWakeup());
}

void TAgentAvailabilityMonitoringActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TAgentAvailabilityMonitoringActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "xxxxx "
            "TAgentAvailabilityMonitoringActor::HandleReadBlocksResponse: "
            "%s",
            FormatError(msg->GetError()).c_str());

        return;
    }

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "xxxxx TAgentAvailabilityMonitoringActor::HandleReadBlocksResponse: "
        "OK!!!");

    NCloud::Send<TEvNonreplPartitionPrivate::TEvAgentIsBackOnline>(
        ctx,
        ParentActorId,
        0,
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
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
