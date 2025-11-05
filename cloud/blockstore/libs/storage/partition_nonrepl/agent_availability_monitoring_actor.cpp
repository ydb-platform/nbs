#include "agent_availability_monitoring_actor.h"

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace google::protobuf;

namespace {

///////////////////////////////////////////////////////////////////////////////

std::optional<ui32> GetLaggingAgentNodeId(
    const TNonreplicatedPartitionConfigPtr& partConfig,
    const RepeatedPtrField<NProto::TDeviceMigration>& migrations,
    const NProto::TLaggingAgent& laggingAgent)
{
    const auto& laggingDevice = laggingAgent.GetDevices()[0];
    const ui32 rowIndex = laggingDevice.GetRowIndex();
    if (partConfig->GetDevices()[rowIndex].GetAgentId() ==
        laggingAgent.GetAgentId())
    {
        return partConfig->GetDevices()[rowIndex].GetNodeId();
    }

    for (const auto& migration: migrations) {
        if (migration.GetTargetDevice().GetAgentId() ==
            laggingAgent.GetAgentId())
        {
            return migration.GetTargetDevice().GetNodeId();
        }
    }

    return std::nullopt;
}

}   // namespace

///////////////////////////////////////////////////////////////////////////////

TAgentAvailabilityMonitoringActor::TAgentAvailabilityMonitoringActor(
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig,
        RepeatedPtrField<NProto::TDeviceMigration> migrations,
        TActorId nonreplPartitionActorId,
        TActorId parentActorId,
        NProto::TLaggingAgent laggingAgent)
    : Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , Migrations(std::move(migrations))
    , NonreplPartitionActorId(nonreplPartitionActorId)
    , ParentActorId(parentActorId)
    , LaggingAgent(std::move(laggingAgent))
{
    Y_ABORT_IF(LaggingAgent.GetDevices().empty());

    auto nodeId = GetLaggingAgentNodeId(PartConfig, Migrations, LaggingAgent);
    Y_ABORT_UNLESS(nodeId.has_value());
    LaggingAgentNodeId = *nodeId;
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
        "[%s] Checking lagging agent %s availability by reading device %s "
        "first block checksum",
        PartConfig->GetName().c_str(),
        LaggingAgent.GetAgentId().Quote().c_str(),
        LaggingAgent.GetDevices()[0].GetDeviceUUID().Quote().c_str());

    auto request =
        std::make_unique<TEvDiskAgent::TEvChecksumDeviceBlocksRequest>();
    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(CheckHealthClientId));
    request->Record.SetDeviceUUID(LaggingAgent.GetDevices()[0].GetDeviceUUID());
    request->Record.SetStartIndex(0);
    request->Record.SetBlockSize(PartConfig->GetBlockSize());
    request->Record.SetBlocksCount(1);

    auto event = std::make_unique<IEventHandle>(
        MakeDiskAgentServiceId(LaggingAgentNodeId),
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

void TAgentAvailabilityMonitoringActor::HandleChecksumBlocksUndelivery(
    const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Couldn't read block checksum from lagging agent %s due to "
        "undelivered request",
        PartConfig->GetName().c_str(),
        LaggingAgent.GetAgentId().Quote().c_str());
}

void TAgentAvailabilityMonitoringActor::HandleChecksumBlocksResponse(
    const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "[%s] Couldn't read block checksum from lagging agent %s due to "
            "error: %s",
            PartConfig->GetName().c_str(),
            LaggingAgent.GetAgentId().Quote().c_str(),
            FormatError(msg->GetError()).c_str());

        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] Successfully read block checksum from lagging agent %s",
        PartConfig->GetName().c_str(),
        LaggingAgent.GetAgentId().Quote().c_str());

    NCloud::Send<TEvNonreplPartitionPrivate::TEvAgentIsBackOnline>(
        ctx,
        ParentActorId,
        0,   // cookie
        LaggingAgent.GetAgentId());
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
        HFunc(
            TEvDiskAgent::TEvChecksumDeviceBlocksResponse,
            HandleChecksumBlocksResponse);
        HFunc(
            TEvDiskAgent::TEvChecksumDeviceBlocksRequest,
            HandleChecksumBlocksUndelivery);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvVolume::TEvRWClientIdChanged);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
