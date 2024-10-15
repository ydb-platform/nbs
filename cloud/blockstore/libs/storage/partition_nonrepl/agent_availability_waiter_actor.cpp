#include "agent_availability_waiter_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>
#include "cloud/blockstore/libs/storage/disk_agent/model/public.h"

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using TEvPartition = NPartition::TEvPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

TAgentAvailabilityWaiterActor::TAgentAvailabilityWaiterActor(
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
    for (const auto& device : partConfig->GetDevices()) {
        if (device.GetAgentId() == AgentId) {
            break;
        }

        ReadBlockIndex += device.GetBlocksCount();
    }
}

TAgentAvailabilityWaiterActor::~TAgentAvailabilityWaiterActor() = default;


void TAgentAvailabilityWaiterActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ScheduleCheckAvailabilityRequest(ctx);
}

void TAgentAvailabilityWaiterActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TAgentAvailabilityWaiterActor wakeup ",
        AgentId.c_str());

    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(ReadBlockIndex);
    request->Record.SetBlocksCount(1);

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

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

void TAgentAvailabilityWaiterActor::ScheduleCheckAvailabilityRequest(const TActorContext& ctx) {

    ctx.Schedule(TDuration::MilliSeconds(333), new TEvents::TEvWakeup());
}

void TAgentAvailabilityWaiterActor::HandleReadBlocks(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TAgentAvailabilityWaiterActor::HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx) {
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        return;
    }

    NCloud::Send<NPartition::TEvPartition::TEvAgentIsBackOnline>(
        ctx,
        ParentActorId,
        0,
        AgentId);
}

void TAgentAvailabilityWaiterActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    NCloud::Reply(ctx, *ev, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////


STFUNC(TAgentAvailabilityWaiterActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocks);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
