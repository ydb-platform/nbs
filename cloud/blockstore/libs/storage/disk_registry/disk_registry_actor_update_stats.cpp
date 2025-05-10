#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdateAgentStats(
    const TEvDiskRegistry::TEvUpdateAgentStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateAgentStats);

    auto* msg = ev->Get();
    auto& stats = *msg->Record.MutableAgentStats();

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received UpdateAgentStats request: NodeId=%u",
        TabletID(),
        stats.GetNodeId());

    auto error = State->UpdateAgentCounters(stats);

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "UpdateAgentCounters error: %s",
            FormatError(error).c_str());
    }

    auto response = std::make_unique<TEvDiskRegistry::TEvUpdateAgentStatsResponse>(
        std::move(error));

    NCloud::Reply(ctx, *ev, std::move(response));

    for (const auto& uuid: State->CollectBrokenDevices(stats)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Device with IO errors detected: %s", uuid.c_str());

        auto request = std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateRequest>();
        request->Record.SetDeviceUUID(uuid);
        request->Record.SetDeviceState(NProto::DEVICE_STATE_ERROR);
        request->Record.SetReason("IO errors");

        NCloud::Send(ctx, ctx.SelfID, std::move(request));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
