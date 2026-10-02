#include "disk_agent_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleDeallocateDevice(
    const TEvDiskAgent::TEvDeallocateDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(DeallocateDevice);

    const auto& request = ev->Get()->Record;

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Deallocate device " << request.GetDeviceUUID().Quote());

    //
    // TODO(#6956): Remove JournalledDevice from JournalledDeviceTcpServer.
    // The request may be repeated, so its handling must be idempotent.
    //

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvDiskAgent::TEvDeallocateDeviceResponse>());
}

}   // namespace NCloud::NBlockStore::NStorage
