#include "disk_agent_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleAllocateDevice(
    const TEvDiskAgent::TEvAllocateDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(AllocateDevice);

    const auto& request = ev->Get()->Record;

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Allocate device " << request.GetDeviceUUID().Quote()
            << ", journal config: "
            << request.GetJournalConfig().ShortDebugString().Quote());

    //
    // TODO(#6956): Validate request.GetJournalConfig().GetEnabled() == true.
    // Create JournalledDevice and add it to JournalledDeviceTcpServer.
    // The request may be repeated, so its handling must be idempotent.
    //

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvDiskAgent::TEvAllocateDeviceResponse>());
}

}   // namespace NCloud::NBlockStore::NStorage
