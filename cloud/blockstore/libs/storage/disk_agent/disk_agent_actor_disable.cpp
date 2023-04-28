#include "disk_agent_actor.h"

#include <cloud/storage/core/libs/kikimr/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleDisableConcreteAgent(
    const TEvDiskAgent::TEvDisableConcreteAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "DisableConcreteAgentRequest received, DeviceUUIDs=%s",
        [&] {
            TStringStream out;
            for (auto& d: record.GetDeviceUUIDs()) {
                if (out.Str()) {
                    out << " ";
                }
                out << d;
            }
            return out.Str();
        }().c_str());

    if (record.DeviceUUIDsSize()) {
        for (const auto& d: record.GetDeviceUUIDs()) {
            State->DisableDevice(d);
        }
    } else {
        HandlePoisonPill(nullptr, ctx);
    }

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvDiskAgent::TEvDisableConcreteAgentResponse>());
}

}   // namespace NCloud::NBlockStore::NStorage
