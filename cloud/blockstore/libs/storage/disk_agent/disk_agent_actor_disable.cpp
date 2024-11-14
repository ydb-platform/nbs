#include "disk_agent_actor.h"

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <util/string/join.h>

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
        JoinSeq(" ", record.GetDeviceUUIDs()).c_str());

    if (record.DeviceUUIDsSize()) {
        for (const auto& d: record.GetDeviceUUIDs()) {
            State->DisableDevice(d);
            State->ReportDisabledDeviceError(d);
        }
    } else {
        HandlePoisonPill(nullptr, ctx);
    }

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvDiskAgent::TEvDisableConcreteAgentResponse>());
}

void TDiskAgentActor::HandleEnableAgentDevice(
    const TEvDiskAgent::TEvEnableAgentDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "EnableAgentDevice received, DeviceUUID=%s",
        record.GetDeviceUUID().c_str());

    State->EnableDevice(record.GetDeviceUUID());

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvDiskAgent::TEvEnableAgentDeviceResponse>());
}

}   // namespace NCloud::NBlockStore::NStorage
