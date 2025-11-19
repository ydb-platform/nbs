#include "disk_agent_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleListNVMeDevices(
    const TEvDiskAgentPrivate::TEvListNVMeDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!State) {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvListNVMeDevicesResponse>(
                MakeError(E_REJECTED, "Not ready"));
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto [devices, error] = State->GetNVMeDevices();

    auto response =
        std::make_unique<TEvDiskAgentPrivate::TEvListNVMeDevicesResponse>(
            std::move(error));

    response->NVMeDevices = std::move(devices);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TDiskAgentActor::HandleAcquireNVMeDevice(
    const TEvDiskAgentPrivate::TEvAcquireNVMeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!State) {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvAcquireNVMeDeviceResponse>(
                MakeError(E_REJECTED, "Not ready"));
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvDiskAgentPrivate::TEvAcquireNVMeDeviceResponse>(
            State->AcquireNVMeDevice(msg->SerialNumber));
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TDiskAgentActor::HandleReleaseNVMeDevice(
    const TEvDiskAgentPrivate::TEvReleaseNVMeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!State) {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvReleaseNVMeDeviceResponse>(
                MakeError(E_REJECTED, "Not ready"));
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvDiskAgentPrivate::TEvReleaseNVMeDeviceResponse>(
            State->ReleaseNVMeDevice(msg->SerialNumber));
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
