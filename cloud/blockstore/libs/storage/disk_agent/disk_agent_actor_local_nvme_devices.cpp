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
            error);

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

    State->AcquireNVMeDevice(msg->SerialNumber)
        .Subscribe(
            [actorSystem = TActivationContext::ActorSystem(),
             replyTo = ev->Sender,
             replyFrom = ctx.SelfID,
             cookie = ev->Cookie](const auto& future)
            {
                auto response = std::make_unique<
                    TEvDiskAgentPrivate::TEvAcquireNVMeDeviceResponse>(
                    future.GetValue());

                actorSystem->Send(new IEventHandle{
                    replyTo,
                    replyFrom,
                    response.release(),
                    0,  // flags
                    cookie});
            });
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

    State->ReleaseNVMeDevice(msg->SerialNumber)
        .Subscribe(
            [actorSystem = TActivationContext::ActorSystem(),
             replyTo = ev->Sender,
             replyFrom = ctx.SelfID,
             cookie = ev->Cookie](const auto& future)
            {
                auto response = std::make_unique<
                    TEvDiskAgentPrivate::TEvReleaseNVMeDeviceResponse>(
                    future.GetValue());

                actorSystem->Send(new IEventHandle{
                    replyTo,
                    replyFrom,
                    response.release(),
                    0,  // flags
                    cookie});
            });
}

}   // namespace NCloud::NBlockStore::NStorage
