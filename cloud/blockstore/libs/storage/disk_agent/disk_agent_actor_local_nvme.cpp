#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/local_nvme/service.h>

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

    if (!LocalNVMeService) {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvListNVMeDevicesResponse>(
                MakeError(E_INVALID_STATE, "Local NVMe service is not initialized"));
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    LocalNVMeService->ListNVMeDevices().Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         replyTo = ev->Sender,
         replyFrom = ctx.SelfID,
         cookie = ev->Cookie](const auto& future)
        {
            auto [devices, error] = future.GetValue();

            auto response = std::make_unique<
                TEvDiskAgentPrivate::TEvListNVMeDevicesResponse>(
                error,
                devices);

            actorSystem->Send(new IEventHandle{
                replyTo,
                replyFrom,
                response.release(),
                0,   // flags
                cookie});
        });
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

    if (!LocalNVMeService) {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvListNVMeDevicesResponse>(
                MakeError(E_INVALID_STATE, "Local NVMe service is not initialized"));
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto* msg = ev->Get();

    LocalNVMeService->AcquireNVMeDevice(msg->SerialNumber)
        .Subscribe(
            [actorSystem = TActivationContext::ActorSystem(),
             replyTo = ev->Sender,
             replyFrom = ctx.SelfID,
             cookie = ev->Cookie](const auto& future)
            {
                const auto& [device, error] = future.GetValue();

                auto response = std::make_unique<
                    TEvDiskAgentPrivate::TEvAcquireNVMeDeviceResponse>(
                    error,
                    device);

                actorSystem->Send(new IEventHandle{
                    replyTo,
                    replyFrom,
                    response.release(),
                    0,   // flags
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

    if (!LocalNVMeService) {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvListNVMeDevicesResponse>(
                MakeError(E_INVALID_STATE, "Local NVMe service is not initialized"));
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    auto* msg = ev->Get();

    LocalNVMeService->ReleaseNVMeDevice(msg->SerialNumber)
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
                    0,   // flags
                    cookie});
            });
}

}   // namespace NCloud::NBlockStore::NStorage
