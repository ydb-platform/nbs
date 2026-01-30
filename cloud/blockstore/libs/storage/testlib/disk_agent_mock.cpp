#include "disk_agent_mock.h"

#include <cloud/blockstore/libs/local_nvme/service.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentMock::HandleListNVMeDevices(
    TEvDiskAgentPrivate::TEvListNVMeDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto [disks, error] = LocalNVMeService->ListNVMeDevices();

    auto response =
        std::make_unique<TEvDiskAgentPrivate::TEvListNVMeDevicesResponse>(
            std::move(error),
            std::move(disks));

    Reply(ctx, *ev, std::move(response));
}

void TDiskAgentMock::HandleAcquireNVMeDevice(
    TEvDiskAgentPrivate::TEvAcquireNVMeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LocalNVMeService->AcquireNVMeDevice(msg->SerialNumber)
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
                    0,   // flags
                    cookie});
            });
}

void TDiskAgentMock::HandleReleaseNVMeDevice(
    TEvDiskAgentPrivate::TEvReleaseNVMeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
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
