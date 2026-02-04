#include "disk_agent_mock.h"

#include <cloud/blockstore/libs/local_nvme/service.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentMock::HandleListNVMeDevices(
    TEvDiskAgentPrivate::TEvListNVMeDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
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
