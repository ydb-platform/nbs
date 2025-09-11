#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

template <typename T>
TResultOrError<T> UnpackFuture(TFuture<T> f)
{
    try {
        return f.GetValue();
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }
}

}   // namespace

void TDiskAgentActor::AddOpenCloseDeviceRequest(
    const NActors::TActorContext& ctx,
    const TOpenCloseDeviceRequest& request)
{
    OpenCloseDevicesRequests.push_back(request);

    if (OpenCloseDevicesRequests.size() == 1) {
        ProcessNextOpenCloseDevicesRequests(ctx);
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Postponing open/close device request, another request is "
            "in progress");
    }
}

void TDiskAgentActor::AddOpenDeviceRequest(
    const NActors::TActorContext& ctx,
    const TOpenDevice& request)
{
    AddOpenCloseDeviceRequest(ctx, request);
}

void TDiskAgentActor::AddCloseDeviceRequest(
    const NActors::TActorContext& ctx,
    const TCloseDevice& request)
{
    AddOpenCloseDeviceRequest(ctx, request);
}

bool TDiskAgentActor::ProcessOpenDevicesRequests(
    const NActors::TActorContext& ctx,
    const TOpenCloseDeviceRequest& request)
{
    Y_DEBUG_ABORT_UNLESS(request.IsOpen);
    const auto& path = request.DeviceName;
    auto [uuidToFuture, error] =
        State->OpenDevice(path, request.DeviceGeneration);

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to open device %s: %s",
            path.c_str(),
            FormatError(error).c_str());

        auto response =
            std::make_unique<TEvDiskAgent::TEvOpenDeviceResponse>(error);
        NCloud::Reply(ctx, *request.RequestInfo, std::move(response));
        return true;
    }

    if (error.GetCode() == S_ALREADY) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Device %s is already opened",
            path.c_str());

        auto response = std::make_unique<TEvDiskAgent::TEvOpenDeviceResponse>();

        auto allDevicesForPath =
            State->GetAllDevicesForPath(request.DeviceName);
        for (const auto& device: allDevicesForPath) {
            *response->Record.AddOpenedDevices() = device;
        }

        NCloud::Reply(ctx, *request.RequestInfo, std::move(response));
        return true;
    }

    Y_DEBUG_ABORT_UNLESS(!uuidToFuture.empty());
    TVector<TFuture<IStoragePtr>> futures;
    for (const auto& [_, future]: uuidToFuture) {
        futures.push_back(future);
    }

    WaitAll(futures).Subscribe(
        [actorSystem = ctx.ActorSystem(),
         replyTo = ctx.SelfID,
         uuidToFuture = std::move(uuidToFuture)](auto)
        {
            THashMap<TString, TResultOrError<IStoragePtr>> deviceOpenResults;
            for (const auto& [uuid, future]: uuidToFuture) {
                deviceOpenResults.try_emplace(uuid, UnpackFuture(future));
            }

            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvDeviceOpened>(
                    std::move(deviceOpenResults));
            actorSystem->Send(replyTo, response.release());
        });

    return false;
}

bool TDiskAgentActor::ProcessCloseDevicesRequests(
    const NActors::TActorContext& ctx,
    const TOpenCloseDeviceRequest& request)
{
    auto error =
        State->CloseDevice(request.DeviceName, request.DeviceGeneration);
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to close device %s: %s",
            request.DeviceName.c_str(),
            FormatError(error).c_str());
    }

    auto response = std::make_unique<TEvDiskAgent::TEvCloseDeviceResponse>();
    *response->Record.MutableError() = error;
    NCloud::Reply(ctx, *request.RequestInfo, std::move(response));

    return true;
}

void TDiskAgentActor::ProcessNextOpenCloseDevicesRequests(
    const NActors::TActorContext& ctx)
{
    if (OpenCloseDevicesRequests.size() == 0) {
        return;
    }
    auto& request = OpenCloseDevicesRequests.front();

    bool shouldProcessNextRequest = true;
    if (request.IsOpen) {
        shouldProcessNextRequest = ProcessOpenDevicesRequests(ctx, request);
    } else {
        shouldProcessNextRequest = ProcessCloseDevicesRequests(ctx, request);
    }

    if (shouldProcessNextRequest) {
        OpenCloseDevicesRequests.pop_front();
        ProcessNextOpenCloseDevicesRequests(ctx);
    }
}

void TDiskAgentActor::HandleOpenDevice(
    const TEvDiskAgent::TEvOpenDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    AddOpenDeviceRequest(
        ctx,
        {.DeviceName = msg->Record.GetDevicePath(),
         .DeviceGeneration = msg->Record.GetDeviceGeneration(),
         .RequestInfo = requestInfo});
}

void TDiskAgentActor::HandleDeviceOpened(
    const TEvDiskAgentPrivate::TEvDeviceOpened::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto request = OpenCloseDevicesRequests.front();
    Y_DEBUG_ABORT_UNLESS(request.IsOpen);
    if (!request.IsOpen) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Unexpected device opened event");
        return;
    }

    for (auto [uuid, result]: msg->Devices) {
        auto [device, error] = result;
        State->DeviceOpened(error, uuid, device);
    }

    auto response = std::make_unique<TEvDiskAgent::TEvOpenDeviceResponse>();
    auto allDevicesForPath = State->GetAllDevicesForPath(request.DeviceName);
    for (const auto& device: allDevicesForPath) {
        *response->Record.AddOpenedDevices() = device;
    }

    NCloud::Reply(ctx, *request.RequestInfo, std::move(response));

    OpenCloseDevicesRequests.pop_front();
    ProcessNextOpenCloseDevicesRequests(ctx);
}

void TDiskAgentActor::HandleCloseDevice(
    const TEvDiskAgent::TEvCloseDeviceRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    AddCloseDeviceRequest(
        ctx,
        {.DeviceName = msg->Record.GetDevicePath(),
         .DeviceGeneration = msg->Record.GetDeviceGeneration(),
         .RequestInfo = requestInfo});
}

}   // namespace NCloud::NBlockStore::NStorage
