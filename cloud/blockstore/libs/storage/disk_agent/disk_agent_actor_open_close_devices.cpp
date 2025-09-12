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

NProto::TError TDiskAgentActor::AddOpenCloseDeviceRequest(
    const NActors::TActorContext& ctx,
    const TString& path,
    TRequestInfoPtr requestInfo)
{
    auto [it, inserted] = OpenCloseDevicesInProgress.emplace(path, requestInfo);
    if (!inserted) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Already processing open close request for device %s, rejecting "
            "request",
            path.Quote().c_str());

        return MakeError(E_REJECTED, "another request is in progress");
    }

    return {};
}

void TDiskAgentActor::ReplyToOpenCloseDeviceRequest(
    const NActors::TActorContext& ctx,
    const TString& path,
    NActors::IEventBasePtr response)
{
    auto* requestInfo = OpenCloseDevicesInProgress.FindPtr(path);
    Y_DEBUG_ABORT_UNLESS(requestInfo);
    if (!requestInfo) {
        return;
    }

    NCloud::Reply(ctx, **requestInfo, std::move(response));
    OpenCloseDevicesInProgress.erase(path);
}

void TDiskAgentActor::HandleOpenDevice(
    const TEvDiskAgent::TEvOpenDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto path = record.GetDevicePath();
    if (auto error =
            AddOpenCloseDeviceRequest(ctx, path, std::move(requestInfo));
        HasError(error))
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvOpenDeviceResponse>(error));
        return;
    }

    auto [uuidToFuture, error] =
        State->OpenDevice(path, record.GetDeviceGeneration());

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to open device %s: %s",
            path.c_str(),
            FormatError(error).c_str());

        auto response =
            std::make_unique<TEvDiskAgent::TEvOpenDeviceResponse>(error);
        ReplyToOpenCloseDeviceRequest(ctx, path, std::move(response));
        return;
    }

    if (error.GetCode() == S_ALREADY) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Device %s is already opened",
            path.c_str());

        auto response = std::make_unique<TEvDiskAgent::TEvOpenDeviceResponse>();

        auto allDevicesForPath = State->GetAllDevicesForPath(path);
        for (const auto& device: allDevicesForPath) {
            *response->Record.AddOpenedDevices() = device;
        }

        ReplyToOpenCloseDeviceRequest(ctx, path, std::move(response));
        return;
    }

    Y_DEBUG_ABORT_UNLESS(!uuidToFuture.empty());
    TVector<TFuture<IStoragePtr>> futures;
    for (const auto& [_, future]: uuidToFuture) {
        futures.push_back(future);
    }

    WaitAll(futures).Subscribe(
        [actorSystem = ctx.ActorSystem(),
         replyTo = ctx.SelfID,
         uuidToFuture = std::move(uuidToFuture),
         path = std::move(path)](auto) mutable
        {
            THashMap<TString, TResultOrError<IStoragePtr>> deviceOpenResults;
            for (const auto& [uuid, future]: uuidToFuture) {
                deviceOpenResults.try_emplace(uuid, UnpackFuture(future));
            }

            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvDeviceOpened>(
                    std::move(deviceOpenResults),
                    std::move(path));
            actorSystem->Send(replyTo, response.release());
        });
}

void TDiskAgentActor::HandleDeviceOpened(
    const TEvDiskAgentPrivate::TEvDeviceOpened::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    for (auto [uuid, result]: msg->Devices) {
        auto [device, error] = result;
        State->DeviceOpened(error, uuid, device);
    }

    auto response = std::make_unique<TEvDiskAgent::TEvOpenDeviceResponse>();
    auto allDevicesForPath = State->GetAllDevicesForPath(msg->Path);
    for (const auto& device: allDevicesForPath) {
        *response->Record.AddOpenedDevices() = device;
    }

    ReplyToOpenCloseDeviceRequest(ctx, msg->Path, std::move(response));
}

void TDiskAgentActor::HandleCloseDevice(
    const TEvDiskAgent::TEvCloseDeviceRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto path = record.GetDevicePath();
    if (auto error =
            AddOpenCloseDeviceRequest(ctx, path, std::move(requestInfo));
        HasError(error))
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvCloseDeviceResponse>(error));
        return;
    }

    auto error = State->CloseDevice(path, record.GetDeviceGeneration());
    if (HasError(error)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to close device %s: %s",
            path.Quote().c_str(),
            FormatError(error).c_str());
    }

    auto response =
        std::make_unique<TEvDiskAgent::TEvCloseDeviceResponse>(error);
    ReplyToOpenCloseDeviceRequest(ctx, path, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
