#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool TDiskAgentActor::CanStartSecureErase(const TString& uuid)
{
    const auto& name = State->GetDeviceName(uuid);
    return !SecureEraseDevicesNames.contains(name) &&
           SecureEraseDevicesNames.size() <
               AgentConfig->GetMaxParallelSecureErasesAllowed();
}

void TDiskAgentActor::SecureErase(
    const NActors::TActorContext& ctx,
    const TString& deviceId)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_AGENT,
        "Start secure erase for " << deviceId.Quote());

    SecureEraseDevicesNames.emplace(State->GetDeviceName(deviceId));

    auto* actorSystem = ctx.ActorSystem();
    auto replyTo = ctx.SelfID;

    auto reply = [actorSystem, deviceId, replyTo](auto error)
    {
        auto response =
            std::make_unique<TEvDiskAgentPrivate::TEvSecureEraseCompleted>(
                std::move(error),
                deviceId);

        actorSystem->Send(
            new IEventHandle(replyTo, replyTo, response.release()));
    };

    const auto& recentBlocksTracker = GetRecentBlocksTracker(deviceId);
    if (recentBlocksTracker.HasInflight()) {
        ReportDiskAgentSecureEraseDuringIo({{"device", deviceId}});

        reply(MakeError(E_REJECTED, TStringBuilder()
                << "SecureErase with inflight ios present for device "
                << deviceId));
        return;
    }

    try {
        auto result = State->SecureErase(deviceId, ctx.Now());

        result.Subscribe(
            [reply](const auto& future)
            {
                try {
                    reply(future.GetValue());
                } catch (...) {
                    reply(MakeError(E_FAIL, CurrentExceptionMessage()));
                }
            });
    } catch (const TServiceError& e) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "Secure erase device " << deviceId << " has failed with error: "
            << e.what());

        reply(MakeError(e.GetCode(), e.what()));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleSecureEraseDevice(
    const TEvDiskAgent::TEvSecureEraseDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(SecureEraseDevice);

    const auto& request = ev->Get()->Record;
    const auto& deviceId = request.GetDeviceUUID();

    if (!State->FindDeviceConfig(deviceId)) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Received secure erase for unknown device %s",
            deviceId.Quote().c_str());

        auto response =
            std::make_unique<TEvDiskAgent::TEvSecureEraseDeviceResponse>(
                MakeError(
                    E_NOT_FOUND,
                    TStringBuilder()
                        << "Device " << deviceId.Quote() << " not found"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Secure erase device " << deviceId.Quote());

    auto& pendingRequests = SecureErasePendingRequests[deviceId];

    pendingRequests.emplace_back(
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext));

    if (!CanStartSecureErase(deviceId)) {
        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "Postpone secure erase for " << deviceId.Quote());

        return;
    }

    SecureErase(ctx, deviceId);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleSecureEraseCompleted(
    const TEvDiskAgentPrivate::TEvSecureEraseCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "Secure erase for " << msg->DeviceId.Quote() << " failed. Error: "
                << FormatError(error));
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "Secure erase for " << msg->DeviceId.Quote() << " succeeded");

        // The device has been secure erased and now a new client can use it.
        auto& recentBlocksTracker = GetRecentBlocksTracker(msg->DeviceId);
        recentBlocksTracker.Reset();
    }

    // send responses

    if (auto it = SecureErasePendingRequests.find(msg->DeviceId);
        it != SecureErasePendingRequests.end())
    {
        for (auto& requestInfo: it->second) {
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TEvDiskAgent::TEvSecureEraseDeviceResponse>(error));
        }

        SecureErasePendingRequests.erase(it);
    }
    SecureEraseDevicesNames.erase(State->GetDeviceName(msg->DeviceId));

    // erase next device

    TVector<TString> devicesToErase;
    for (const auto& [deviceUUID, pendingRequests]: SecureErasePendingRequests) {
        Y_DEBUG_ABORT_UNLESS(!pendingRequests.empty());

        if (pendingRequests.empty()) {
            devicesToErase.emplace_back(deviceUUID);
            continue;
        }

        if (CanStartSecureErase(deviceUUID)) {
            SecureErase(ctx, deviceUUID);
        }
    }

    for (const auto& uuid: devicesToErase) {
        SecureErasePendingRequests.erase(uuid);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
