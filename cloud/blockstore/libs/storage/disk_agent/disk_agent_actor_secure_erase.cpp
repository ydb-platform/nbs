#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::SecureErase(
    const NActors::TActorContext& ctx,
    const TString& deviceId)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_AGENT,
        "Start secure erase for " << deviceId.Quote());

    auto* actorSystem = ctx.ActorSystem();
    auto replyTo = ctx.SelfID;

    auto reply = [=] (auto error) {
        auto response = std::make_unique<TEvDiskAgentPrivate::TEvSecureEraseCompleted>(
            std::move(error),
            deviceId);

        actorSystem->Send(
            new IEventHandle(
                replyTo,
                replyTo,
                response.release()));
    };

    try {
        auto result = State->SecureErase(deviceId, ctx.Now());

        result.Subscribe([=] (const auto& future) {
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

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_AGENT,
        "Secure erase device " << deviceId.Quote());

    auto& pendingRequests = SecureErasePendingRequests[deviceId];

    pendingRequests.emplace_back(
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext,
            ev->TraceId.Clone()));

    if (SecureErasePendingRequests.size() > 1 || pendingRequests.size() > 1) {
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

    // erase next device

    while (!SecureErasePendingRequests.empty()) {
        auto it = SecureErasePendingRequests.begin();

        auto& [deviceId, pendingRequests] = *it;

        Y_VERIFY_DEBUG(!pendingRequests.empty());

        if (pendingRequests.empty()) {
            SecureErasePendingRequests.erase(it);
            continue;
        }

        SecureErase(ctx, deviceId);
        break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
