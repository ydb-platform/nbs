#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod, typename T>
void Reply(
    TActorSystem& actorSystem,
    const TActorId& replyFrom,
    TRequestInfo& request,
    T&& result,
    ui64 started)
{
    AtomicAdd(request.ExecCycles, GetCycleCount() - started);

    auto response = std::make_unique<typename TMethod::TResponse>(
        std::move(result));

    LWTRACK(
        ResponseSent_DiskAgent,
        request.CallContext->LWOrbit,
        TMethod::Name,
        request.CallContext->RequestId);

    actorSystem.Send(
        new IEventHandle(
            request.Sender,
            replyFrom,
            response.release(),
            0,          // flags
            request.Cookie));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod, typename TOp>
void TDiskAgentActor::PerformIO(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    TOp operation)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    const ui64 started = GetCycleCount();

    auto& record = msg->Record;
    const auto deviceUUID = record.GetDeviceUUID();
    const auto sessionId = record.GetSessionId();

    if (State->IsDeviceDisabled(deviceUUID)) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT,
            "Dropped %s request to device %s, session %s",
            TMethod::Name,
            deviceUUID.c_str(),
            sessionId.c_str());

        return;
    }

    LWTRACK(
        RequestReceived_DiskAgent,
        requestInfo->CallContext->LWOrbit,
        TMethod::Name,
        static_cast<ui32>(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED),
        requestInfo->CallContext->RequestId,
        deviceUUID);

    auto* actorSystem = ctx.ActorSystem();
    auto replyFrom = ctx.SelfID;

    auto replySuccess = [=] (auto result) {
        Reply<TMethod>(
            *actorSystem,
            replyFrom,
            *requestInfo,
            std::move(result),
            started);
    };

    auto replyError = [=] (auto code, auto message) {
        Reply<TMethod>(
            *actorSystem,
            replyFrom,
            *requestInfo,
            MakeError(code, std::move(message)),
            started);
    };

    LOG_TRACE(ctx, TBlockStoreComponents::DISK_AGENT,
        "%s [%s / %s]",
        TMethod::Name,
        deviceUUID.c_str(),
        sessionId.c_str());

    if (SecureErasePendingRequests.contains(deviceUUID)) {
        replyError(E_REJECTED, "Secure erase in progress");
        return;
    }

    try {
        BLOCKSTORE_DISK_AGENT_FAULT_INJECTION(TMethod::Name, deviceUUID);

        auto result =
            std::invoke(operation, *State, ctx.Now(), std::move(record));

        result.Subscribe(
            [=] (auto future) {
                try {
                    replySuccess(future.ExtractValue());
                } catch (...) {
                    LOG_ERROR(*actorSystem, TBlockStoreComponents::DISK_AGENT,
                        "%s [%s / %s] Unexpected io error: %s",
                        TMethod::Name,
                        deviceUUID.c_str(),
                        sessionId.c_str(),
                        CurrentExceptionMessage().c_str()
                    );

                    replyError(E_FAIL, CurrentExceptionMessage());
                }
            });
    } catch (const TServiceError& e) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_AGENT,
            "%s [%s / %s] Service error: %u (%s)",
            TMethod::Name,
            deviceUUID.c_str(),
            sessionId.c_str(),
            e.GetCode(),
            e.what()
        );

        replyError(e.GetCode(), e.what());
    } catch (...) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_AGENT,
            "%s [%s / %s] Unexpected state error: %s",
            TMethod::Name,
            deviceUUID.c_str(),
            sessionId.c_str(),
            CurrentExceptionMessage().c_str()
        );

        replyError(E_FAIL, CurrentExceptionMessage());
    }
}

void TDiskAgentActor::HandleReadDeviceBlocks(
    const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(ReadDeviceBlocks);

    using TMethod = TEvDiskAgent::TReadDeviceBlocksMethod;

    PerformIO<TMethod>(ctx, ev, &TDiskAgentState::Read);
}

void TDiskAgentActor::HandleWriteDeviceBlocks(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(WriteDeviceBlocks);

    using TMethod = TEvDiskAgent::TWriteDeviceBlocksMethod;

    PerformIO<TMethod>(ctx, ev, &TDiskAgentState::Write);
}

void TDiskAgentActor::HandleZeroDeviceBlocks(
    const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(ZeroDeviceBlocks);

    using TMethod = TEvDiskAgent::TZeroDeviceBlocksMethod;

    PerformIO<TMethod>(ctx, ev, &TDiskAgentState::WriteZeroes);
}

void TDiskAgentActor::HandleChecksumDeviceBlocks(
    const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(ChecksumDeviceBlocks);

    using TMethod = TEvDiskAgent::TChecksumDeviceBlocksMethod;

    PerformIO<TMethod>(ctx, ev, &TDiskAgentState::Checksum);
}

}   // namespace NCloud::NBlockStore::NStorage
