#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/actors/multi_agent_write_device_blocks_actor.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetVolumeRequestId(
    const TEvDiskAgentPrivate::TParsedWriteDeviceBlocksRequest& request)
{
    return NStorage::GetVolumeRequestId(request.Record);
}

TBlockRange64 BuildRequestBlockRange(
    const TEvDiskAgentPrivate::TParsedWriteDeviceBlocksRequest& request)
{
    if (!request.StorageSize) {
        return NStorage::BuildRequestBlockRange(request.Record);
    }

    Y_ABORT_UNLESS(request.StorageSize % request.Record.GetBlockSize() == 0);

    return TBlockRange64::WithLength(
        request.Record.GetStartIndex(),
        request.StorageSize / request.Record.GetBlockSize());
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr bool IsWriteDeviceMethod =
    std::is_same_v<T, TEvDiskAgent::TWriteDeviceBlocksMethod> ||
    std::is_same_v<T, TEvDiskAgent::TZeroDeviceBlocksMethod>;

template <typename T>
constexpr bool IsReadDeviceMethod =
    std::is_same_v<T, TEvDiskAgent::TReadDeviceBlocksMethod>;

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod, typename T>
void Reply(
    TActorSystem& actorSystem,
    const TActorId& replyFrom,
    TRequestInfo& request,
    T&& result,
    TBlockRange64 range,
    ui64 volumeRequestId,
    TString deviceUUID,
    ui64 started)
{
    AtomicAdd(request.ExecCycles, GetCycleCount() - started);

    if constexpr (IsWriteDeviceMethod<TMethod>) {
        auto writeCompleted =
            std::make_unique<TEvDiskAgentPrivate::TEvWriteOrZeroCompleted>(
                volumeRequestId,
                range,
                std::move(deviceUUID),
                !HasError(result));
        actorSystem.Send(
            new IEventHandle(replyFrom, replyFrom, writeCompleted.release()));
    }

    auto response =
        std::make_unique<typename TMethod::TResponse>(std::forward<T>(result));

    LWTRACK(
        ResponseSent_DiskAgent,
        request.CallContext->LWOrbit,
        TMethod::Name,
        request.CallContext->RequestId);

    actorSystem.Send(new IEventHandle(
        request.Sender,
        replyFrom,
        response.release(),
        0, // flags
        request.Cookie));
}

std::pair<ui32, TString> HandleException(
    const TActorSystem& actorSystem,
    const char* source,
    const char* methodName,
    const TString& deviceUUID,
    const TString& clientId)
{
    try {
        throw;
    } catch (const TServiceError& e) {
        const bool isHealthChecking = clientId == CheckHealthClientId;
        auto priority = isHealthChecking ? NActors::NLog::PRI_DEBUG
                                         : NActors::NLog::PRI_ERROR;
        LOG_LOG(
            actorSystem,
            priority,
            TBlockStoreComponents::DISK_AGENT,
            "%s [%s / %s] Service %s error: %s (%s)",
            methodName,
            deviceUUID.c_str(),
            clientId.c_str(),
            source,
            FormatResultCode(e.GetCode()).c_str(),
            e.what());

        return { e.GetCode(), e.what() };
    } catch (...) {
        LOG_ERROR(actorSystem, TBlockStoreComponents::DISK_AGENT,
            "%s [%s / %s] Unexpected %s error: %s",
            methodName,
            deviceUUID.c_str(),
            clientId.c_str(),
            source,
            CurrentExceptionMessage().c_str()
        );

        return { E_FAIL, CurrentExceptionMessage() };
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod, typename TEv, typename TOp>
void TDiskAgentActor::PerformIO(
    const TActorContext& ctx,
    const TEv& ev,
    TOp operation)
{
    auto* msg = ev->Get();

    ui64 volumeRequestId = 0;
    TBlockRange64 range = {};
    if constexpr (IsWriteDeviceMethod<TMethod>) {
        volumeRequestId = GetVolumeRequestId(*msg);
        range = BuildRequestBlockRange(*msg);
    } else {
        range = TBlockRange64::WithLength(
            msg->Record.GetStartIndex(),
            msg->Record.GetBlocksCount());
    }

    auto requestInfo = CreateRequestInfo<TMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    const ui64 started = GetCycleCount();

    auto& record = msg->Record;
    const auto deviceUUID = record.GetDeviceUUID();
    const auto clientId = record.GetHeaders().GetClientId();

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
            range,
            volumeRequestId,
            deviceUUID,
            started);
    };

    auto replyError = [=] (auto code, auto message) {
        Reply<TMethod>(
            *actorSystem,
            replyFrom,
            *requestInfo,
            MakeError(code, std::move(message)),
            range,
            volumeRequestId,
            deviceUUID,
            started);
    };

    LOG_TRACE(ctx, TBlockStoreComponents::DISK_AGENT,
        "%s [%s / %s]",
        TMethod::Name,
        deviceUUID.c_str(),
        clientId.c_str());

    if (SecureErasePendingRequests.contains(deviceUUID)) {
        const bool isHealthCheckRead =
            IsReadDeviceMethod<TMethod> && clientId == CheckHealthClientId;
        const bool isBackgroundOpsRead =
            IsReadDeviceMethod<TMethod> && clientId == BackgroundOpsClientId;
        if (!isHealthCheckRead && !isBackgroundOpsRead) {
            ReportDiskAgentIoDuringSecureErase(
                {{"device", deviceUUID},
                 {"client", clientId},
                 {"start", range.Start},
                 {"blocks", range.Size()},
                 {"isWrite", IsWriteDeviceMethod<TMethod>},
                 {"isRdma", 0}});
        }
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
                    auto [code, message] = HandleException(
                        *actorSystem,
                        "io",
                        TMethod::Name,
                        deviceUUID,
                        clientId);

                    replyError(code, message);
                }
            });
    } catch (...) {
        auto [code, message] = HandleException(
            *actorSystem,
            "state",
            TMethod::Name,
            deviceUUID,
            clientId);

        replyError(code, message);
    }
}

template <typename TMethod, typename TRequestPtr>
bool TDiskAgentActor::CheckIntersection(
    const NActors::TActorContext& ctx,
    const TRequestPtr& ev)
{
    auto* msg = ev->Get();

    const auto range = BuildRequestBlockRange(*msg);
    const ui64 volumeRequestId = GetVolumeRequestId(*msg);
    TString deviceUUID = msg->Record.GetDeviceUUID();
    auto& recentBlocksTracker = GetRecentBlocksTracker(deviceUUID);

    const bool overlapsWithInflightRequests =
        recentBlocksTracker.CheckInflight(volumeRequestId, range);
    if (overlapsWithInflightRequests) {
        OldRequestCounters.Delayed->Inc();
        if (!RejectLateRequestsAtDiskAgentEnabled) {
            // Monitoring mode. Don't change the behavior.
            return false;
        }
        PostponedRequests.push_back(
            {volumeRequestId, range, NActors::IEventHandlePtr(ev.Release())});
        return true;
    }

    TString overlapDetails;
    auto result = OverlapStatusToResult(recentBlocksTracker.CheckRecorded(
        volumeRequestId,
        range,
        &overlapDetails));
    if (result != S_OK) {
        if (result == E_REJECTED) {
            OldRequestCounters.Rejected->Inc();
        } else {
            Y_DEBUG_ABORT_UNLESS(false);
        }

        if (!RejectLateRequestsAtDiskAgentEnabled) {
            // Monitoring mode. Don't change the behavior.
            return false;
        }

        auto requestInfo = CreateRequestInfo<TMethod>(
            ev->Sender,
            ev->Cookie,
            msg->CallContext);

        Reply<TMethod>(
            *TActivationContext::ActorSystem(),
            ctx.SelfID,
            *requestInfo,
            MakeError(result, overlapDetails),
            {},
            volumeRequestId,
            std::move(deviceUUID),
            GetCycleCount());

        return true;
    }

    recentBlocksTracker.AddInflight(volumeRequestId, range);
    return false;
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

    auto* msg = ev->Get();
    if (!msg->Record.GetReplicationTargets().empty()) {
        NCloud::Register<TMultiAgentWriteDeviceBlocksActor>(
            ctx,
            SelfId(),
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            std::move(msg->Record),
            TMultiAgentWriteDeviceBlocksActor::TResponsePromise(),
            GetMaxRequestTimeout());
        return;
    }

    if (CheckIntersection<TMethod>(ctx, ev)) {
        return;
    }
    PerformIO<TMethod>(ctx, ev, &TDiskAgentState::Write);
}

void TDiskAgentActor::HandleParsedWriteDeviceBlocks(
    const TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(WriteDeviceBlocks);

    using TMethod = TEvDiskAgent::TWriteDeviceBlocksMethod;

    auto* msg = ev->Get();
    if (!msg->Record.GetReplicationTargets().empty()) {
        Y_DEBUG_ABORT_UNLESS(!msg->Storage);
        NCloud::Register<TMultiAgentWriteDeviceBlocksActor>(
            ctx,
            SelfId(),
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            std::move(msg->Record),
            TMultiAgentWriteDeviceBlocksActor::TResponsePromise(),
            GetMaxRequestTimeout());
        return;
    }

    if (CheckIntersection<TMethod>(ctx, ev)) {
        return;
    }

    if (!msg->Storage) {
        PerformIO<TMethod>(ctx, ev, &TDiskAgentState::Write);
        return;
    }

    // Attach storage to NProto::TWriteBlocksRequest
    struct TWriteBlocksRequestWithStorage
        : NProto::TWriteBlocksRequest
    {
        TStorageBuffer Storage;
    };

    PerformIO<TMethod>(
        ctx,
        ev,
        [storage = std::move(msg->Storage), storageSize = msg->StorageSize](
            TDiskAgentState& self,
            TInstant now,
            NProto::TWriteDeviceBlocksRequest request) mutable
        {
            auto writeRequest =
                std::make_shared<TWriteBlocksRequestWithStorage>();
            writeRequest->MutableHeaders()->Swap(request.MutableHeaders());
            writeRequest->MutableBlocks()->Swap(request.MutableBlocks());
            writeRequest->SetStartIndex(request.GetStartIndex());
            writeRequest->Storage = std::move(storage);
            if (request.HasChecksum()) {
                writeRequest->MutableChecksums()->Add()->CopyFrom(
                    request.GetChecksum());
            }

            TStringBuf buffer{writeRequest->Storage.get(), storageSize};

            return self.WriteBlocks(
                now,
                request.GetDeviceUUID(),
                std::move(writeRequest),
                request.GetBlockSize(),
                buffer);
        });
}

void TDiskAgentActor::HandleMultiAgentWriteDeviceBlocks(
    const TEvDiskAgentPrivate::TEvMultiAgentWriteDeviceBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(ev->Sender == NActors::TActorId());

    auto* msg = ev->Get();

    NCloud::Register<TMultiAgentWriteDeviceBlocksActor>(
        ctx,
        SelfId(),
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(msg->Record),
        std::move(msg->ResponsePromise),
        GetMaxRequestTimeout());
}

void TDiskAgentActor::HandleZeroDeviceBlocks(
    const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(ZeroDeviceBlocks);

    using TMethod = TEvDiskAgent::TZeroDeviceBlocksMethod;

    if (CheckIntersection<TMethod>(ctx, ev)) {
        return;
    }
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

void TDiskAgentActor::HandleWriteOrZeroCompleted(
    const TEvDiskAgentPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto* msg = ev->Get();

    auto& recentBlocksTracker = GetRecentBlocksTracker(msg->DeviceUUID);
    recentBlocksTracker.RemoveInflight(msg->RequestId);
    if (msg->Success) {
        recentBlocksTracker.AddRecorded(msg->RequestId, msg->Range);
    }

    auto executeNotOverlappedRequests =
        [&](TPostponedRequest& postponedRequest) {
            if (recentBlocksTracker.CheckInflight(
                    postponedRequest.VolumeRequestId,
                    postponedRequest.Range))
            {
                return false;
            }

            ctx.Send(postponedRequest.Event.release());
            return true;
        };

    std::erase_if(PostponedRequests, executeNotOverlappedRequests);
}

TRecentBlocksTracker& TDiskAgentActor::GetRecentBlocksTracker(
    const TString& deviceUUID)
{
    if (auto* tracker = RecentBlocksTrackers.FindPtr(deviceUUID)) {
        return *tracker;
    }
    auto [it, inserted] = RecentBlocksTrackers.insert(
        {deviceUUID, TRecentBlocksTracker(deviceUUID)});
    Y_ABORT_UNLESS(inserted);
    return it->second;
}

}   // namespace NCloud::NBlockStore::NStorage
