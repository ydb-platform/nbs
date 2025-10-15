#include "part_nonrepl_actor.h"
#include "part_nonrepl_actor_base_request.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentZeroActor final: public TDiskAgentBaseRequestActor
{
private:
    const NProto::TZeroBlocksRequest Request;
    const ui32 BlockSize;
    const bool AssignVolumeRequestId;

    ui32 RequestsCompleted = 0;

public:
    TDiskAgentZeroActor(
        TRequestInfoPtr requestInfo,
        NProto::TZeroBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId volumeActorId,
        const TActorId& part,
        ui32 blockSize,
        bool assignVolumeRequestId,
        TChildLogTitle logTitle,
        ui64 deviceOperationId);

protected:
    void SendRequest(const NActors::TActorContext& ctx) override;
    NActors::IEventBasePtr MakeResponse(NProto::TError error) override;
    TCompletionEventAndBody MakeCompletionResponse(ui32 blocks) override;
    bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;

private:
    void HandleZeroDeviceBlocksResponse(
        const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleZeroDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentZeroActor::TDiskAgentZeroActor(
        TRequestInfoPtr requestInfo,
        NProto::TZeroBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId volumeActorId,
        const TActorId& part,
        ui32 blockSize,
        bool assignVolumeRequestId,
        TChildLogTitle logTitle,
        ui64 deviceOperationId)
    :TDiskAgentBaseRequestActor(
          std::move(requestInfo),
          GetRequestId(request),
          "ZeroBlocks",
          std::move(timeoutPolicy),
          std::move(deviceRequests),
          std::move(partConfig),
          volumeActorId,
          part,
          std::move(logTitle),
          deviceOperationId)
    , Request(std::move(request))
    , BlockSize(blockSize)
    , AssignVolumeRequestId(assignVolumeRequestId)
{}

void TDiskAgentZeroActor::SendRequest(const TActorContext& ctx)
{
    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();
        request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
        request->Record.SetDeviceUUID(deviceRequest.Device.GetDeviceUUID());
        request->Record.SetStartIndex(deviceRequest.DeviceBlockRange.Start);
        request->Record.SetBlockSize(BlockSize);
        request->Record.SetBlocksCount(deviceRequest.DeviceBlockRange.Size());
        if (AssignVolumeRequestId) {
            request->Record.SetVolumeRequestId(
                Request.GetHeaders().GetVolumeRequestId());
        }

        if (DeviceOperationId) {
            auto latencyStartEvent = std::make_unique<
                TEvVolumePrivate::TEvDiskRegistryDeviceOperationStarted>(
                TEvVolumePrivate::TDiskRegistryDeviceOperationStarted(
                    deviceRequest.Device.GetDeviceUUID(),
                    TDeviceOperationTracker::ERequestType::Zero,
                    DeviceOperationId + cookie));
            ctx.Send(VolumeActorId, latencyStartEvent.release());
        }

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(deviceRequest.Device.GetNodeId()),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            cookie++,
            &ctx.SelfID // forwardOnNondelivery
        );

        ctx.Send(std::move(event));
    }
}

NActors::IEventBasePtr TDiskAgentZeroActor::MakeResponse(
    NProto::TError error)
{
    return std::make_unique<TEvService::TEvZeroBlocksResponse>(
        std::move(error));
}

TDiskAgentBaseRequestActor::TCompletionEventAndBody
TDiskAgentZeroActor::MakeCompletionResponse(ui32 blocks)
{
    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted>();

    completion->Stats.MutableUserWriteCounters()->SetBlocksCount(blocks);

    return TCompletionEventAndBody(std::move(completion));
}

void TDiskAgentZeroActor::HandleZeroDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s ZeroBlocks request #%lu undelivered. Device: %s",
        LogTitle.GetWithTime().c_str(),
        GetRequestId(Request),
        LogDevice(device).c_str());

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentZeroActor::HandleZeroDeviceBlocksResponse(
    const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError(), EStatus::Fail);
        return;
    }

    if (DeviceOperationId) {
        auto latencyFinishEvent = std::make_unique<
            TEvVolumePrivate::TEvDiskRegistryDeviceOperationFinished>(
            TEvVolumePrivate::TDiskRegistryDeviceOperationFinished(
                DeviceOperationId + ev->Cookie));
        ctx.Send(VolumeActorId, latencyFinishEvent.release());
    }

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>();
    Done(ctx, std::move(response), EStatus::Success);
}

bool TDiskAgentZeroActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvZeroDeviceBlocksRequest,
            HandleZeroDeviceBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvZeroDeviceBlocksResponse,
            HandleZeroDeviceBlocksResponse);
        default:
            return false;
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TZeroBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ZeroBlocks",
        requestInfo->CallContext->RequestId);

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    TRequestData request;
    bool ok = InitRequests<TEvService::TZeroBlocksMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests,
        &timeoutPolicy,
        &request);

    if (!ok) {
        return;
    }

    ui32 trackingFreq = Config->GetDeviceOperationTrackingFrequency();
    ui64 operationId =
        (trackingFreq > 0 && DeviceOperationId % trackingFreq == 0)
            ? DeviceOperationId
            : 0;
    DeviceOperationId += deviceRequests.size();

    auto actorId = NCloud::Register<TDiskAgentZeroActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        VolumeActorId,
        SelfId(),
        PartConfig->GetBlockSize(),
        Config->GetAssignIdToWriteAndZeroRequestsEnabled(),
        LogTitle.GetChild(GetCycleCount()),
        operationId);

    RequestsInProgress.AddWriteRequest(actorId, std::move(request));
}

void TNonreplicatedPartitionActor::HandleZeroBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete zero blocks",
        LogTitle.GetWithTime().c_str());

    UpdateStats(msg->Stats);

    const auto requestBytes = msg->Stats.GetUserWriteCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ZeroBlocks.AddRequest(time, requestBytes);
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    OnRequestCompleted(*msg, ctx.Now());
    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
