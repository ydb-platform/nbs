#include "part_nonrepl_actor.h"

#include "part_nonrepl_actor_base_request.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
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

struct TPartialChecksum
{
    ui64 Value;
    ui64 Size;
};

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentChecksumActor final: public TDiskAgentBaseRequestActor
{
private:
    const NProto::TChecksumBlocksRequest Request;

    TMap<ui64, TPartialChecksum> Checksums;
    ui32 RequestsCompleted = 0;

public:
    TDiskAgentChecksumActor(
        TRequestInfoPtr requestInfo,
        NProto::TChecksumBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId volumeActorId,
        const TActorId& part,
        TChildLogTitle logTitle,
        ui64 deviceOperationId);

protected:
    void SendRequest(const NActors::TActorContext& ctx) override;
    NActors::IEventBasePtr MakeResponse(NProto::TError error) override;
    TCompletionEventAndBody MakeCompletionResponse(ui32 blocks) override;
    bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;

private:
    void HandleChecksumDeviceBlocksResponse(
        const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleChecksumDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentChecksumActor::TDiskAgentChecksumActor(
        TRequestInfoPtr requestInfo,
        NProto::TChecksumBlocksRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId volumeActorId,
        const TActorId& part,
        TChildLogTitle logTitle,
        ui64 deviceOperationId)
    : TDiskAgentBaseRequestActor(
          std::move(requestInfo),
          GetRequestId(request),
          "ChecksumBlocks",
          std::move(timeoutPolicy),
          std::move(deviceRequests),
          std::move(partConfig),
          volumeActorId,
          part,
          std::move(logTitle),
          deviceOperationId)
    , Request(std::move(request))
{}

void TDiskAgentChecksumActor::SendRequest(const TActorContext& ctx)
{
    const auto blockSize = PartConfig->GetBlockSize();

    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvChecksumDeviceBlocksRequest>();
        request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
        request->Record.SetDeviceUUID(deviceRequest.Device.GetDeviceUUID());
        request->Record.SetStartIndex(deviceRequest.DeviceBlockRange.Start);
        request->Record.SetBlockSize(blockSize);
        request->Record.SetBlocksCount(deviceRequest.DeviceBlockRange.Size());

        auto latencyStartEvent =
            std::make_unique<TEvVolumePrivate::TEvDeviceOperationStarted>(
                TEvVolumePrivate::TDeviceOperationStarted(
                    deviceRequest.Device.GetDeviceUUID(),
                    TEvVolumePrivate::TDeviceOperationStarted::ERequestType::
                        Checksum,
                    deviceRequest.Device.GetAgentId(),
                    DeviceOperationId + cookie));
        ctx.Send(VolumeActorId, latencyStartEvent.release());

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(deviceRequest.Device.GetNodeId()),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            cookie++,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(event.release());
    }
}

NActors::IEventBasePtr TDiskAgentChecksumActor::MakeResponse(
    NProto::TError error)
{
    return std::make_unique<
        TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>(
        std::move(error));
}

TDiskAgentBaseRequestActor::TCompletionEventAndBody
TDiskAgentChecksumActor::MakeCompletionResponse(ui32 blocks)
{
    auto completion = std::make_unique<
        TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted>();

    completion->Stats.MutableSysChecksumCounters()->SetBlocksCount(blocks);

    return TCompletionEventAndBody(std::move(completion));
}

void TDiskAgentChecksumActor::HandleChecksumDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvChecksumDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s ChecksumBlocks request #%lu undelivered. Device: %s",
        LogTitle.GetWithTime().c_str(),
        GetRequestId(Request),
        LogDevice(device).c_str());

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentChecksumActor::HandleChecksumDeviceBlocksResponse(
    const TEvDiskAgent::TEvChecksumDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError(), EStatus::Fail);
        return;
    }

    const auto& deviceRequest = DeviceRequests[ev->Cookie];
    const auto rangeStart = deviceRequest.BlockRange.Start;
    const auto rangeSize = deviceRequest.DeviceBlockRange.Size() * PartConfig->GetBlockSize();
    Checksums[rangeStart] = {msg->Record.GetChecksum(), rangeSize};

    auto latencyFinishEvent =
        std::make_unique<TEvVolumePrivate::TEvDeviceOperationFinished>(
            TEvVolumePrivate::TDeviceOperationFinished(
                DeviceOperationId + ev->Cookie));
    ctx.Send(VolumeActorId, latencyFinishEvent.release());

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    TBlockChecksum checksum;
    for (const auto& [_, partialChecksum]: Checksums) {
        checksum.Combine(partialChecksum.Value, partialChecksum.Size);
    }

    auto response = std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>();
    response->Record.SetChecksum(checksum.GetValue());

    Done(ctx, std::move(response), EStatus::Success);
}

bool TDiskAgentChecksumActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvChecksumDeviceBlocksRequest,
            HandleChecksumDeviceBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvChecksumDeviceBlocksResponse,
            HandleChecksumDeviceBlocksResponse);
        default:
            return false;
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleChecksumBlocks(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ChecksumBlocks",
        requestInfo->CallContext->RequestId);

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    TRequestData request;
    bool ok = InitRequests<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
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

    ui64 operationId = DeviceOperationId;
    DeviceOperationId += deviceRequests.size();

    auto actorId = NCloud::Register<TDiskAgentChecksumActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        VolumeActorId,
        SelfId(),
        LogTitle.GetChild(GetCycleCount()),
        operationId);

    RequestsInProgress.AddReadRequest(actorId, std::move(request));
}

void TNonreplicatedPartitionActor::HandleChecksumBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete checksum blocks",
        LogTitle.GetWithTime().c_str());

    const auto requestBytes = msg->Stats.GetSysChecksumCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ChecksumBlocks.AddRequest(time, requestBytes);
    NetworkBytes += sizeof(ui64);   //  Checksum is sent as a 64-bit integer.
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    OnRequestCompleted(*msg, ctx.Now());
    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
