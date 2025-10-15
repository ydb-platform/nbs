#include "part_nonrepl_actor.h"

#include "part_nonrepl_actor_base_request.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using EReason = TEvNonreplPartitionPrivate::TCancelRequest::EReason;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentReadActor final: public TDiskAgentBaseRequestActor
{
private:
    const NProto::TReadBlocksRequest Request;
    const bool SkipVoidBlocksToOptimizeNetworkTransfer;

    ui32 RequestsCompleted = 0;
    ui32 VoidBlockCount = 0;
    ui32 NonVoidBlockCount = 0;

    NProto::TReadBlocksResponse Response;
    TVector<NProto::TChecksum> Checksums;

public:
    TDiskAgentReadActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksRequest request,
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
    void HandleReadDeviceBlocksResponse(
        const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentReadActor::TDiskAgentReadActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksRequest request,
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
          "ReadBlocks",
          std::move(timeoutPolicy),
          std::move(deviceRequests),
          std::move(partConfig),
          volumeActorId,
          part,
          std::move(logTitle),
          deviceOperationId)
    , Request(std::move(request))
    , SkipVoidBlocksToOptimizeNetworkTransfer(
          Request.GetHeaders().GetOptimizeNetworkTransfer() ==
          NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS)
{
    Checksums.resize(DeviceRequests.size());
}

void TDiskAgentReadActor::SendRequest(const TActorContext& ctx)
{
    const auto blockRange = TBlockRange64::WithLength(
        Request.GetStartIndex(),
        Request.GetBlocksCount()
    );

    const auto blockSize = PartConfig->GetBlockSize();

    auto* responseBuffers = Response.MutableBlocks()->MutableBuffers();
    responseBuffers->Reserve(blockRange.Size());
    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        responseBuffers->Add();
    }

    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
        request->Record.SetDeviceUUID(deviceRequest.Device.GetDeviceUUID());
        request->Record.SetStartIndex(deviceRequest.DeviceBlockRange.Start);
        request->Record.SetBlockSize(blockSize);
        request->Record.SetBlocksCount(deviceRequest.DeviceBlockRange.Size());

        if (DeviceOperationId) {
            auto latencyStartEvent = std::make_unique<
                TEvVolumePrivate::TEvDiskRegistryDeviceOperationStarted>(
                TEvVolumePrivate::TDiskRegistryDeviceOperationStarted(
                    deviceRequest.Device.GetDeviceUUID(),
                    TDeviceOperationTracker::ERequestType::Read,
                    DeviceOperationId + cookie));
            ctx.Send(VolumeActorId, latencyStartEvent.release());
        }

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(deviceRequest.Device.GetNodeId()),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            cookie++,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(std::move(event));
    }
}

NActors::IEventBasePtr TDiskAgentReadActor::MakeResponse(
    NProto::TError error)
{
    return std::make_unique<TEvService::TEvReadBlocksResponse>(
        std::move(error));
}

TDiskAgentBaseRequestActor::TCompletionEventAndBody
TDiskAgentReadActor::MakeCompletionResponse(ui32 blocks)
{
    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>();

    completion->Stats.MutableUserReadCounters()->SetBlocksCount(blocks);
    completion->NonVoidBlockCount = NonVoidBlockCount;
    completion->VoidBlockCount = VoidBlockCount;

    return TCompletionEventAndBody(std::move(completion));
}

void TDiskAgentReadActor::HandleReadDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s ReadBlocks request #%lu undelivered. Device: %s",
        LogTitle.GetWithTime().c_str(),
        GetRequestId(Request),
        LogDevice(device).c_str());

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentReadActor::HandleReadDeviceBlocksResponse(
    const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError(), EStatus::Fail);
        return;
    }

    Checksums[ev->Cookie].CopyFrom(msg->Record.GetChecksum());
    auto& srcBuffers = *msg->Record.MutableBlocks()->MutableBuffers();
    auto& destBuffers = *Response.MutableBlocks()->MutableBuffers();
    const auto blockSize = PartConfig->GetBlockSize();

    const auto& blockRange = DeviceRequests[ev->Cookie].BlockRange;
    Y_ABORT_UNLESS(msg->Record.GetBlocks().BuffersSize() == blockRange.Size());
    for (ui32 i = 0; i < blockRange.Size(); ++i) {
        auto& srcBuffer = srcBuffers[i];
        auto& destBuffer =
            destBuffers[blockRange.Start + i - Request.GetStartIndex()];
        destBuffer.swap(srcBuffer);
        if (destBuffer.empty()) {
            STORAGE_CHECK_PRECONDITION(SkipVoidBlocksToOptimizeNetworkTransfer);
            destBuffer.resize(blockSize, 0);
            ++VoidBlockCount;
        } else {
            ++NonVoidBlockCount;
        }
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

    auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
    response->Record = std::move(Response);
    response->Record.SetAllZeroes(VoidBlockCount == Request.GetBlocksCount());
    if (auto checksum = CombineChecksums(Checksums);
        checksum.GetByteCount() > 0)
    {
        *response->Record.MutableChecksum() = std::move(checksum);
    }
    Done(ctx, std::move(response), EStatus::Success);
}

bool TDiskAgentReadActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvReadDeviceBlocksRequest,
            HandleReadDeviceBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvReadDeviceBlocksResponse,
            HandleReadDeviceBlocksResponse);
        default:
            return false;
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TReadBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    TRequestData request;
    bool ok = InitRequests<TEvService::TReadBlocksMethod>(
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

    if (Config->GetOptimizeVoidBuffersTransferForReadsEnabled()) {
        msg->Record.MutableHeaders()->SetOptimizeNetworkTransfer(
            NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS);
    }

    ui32 trackingFreq = Config->GetDeviceOperationTrackingFrequency();
    ui64 operationId =
        (trackingFreq > 0 && DeviceOperationId % trackingFreq == 0)
            ? DeviceOperationId
            : 0;
    DeviceOperationId += deviceRequests.size();

    auto actorId = NCloud::Register<TDiskAgentReadActor>(
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

void TNonreplicatedPartitionActor::HandleReadBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete read blocks",
        LogTitle.GetWithTime().c_str());

    UpdateStats(msg->Stats);

    const auto requestBytes = msg->Stats.GetUserReadCounters().GetBlocksCount()
        * PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ReadBlocks.AddRequest(time, requestBytes);

    PartCounters->Interconnect.ReadBytes.Increment(requestBytes);
    PartCounters->Interconnect.ReadCount.Increment(1);

    const ui64 nonVoidBytes =
        static_cast<ui64>(msg->NonVoidBlockCount) * PartConfig->GetBlockSize();
    const ui64 voidBytes =
        static_cast<ui64>(msg->VoidBlockCount) * PartConfig->GetBlockSize();
    PartCounters->RequestCounters.ReadBlocks.RequestNonVoidBytes +=
        nonVoidBytes;
    PartCounters->RequestCounters.ReadBlocks.RequestVoidBytes += voidBytes;

    NetworkBytes += nonVoidBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    OnRequestCompleted(*msg, ctx.Now());

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
