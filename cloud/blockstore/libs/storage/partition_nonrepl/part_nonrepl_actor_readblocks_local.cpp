#include "part_nonrepl_actor.h"

#include "part_nonrepl_actor_base_request.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using EReason = TEvNonreplPartitionPrivate::TCancelRequest::EReason;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentReadLocalActor final
    : public TDiskAgentBaseRequestActor
{
private:
    const NProto::TReadBlocksLocalRequest Request;
    const bool SkipVoidBlocksToOptimizeNetworkTransfer;
    const TBlockRange64 BlockRange;
    const bool ShouldReportBlockRangeOnFailure;

    ui32 RequestsCompleted = 0;
    ui32 VoidBlockCount = 0;
    ui32 NonVoidBlockCount = 0;

    TVector<NProto::TChecksum> Checksums;

public:
    TDiskAgentReadLocalActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksLocalRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TBlockRange64 range,
        bool shouldReportBlockRangeOnFailure,
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

TDiskAgentReadLocalActor::TDiskAgentReadLocalActor(
        TRequestInfoPtr requestInfo,
        NProto::TReadBlocksLocalRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TBlockRange64 range,
        bool shouldReportBlockRangeOnFailure,
        TActorId volumeActorId,
        const TActorId& part,
        TChildLogTitle logTitle,
        ui64 deviceOperationId)
    : TDiskAgentBaseRequestActor(
          std::move(requestInfo),
          GetRequestId(request),
          "ReadBlocksLocal",
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
    , BlockRange(range)
    , ShouldReportBlockRangeOnFailure(shouldReportBlockRangeOnFailure)

{
    Checksums.resize(DeviceRequests.size());
}

void TDiskAgentReadLocalActor::SendRequest(const TActorContext& ctx)
{
    const auto blockSize = PartConfig->GetBlockSize();

    ui32 cookie = 0;
    for (const auto& deviceRequest: DeviceRequests) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();
        request->Record.MutableHeaders()->CopyFrom(Request.GetHeaders());
        request->Record.SetDeviceUUID(deviceRequest.Device.GetDeviceUUID());
        request->Record.SetStartIndex(deviceRequest.DeviceBlockRange.Start);
        request->Record.SetBlockSize(blockSize);
        request->Record.SetBlocksCount(deviceRequest.DeviceBlockRange.Size());

        auto latencyStartEvent = std::make_unique<
            TEvVolumePrivate::TEvDeviceOperationStarted>(
            TEvVolumePrivate::TDeviceOperationStarted(
                deviceRequest.Device.GetDeviceUUID(),
                TEvVolumePrivate::TDeviceOperationStarted::ERequestType::Read,
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

NActors::IEventBasePtr TDiskAgentReadLocalActor::MakeResponse(
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
        std::move(error));
    if (ShouldReportBlockRangeOnFailure) {
        response->Record.FailInfo.FailedRanges.push_back(
            DescribeRange(BlockRange));
    }
    return response;
}

TDiskAgentBaseRequestActor::TCompletionEventAndBody
TDiskAgentReadLocalActor::MakeCompletionResponse(ui32 blocks)
{
    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>();

    completion->Stats.MutableUserReadCounters()->SetBlocksCount(blocks);
    completion->NonVoidBlockCount = NonVoidBlockCount;
    completion->VoidBlockCount = VoidBlockCount;

    return TCompletionEventAndBody(std::move(completion));
}

void TDiskAgentReadLocalActor::HandleReadDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvReadDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s ReadBlocksLocal request #%lu undelivered. Device: %s",
        LogTitle.GetWithTime().c_str(),
        GetRequestId(Request),
        LogDevice(device).c_str());

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentReadLocalActor::HandleReadDeviceBlocksResponse(
    const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError(), EStatus::Fail);
        return;
    }

    auto guard = Request.Sglist.Acquire();

    if (!guard) {
        HandleError(
            ctx,
            PartConfig->MakeError(
                E_CANCELLED,
                "failed to acquire sglist in DiskAgentReadActor"),
            EStatus::Fail);
        return;
    }

    Checksums[ev->Cookie].CopyFrom(msg->Record.GetChecksum());

    const auto blockRange = DeviceRequests[ev->Cookie].BlockRange;

    if (blockRange.Size() != 0) {
        auto voidBlockStat = CopyToSgList(
            msg->Record.GetBlocks(),
            guard.Get(),
            blockRange.Start - Request.GetStartIndex(),
            PartConfig->GetBlockSize());

        if (SkipVoidBlocksToOptimizeNetworkTransfer) {
            NonVoidBlockCount +=
                voidBlockStat.TotalBlockCount - voidBlockStat.VoidBlockCount;
            VoidBlockCount += voidBlockStat.VoidBlockCount;
        } else {
            STORAGE_CHECK_PRECONDITION(voidBlockStat.VoidBlockCount == 0);
        }
    }

    auto latencyFinishEvent =
        std::make_unique<TEvVolumePrivate::TEvDeviceOperationFinished>(
            TEvVolumePrivate::TDeviceOperationFinished(
                DeviceOperationId + ev->Cookie));
    ctx.Send(VolumeActorId, latencyFinishEvent.release());

    if (++RequestsCompleted < DeviceRequests.size()) {
        return;
    }

    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
    response->Record.SetAllZeroes(VoidBlockCount == Request.GetBlocksCount());
    if (auto checksum = CombineChecksums(Checksums);
        checksum.GetByteCount() > 0)
    {
        *response->Record.MutableChecksum() = std::move(checksum);
    }

    Done(ctx, std::move(response), EStatus::Success);
}

bool TDiskAgentReadLocalActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
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

void TNonreplicatedPartitionActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TReadBlocksLocalMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    TRequestData request;
    bool ok = InitRequests<TEvService::TReadBlocksLocalMethod>(
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

    ui64 operationId = DeviceOperationId;
    DeviceOperationId += deviceRequests.size();

    auto actorId = NCloud::Register<TDiskAgentReadLocalActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        blockRange,
        msg->Record.ShouldReportFailedRangesOnFailure,
        VolumeActorId,
        SelfId(),
        LogTitle.GetChild(GetCycleCount()),
        operationId);

    RequestsInProgress.AddReadRequest(actorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
