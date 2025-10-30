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

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

TDuration CalcOverallTimeout(
    const NProto::TMultiAgentWriteRequest& request,
    TDuration networkForwardingTimeout)
{
    TDuration maxRequestTimeout;
    for (const auto& deviceInfo: request.DevicesAndRanges) {
        maxRequestTimeout = Max(maxRequestTimeout, deviceInfo.RequestTimeout);
    }

    return maxRequestTimeout + networkForwardingTimeout;
}

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentMultiWriteActor final: public TDiskAgentBaseRequestActor
{
private:
    const bool AssignVolumeRequestId;

    NProto::TMultiAgentWriteRequest Request;

public:
    TDiskAgentMultiWriteActor(
        TRequestInfoPtr requestInfo,
        NProto::TMultiAgentWriteRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId volumeActorId,
        const TActorId& part,
        bool assignVolumeRequestId,
        TChildLogTitle logTitle,
        ui64 deviceOperationId);

protected:
    void SendRequest(const NActors::TActorContext& ctx) override;
    NActors::IEventBasePtr MakeResponse(NProto::TError error) override;
    TCompletionEventAndBody MakeCompletionResponse(ui32 blocks) override;
    bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;

private:
    void HandleWriteDeviceBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteDeviceBlocksUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskAgentMultiWriteActor::TDiskAgentMultiWriteActor(
        TRequestInfoPtr requestInfo,
        NProto::TMultiAgentWriteRequest request,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        TActorId volumeActorId,
        const TActorId& part,
        bool assignVolumeRequestId,
        TChildLogTitle logTitle,
        ui64 deviceOperationId)
    : TDiskAgentBaseRequestActor(
          std::move(requestInfo),
          GetRequestId(request),
          "MultiAgentWriteBlocks",
          std::move(timeoutPolicy),
          std::move(deviceRequests),
          std::move(partConfig),
          volumeActorId,
          part,
          std::move(logTitle),
          deviceOperationId)
    , AssignVolumeRequestId(assignVolumeRequestId)
    , Request(std::move(request))
{}

void TDiskAgentMultiWriteActor::SendRequest(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();

    *request->Record.MutableHeaders() = Request.GetHeaders();
    request->Record.SetBlockSize(Request.BlockSize);
    request->Record.MutableBlocks()->Swap(Request.MutableBlocks());

    for (const auto& deviceInfo: Request.DevicesAndRanges) {
        auto* replicationTarget = request->Record.AddReplicationTargets();
        replicationTarget->SetNodeId(deviceInfo.Device.GetNodeId());
        replicationTarget->SetDeviceUUID(deviceInfo.Device.GetDeviceUUID());
        replicationTarget->SetStartIndex(deviceInfo.DeviceBlockRange.Start);
        replicationTarget->SetTimeout(deviceInfo.RequestTimeout.MilliSeconds());
    }

    if (AssignVolumeRequestId) {
        request->Record.SetVolumeRequestId(
            Request.GetHeaders().GetVolumeRequestId());
    }

    if (auto checksum = CombineChecksums(Request.GetChecksums());
        checksum.GetByteCount() > 0)
    {
        if (checksum.GetByteCount() == Request.Range.Size() * Request.BlockSize)
        {
            *request->Record.MutableChecksum() = std::move(checksum);
        } else {
            ReportChecksumCalculationError(
                "DiskAgentMultiWriteActor: Incorrectly calculated checksum for "
                "block range",
                {{"range", Request.Range.Print()},
                 {"request range length", Request.Range.Size()},
                 {"checksum length",
                  checksum.GetByteCount() / Request.BlockSize},
                 {"disk id", PartConfig->GetName().Quote()}});
        }
    }

    OnRequestStarted(
        ctx,
        Request.DevicesAndRanges[0].Device.GetDeviceUUID(),
        TDeviceOperationTracker::ERequestType::Write,
        0);

    auto event = std::make_unique<NActors::IEventHandle>(
        MakeDiskAgentServiceId(Request.DevicesAndRanges[0].Device.GetNodeId()),
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());
}

NActors::IEventBasePtr TDiskAgentMultiWriteActor::MakeResponse(
    NProto::TError error)
{
    return std::make_unique<
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
        std::move(error));
}

TDiskAgentBaseRequestActor::TCompletionEventAndBody
TDiskAgentMultiWriteActor::MakeCompletionResponse(ui32 blocks)
{
    auto completion = std::make_unique<
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted>();

    completion->Stats.MutableUserWriteCounters()->SetBlocksCount(blocks);

    return TCompletionEventAndBody(std::move(completion));
}

void TDiskAgentMultiWriteActor::HandleWriteDeviceBlocksUndelivery(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    OnRequestFinished(ctx, ev->Cookie);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s MultiAgentWriteBlocks request #%lu undelivered. Device: %s",
        LogTitle.GetWithTime().c_str(),
        GetRequestId(Request),
        LogDevice(Request.DevicesAndRanges[0].Device).c_str());

    // Ignore undelivered event. Wait for TEvWakeup.
}

void TDiskAgentMultiWriteActor::HandleWriteDeviceBlocksResponse(
    const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    OnRequestFinished(ctx, ev->Cookie);

    auto replyInconsistentError = [&]()
    {
        auto response = std::make_unique<
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
            MakeError(E_REJECTED));
        response->Record.InconsistentResponse = true;
        Done(ctx, std::move(response), EStatus::Fail);
    };

    const bool subResponsesPresent =
        msg->Record.GetReplicationResponses().size() ==
        static_cast<int>(Request.DevicesAndRanges.size());

    if (!subResponsesPresent) {
        replyInconsistentError();
        return;
    }

    auto error = msg->GetError();

    if (HasError(error)) {
        const bool executorTimeout =
            msg->Record.GetReplicationResponses(0).GetCode() == E_TIMEOUT;

        HandleError(
            ctx,
            error,
            executorTimeout ? EStatus::Timeout : EStatus::Fail);
        return;
    }

    bool subResponsesOk = AllOf(
        msg->Record.GetReplicationResponses(),
        [](const NProto::TError& subResponseError)
        { return subResponseError.GetCode() == S_OK; });

    if (error.GetCode() != S_OK || !subResponsesOk) {
        replyInconsistentError();
        return;
    }

    Y_DEBUG_ABORT_UNLESS(error.GetCode() == S_OK);
    Done(ctx, MakeResponse(std::move(error)), EStatus::Success);
}

bool TDiskAgentMultiWriteActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksRequest,
            HandleWriteDeviceBlocksUndelivery);
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksResponse,
            HandleWriteDeviceBlocksResponse);
        default:
            return false;
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleMultiAgentWrite(
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfoWithResponse<
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "MultiAgentWriteBlocks",
        requestInfo->CallContext->RequestId);

    auto replyError = [&](ui32 errorCode, TString errorReason)
    {
        auto response = std::make_unique<
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
            PartConfig->MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "MultiAgentWriteBlocks",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    };

    TVector<TDeviceRequest> deviceRequests;
    TRequestTimeoutPolicy timeoutPolicy;
    TRequestData request;
    bool ok = InitRequests<
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
        "MultiAgentWriteBlocks",
        true,
        *msg,
        ctx,
        *requestInfo,
        msg->Record.Range,
        &deviceRequests,
        &timeoutPolicy,
        &request);

    if (!ok) {
        return;
    }

    if (deviceRequests.size() != 1) {
        // TMultiAgentWriteActor perform TEvMultiAgentWriteRequest only if all
        // TEvGetDeviceForRangeRequests to replicas have returned success. These
        // requests are response with an error if the request hits two disk-agents.
        ReportMultiAgentRequestAffectsTwoDevices(
            "partActor",
            {{"disk", PartConfig->GetName()}, {"range", msg->Record.Range}});
        replyError(
            E_ARGUMENT,
            "Can't execute MultiAgentWriteBlocks request cross device borders");
        return;
    }

    timeoutPolicy.Timeout =
        CalcOverallTimeout(msg->Record, Config->GetNetworkForwardingTimeout());

    ui64 operationId = GenerateOperationId(deviceRequests.size());

    auto actorId = NCloud::Register<TDiskAgentMultiWriteActor>(
        ctx,
        requestInfo,
        std::move(msg->Record),
        std::move(timeoutPolicy),
        std::move(deviceRequests),
        PartConfig,
        VolumeActorId,
        SelfId(),
        Config->GetAssignIdToWriteAndZeroRequestsEnabled(),
        LogTitle.GetChild(GetCycleCount()),
        operationId);

    RequestsInProgress.AddWriteRequest(actorId, std::move(request));
}

void TNonreplicatedPartitionActor::HandleMultiAgentWriteBlocksCompleted(
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted::TPtr&
        ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete multi agent write blocks",
        LogTitle.GetWithTime().c_str());

    UpdateStats(msg->Stats);

    const auto requestBytes =
        msg->Stats.GetUserWriteCounters().GetBlocksCount() *
        PartConfig->GetBlockSize();
    const auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.WriteBlocksMultiAgent.AddRequest(time, requestBytes);
    PartCounters->Interconnect.WriteBytesMultiAgent.Increment(requestBytes);
    PartCounters->Interconnect.WriteCountMultiAgent.Increment(1);

    NetworkBytes += requestBytes;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    RequestsInProgress.RemoveRequest(ev->Sender);
    OnRequestCompleted(*msg, ctx.Now());

    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
