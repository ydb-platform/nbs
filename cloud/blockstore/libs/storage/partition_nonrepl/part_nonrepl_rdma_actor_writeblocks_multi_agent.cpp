#include "part_nonrepl_rdma_actor.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceRequestInfo
{
    NRdma::IClientEndpointPtr Endpoint;
    NRdma::TClientRequestPtr ClientRequest;
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaMultiWriteBlocksResponseHandler: public NRdma::IClientHandler
{
private:
    TActorSystem* ActorSystem;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TRequestInfoPtr RequestInfo;
    const ui32 RequestBlockCount;
    const size_t ReplicationTargetCount = 0;
    const NActors::TActorId ParentActorId;
    const ui64 RequestId;

    TAdaptiveLock Lock;
    NProto::TError Error;
    bool InconsistentResponse = false;

    // Indices of devices that participated in the request.
    TStackVec<ui32, 2> DeviceIndices;

    // Indices of devices where requests have resulted in errors.
    TStackVec<ui32, 2> ErrorDeviceIndices;

public:
    TRdmaMultiWriteBlocksResponseHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            ui32 requestBlockCount,
            size_t replicationTargetCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : ActorSystem(actorSystem)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , RequestBlockCount(requestBlockCount)
        , ReplicationTargetCount(replicationTargetCount)
        , ParentActorId(parentActorId)
        , RequestId(requestId)
    {}

    void HandleResult(TStringBuf buffer)
    {
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);

        if (HasError(err)) {
            Error = std::move(err);
            return;
        }

        const auto& concreteProto =
            static_cast<NProto::TWriteDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            Error = concreteProto.GetError();
            return;
        }

        bool subResponsesOk =
            concreteProto.GetReplicationResponses().size() ==
                static_cast<int>(ReplicationTargetCount) &&
            AllOf(
                concreteProto.GetReplicationResponses(),
                [](const NProto::TError& subResponseError)
                { return subResponseError.GetCode() == S_OK; });
        if (!subResponsesOk) {
            TString subResponses = Accumulate(
                concreteProto.GetReplicationResponses(),
                TString{},
                [](const TString& acc, const NProto::TError& err)
                {
                    return acc ? acc + "," + FormatError(err)
                               : FormatError(err);
                });
            Error = MakeError(E_REJECTED, "Responses: [" + subResponses + "]");
            InconsistentResponse = true;
        }
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        TRequestScope timer(*RequestInfo);

        auto guard = Guard(Lock);

        const auto buffer = req->ResponseBuffer.Head(responseBytes);
        const auto* reqCtx =
            static_cast<TDeviceRequestRdmaContext*>(req->Context.get());

        DeviceIndices.emplace_back(reqCtx->DeviceIdx);

        if (status == NRdma::RDMA_PROTO_OK) {
            HandleResult(buffer);
        } else {
            Error = NRdma::ParseError(buffer);
            ConvertRdmaErrorIfNeeded(status, Error);
            if (NeedToNotifyAboutDeviceRequestError(Error)) {
                ErrorDeviceIndices.emplace_back(reqCtx->DeviceIdx);
            }
        }

        ProcessError(*ActorSystem, *PartConfig, Error);

        {
            // Send response.
            auto response = std::make_unique<
                TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(Error);
            response->Record.InconsistentResponse = InconsistentResponse;
            SendEvent(
                RequestInfo->Sender,
                std::move(response),
                RequestInfo->Cookie);
        }

        {
            // Send completion event.
            using TCompletionEvent =
                TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted;
            auto completion =
                std::make_unique<TCompletionEvent>(std::move(Error));
            auto& counters = *completion->Stats.MutableUserWriteCounters();
            counters.SetBlocksCount(RequestBlockCount);

            completion->TotalCycles = RequestInfo->GetTotalCycles();
            completion->DeviceIndices = DeviceIndices;
            completion->ErrorDeviceIndices = ErrorDeviceIndices;

            timer.Finish();
            completion->ExecCycles = RequestInfo->GetExecCycles();

            SendEvent(ParentActorId, std::move(completion), RequestId);
        }
    }

    void SendEvent(
        NActors::TActorId recipient,
        std::unique_ptr<IEventBase> ev,
        ui64 cookie) const
    {
        auto completionEvent = std::make_unique<IEventHandle>(
            recipient,
            TActorId(),
            ev.release(),
            0,
            cookie);
        ActorSystem->Send(completionEvent.release());
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TWriteDeviceBlocksRequest MakeWriteDeviceBlocksRequest(
    const NProto::TMultiAgentWriteRequest& request,
    bool assignVolumeRequestId)
{
    NProto::TWriteDeviceBlocksRequest result;
    *result.MutableHeaders() = request.GetHeaders();
    result.SetBlockSize(request.BlockSize);

    for (const auto& deviceInfo: request.DevicesAndRanges) {
        auto* replicationTarget = result.AddReplicationTargets();
        replicationTarget->SetNodeId(deviceInfo.Device.GetNodeId());
        replicationTarget->SetDeviceUUID(deviceInfo.Device.GetDeviceUUID());
        replicationTarget->SetStartIndex(deviceInfo.DeviceBlockRange.Start);
        replicationTarget->SetTimeout(deviceInfo.RequestTimeout.MilliSeconds());
    }

    if (assignVolumeRequestId) {
        result.SetVolumeRequestId(request.GetHeaders().GetVolumeRequestId());
        result.SetMultideviceRequest(false);
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::HandleMultiAgentWrite(
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TWriteBlocksMethod>(
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

    for (const auto& buffer: msg->Record.GetBlocks().GetBuffers()) {
        if (buffer.size() % PartConfig->GetBlockSize() != 0) {
            replyError(
                E_ARGUMENT,
                TStringBuilder()
                    << "buffer not divisible by blockSize: " << buffer.size()
                    << " % " << PartConfig->GetBlockSize() << " != 0");
            return;
        }
    }

    TVector<TDeviceRequest> deviceRequests;
    bool ok = InitRequests<
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
        "MultiAgentWriteBlocks",
        true,
        *msg,
        ctx,
        *requestInfo,
        msg->Record.Range,
        &deviceRequests);

    if (!ok) {
        return;
    }

    if (deviceRequests.size() != 1) {
        replyError(
            E_ARGUMENT,
            "Can't execute MultiAgentWriteBlocks request cross device borders");
        return;
    }

    const auto& deviceRequest = deviceRequests[0];

    NProto::TWriteDeviceBlocksRequest writeDeviceBlocksRequest =
        MakeWriteDeviceBlocksRequest(
            msg->Record,
            AssignIdToWriteAndZeroRequestsEnabled);

    const auto requestId = RequestsInProgress.GenerateRequestId();
    auto requestResponseHandler =
        std::make_shared<TRdmaMultiWriteBlocksResponseHandler>(
            ctx.ActorSystem(),
            PartConfig,
            requestInfo,
            msg->Record.Range.Size(),
            msg->Record.DevicesAndRanges.size(),
            SelfId(),
            requestId);

    auto ep = AgentId2Endpoint[deviceRequest.Device.GetAgentId()];
    Y_ABORT_UNLESS(ep);

    auto [req, err] = ep->AllocateRequest(
        requestResponseHandler,
        std::make_unique<TDeviceRequestRdmaContext>(deviceRequest.DeviceIdx),
        NRdma::TProtoMessageSerializer::MessageByteSize(
            writeDeviceBlocksRequest,
            msg->Record.Range.Size() * msg->Record.BlockSize),
        4_KB);

    if (HasError(err)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "Failed to allocate rdma memory for WriteDeviceBlocksRequest, "
            " error: %s",
            FormatError(err).c_str());

        NotifyDeviceTimedOutIfNeeded(ctx, deviceRequest.Device.GetDeviceUUID());

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
                std::move(err)));

        return;
    }

    NRdma::TProtoMessageSerializer::SerializeWithData(
        req->RequestBuffer,
        TBlockStoreProtocol::WriteDeviceBlocksRequest,
        GetFlags(),
        writeDeviceBlocksRequest,
        GetSgList(msg->Record.GetBlocks()));

    const auto sentRequestId =
        ep->SendRequest(std::move(req), requestInfo->CallContext);

    RequestsInProgress.AddWriteRequest(
        requestId,
        TRequestContext{TDeviceRequestContext{
            .DeviceIndex = deviceRequest.DeviceIdx,
            .SentRequestId = sentRequestId}});
}

}   // namespace NCloud::NBlockStore::NStorage
