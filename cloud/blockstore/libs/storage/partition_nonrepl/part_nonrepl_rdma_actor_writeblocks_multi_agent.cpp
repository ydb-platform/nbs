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

class TRdmaMultiWriteBlocksResponseHandler
    : public TRdmaDeviceRequestHandlerBase<TRdmaMultiWriteBlocksResponseHandler>
{
    using TBase =
        TRdmaDeviceRequestHandlerBase<TRdmaMultiWriteBlocksResponseHandler>;

private:
    const size_t ReplicationTargetCount = 0;

    bool InconsistentResponse = false;

public:
    using TRequestContext = TDeviceRequestRdmaContext;
    using TResponseProto = NProto::TWriteDeviceBlocksResponse;

    TRdmaMultiWriteBlocksResponseHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            ui32 requestBlockCount,
            size_t replicationTargetCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : TBase(
              actorSystem,
              std::move(partConfig),
              std::move(requestInfo),
              requestId,
              parentActorId,
              requestBlockCount,
              1)
        , ReplicationTargetCount(replicationTargetCount)
    {}

    NProto::TError ProcessSubResponseProto(
        const TRequestContext& ctx,
        TResponseProto& proto,
        TStringBuf data)
    {
        Y_UNUSED(ctx, data);

        bool subResponsesOk =
            proto.GetReplicationResponses().size() ==
                static_cast<int>(ReplicationTargetCount) &&
            AllOf(
                proto.GetReplicationResponses(),
                [](const NProto::TError& subResponseError)
                { return subResponseError.GetCode() == S_OK; });
        if (!subResponsesOk) {
            TString subResponses = Accumulate(
                proto.GetReplicationResponses(),
                TString{},
                [](const TString& acc, const NProto::TError& err)
                {
                    return acc ? acc + "," + FormatError(err)
                               : FormatError(err);
                });
            InconsistentResponse = true;
            return MakeError(E_REJECTED, "Responses: [" + subResponses + "]");
        }

        return {};
    }

    std::unique_ptr<NActors::IEventBase> CreateResponse(
        NProto::TError error)
    {
        auto response = std::make_unique<
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse>(
            std::move(error));
        response->Record.InconsistentResponse = InconsistentResponse;
        return response;
    }

    std::unique_ptr<NActors::IEventBase> CreateCompletionEvent()
    {
        auto completion = CreateConcreteCompletionEvent<
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteBlocksCompleted>();
        auto& counters = *completion->Stats.MutableUserWriteCounters();
        counters.SetBlocksCount(GetRequestBlockCount());

        return completion;
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
        // TMultiAgentWriteActor perform TEvMultiAgentWriteRequest only if all
        // TEvGetDeviceForRangeRequests to replicas have returned success. These
        // requests are response with an error if the request hits two disk-agents.
        ReportMultiAgentRequestAffectsTwoDevices(
            TStringBuilder() << "disk id: " << PartConfig->GetName().Quote()
                             << " range: " << msg->Record.Range.Print());
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
