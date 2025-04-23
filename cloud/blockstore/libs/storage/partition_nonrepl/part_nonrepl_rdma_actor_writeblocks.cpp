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

class TRdmaWriteBlocksResponseHandler: public NRdma::IClientHandler
{
private:
    TActorSystem* ActorSystem;
    TNonreplicatedPartitionConfigPtr PartConfig;
    TRequestInfoPtr RequestInfo;
    TAdaptiveLock Lock;
    size_t ResponseCount;
    const bool ReplyLocal;
    NProto::TError Error;
    const ui32 RequestBlockCount;
    const NActors::TActorId ParentActorId;
    const ui64 RequestId;

    // Indices of devices that participated in the request.
    TStackVec<ui32, 2> DeviceIndices;

    // Indices of devices where requests have resulted in errors.
    TStackVec<ui32, 2> ErrorDeviceIndices;

public:
    TRdmaWriteBlocksResponseHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            bool replyLocal,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : ActorSystem(actorSystem)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , ResponseCount(requestCount)
        , ReplyLocal(replyLocal)
        , RequestBlockCount(requestBlockCount)
        , ParentActorId(parentActorId)
        , RequestId(requestId)
    {}

    std::unique_ptr<IEventBase> MakeResponse() const
    {
        if (ReplyLocal) {
            return std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                Error);
        }

        return std::make_unique<TEvService::TEvWriteBlocksResponse>(Error);
    }

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
        }
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        TRequestScope timer(*RequestInfo);

        auto guard = Guard(Lock);

        auto buffer = req->ResponseBuffer.Head(responseBytes);
        auto* reqCtx = static_cast<TDeviceRequestContext*>(req->Context.get());

        DeviceIndices.emplace_back(reqCtx->DeviceIdx);

        if (status == NRdma::RDMA_PROTO_OK) {
            HandleResult(buffer);
        } else {
            Error = NRdma::ParseError(buffer);
            if (NeedToNotifyAboutDeviceRequestError(Error)) {
                ErrorDeviceIndices.emplace_back(reqCtx->DeviceIdx);
            }
        }

        if (--ResponseCount != 0) {
            return;
        }

        // Got all device responses. Do processing.

        ProcessError(*ActorSystem, *PartConfig, Error);

        auto response = MakeResponse();
        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            response.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());

        using TCompletionEvent =
            TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted;
        auto completion = std::make_unique<TCompletionEvent>(std::move(Error));
        auto& counters = *completion->Stats.MutableUserWriteCounters();
        completion->TotalCycles = RequestInfo->GetTotalCycles();
        completion->DeviceIndices = DeviceIndices;
        completion->ErrorDeviceIndices = ErrorDeviceIndices;

        timer.Finish();
        completion->ExecCycles = RequestInfo->GetExecCycles();

        counters.SetBlocksCount(RequestBlockCount);
        auto completionEvent = std::make_unique<IEventHandle>(
            ParentActorId,
            TActorId(),
            completion.release(),
            0,
            RequestId);
        ActorSystem->Send(completionEvent.release());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
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
        "WriteBlocks",
        requestInfo->CallContext->RequestId);

    auto replyError = [this] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            PartConfig->MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "WriteBlocks",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    for (const auto& buffer: msg->Record.GetBlocks().GetBuffers()) {
        if (buffer.size() % PartConfig->GetBlockSize() != 0) {
            replyError(
                ctx,
                *requestInfo,
                E_ARGUMENT,
                TStringBuilder() << "buffer not divisible by blockSize: "
                    << buffer.size() << " % " << PartConfig->GetBlockSize()
                    << " != 0");
            return;
        }
    }

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        CalculateWriteRequestBlockCount(msg->Record, PartConfig->GetBlockSize())
    );

    TVector<TDeviceRequest> deviceRequests;
    bool ok = InitRequests<TEvService::TWriteBlocksMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests
    );

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestResponseHandler =
        std::make_shared<TRdmaWriteBlocksResponseHandler>(
            ctx.ActorSystem(),
            PartConfig,
            requestInfo,
            deviceRequests.size(),
            false,
            blockRange.Size(),
            SelfId(),
            requestId);

    TDeviceRequestBuilder builder(
        deviceRequests,
        PartConfig->GetBlockSize(),
        msg->Record);

    TVector<TDeviceRequestInfo> requests;

    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);

        NProto::TWriteDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());
        if (AssignIdToWriteAndZeroRequestsEnabled) {
            deviceRequest.SetVolumeRequestId(
                msg->Record.GetHeaders().GetVolumeRequestId());
            deviceRequest.SetMultideviceRequest(deviceRequests.size() > 1);
        }

        auto context = std::make_unique<TDeviceRequestContext>();
        context->DeviceIdx = r.DeviceIdx;

        auto [req, err] = ep->AllocateRequest(
            requestResponseHandler,
            std::move(context),
            NRdma::TProtoMessageSerializer::MessageByteSize(
                deviceRequest,
                r.DeviceBlockRange.Size() * PartConfig->GetBlockSize()),
            4_KB);

        if (HasError(err)) {
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                "Failed to allocate rdma memory for WriteDeviceBlocksRequest, "
                " error: %s",
                FormatError(err).c_str());

            using TResponse = TEvService::TEvWriteBlocksResponse;
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TResponse>(std::move(err)));

            return;
        }

        TSgList sglist;
        builder.BuildNextRequest(&sglist);

        ui32 flags = 0;
        if (RdmaClient->IsAlignedDataEnabled()) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        NRdma::TProtoMessageSerializer::SerializeWithData(
            req->RequestBuffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            flags,
            deviceRequest,
            sglist);

        requests.push_back({std::move(ep), std::move(req)});
    }

    for (auto& request: requests) {
        request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
    }

    RequestsInProgress.AddWriteRequest(requestId);
}

void TNonreplicatedPartitionRdmaActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TWriteBlocksLocalMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "WriteBlocks",
        requestInfo->CallContext->RequestId);

    auto replyError = [this] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            PartConfig->MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "WriteBlocks",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    auto guard = msg->Record.Sglist.Acquire();

    if (!guard) {
        replyError(
            ctx,
            *requestInfo,
            E_CANCELLED,
            "failed to acquire sglist in NonreplicatedPartitionRdmaActor");
        return;
    }

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.BlocksCount);

    TVector<TDeviceRequest> deviceRequests;
    bool ok = InitRequests<TEvService::TWriteBlocksLocalMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests
    );

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestResponseHandler =
        std::make_shared<TRdmaWriteBlocksResponseHandler>(
            ctx.ActorSystem(),
            PartConfig,
            requestInfo,
            deviceRequests.size(),
            true,
            blockRange.Size(),
            SelfId(),
            requestId);

    const auto& sglist = guard.Get();

    TVector<TDeviceRequestInfo> requests;

    ui64 blocks = 0;
    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);

        NProto::TWriteDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());
        if (AssignIdToWriteAndZeroRequestsEnabled) {
            deviceRequest.SetVolumeRequestId(
                msg->Record.GetHeaders().GetVolumeRequestId());
            deviceRequest.SetMultideviceRequest(deviceRequests.size() > 1);
        }
        auto context = std::make_unique<TDeviceRequestContext>();
        context->DeviceIdx = r.DeviceIdx;

        auto [req, err] = ep->AllocateRequest(
            requestResponseHandler,
            std::move(context),
            NRdma::TProtoMessageSerializer::MessageByteSize(
                deviceRequest,
                r.DeviceBlockRange.Size() * PartConfig->GetBlockSize()),
            4_KB);

        if (HasError(err)) {
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                "Failed to allocate rdma memory for WriteDeviceBlocksRequest"
                ", error: %s",
                FormatError(err).c_str());

            using TResponse = TEvService::TEvWriteBlocksLocalResponse;
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TResponse>(std::move(err)));

            return;
        }

        ui32 flags = 0;
        if (RdmaClient->IsAlignedDataEnabled()) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
        }

        NRdma::TProtoMessageSerializer::SerializeWithData(
            req->RequestBuffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            flags,
            deviceRequest,
            TBlockDataRefSpan(
                sglist.begin() + blocks,
                r.DeviceBlockRange.Size()));

        blocks += r.DeviceBlockRange.Size();

        requests.push_back({std::move(ep), std::move(req)});
    }

    for (auto& request: requests) {
        request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
    }

    RequestsInProgress.AddWriteRequest(requestId);
}

}   // namespace NCloud::NBlockStore::NStorage
