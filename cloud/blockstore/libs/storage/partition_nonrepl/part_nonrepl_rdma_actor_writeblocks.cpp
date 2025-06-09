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

class TRdmaWriteBlocksResponseHandler final
    : public TRdmaDeviceRequestHandlerBase<TRdmaWriteBlocksResponseHandler>
{
    using TBase =
        TRdmaDeviceRequestHandlerBase<TRdmaWriteBlocksResponseHandler>;

private:
    bool ReplyLocal;

public:
    using TRequestContext = TDeviceRequestRdmaContext;
    using TResponseProto = NProto::TWriteDeviceBlocksResponse;

    TRdmaWriteBlocksResponseHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            bool replyLocal,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : TBase(
              actorSystem,
              std::move(partConfig),
              std::move(requestInfo),
              requestId,
              parentActorId,
              requestBlockCount,
              requestCount)
        , ReplyLocal(replyLocal)
    {}

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted>
    CreateCompletionEvent(const NProto::TError& error)
    {
        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted>(error);
        auto& counters = *completion->Stats.MutableUserWriteCounters();
        counters.SetBlocksCount(GetRequestBlockCount());
        return completion;
    }

    std::unique_ptr<IEventBase> CreateResponse(
        const NProto::TError& error) const
    {
        if (ReplyLocal) {
            return std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                error);
        }

        return std::make_unique<TEvService::TEvWriteBlocksResponse>(error);
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

    TRequestContext sentRequestCtx;

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

        auto context = std::make_unique<TDeviceRequestRdmaContext>();
        context->DeviceIdx = r.DeviceIdx;

        sentRequestCtx.emplace_back(r.DeviceIdx);

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

            NotifyDeviceTimedOutIfNeeded(ctx, r.Device.GetDeviceUUID());

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

    for (size_t i = 0; i < requests.size(); ++i) {
        auto& request = requests[i];
        sentRequestCtx[i].SentRequestId = request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
    }

    RequestsInProgress.AddWriteRequest(requestId, sentRequestCtx);
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
    TRequestContext sentRequestCtx;

    ui64 blocks = 0;
    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);

        sentRequestCtx.emplace_back(r.DeviceIdx);

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
        auto context = std::make_unique<TDeviceRequestRdmaContext>();
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

            NotifyDeviceTimedOutIfNeeded(ctx, r.Device.GetDeviceUUID());

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

    for (size_t i = 0; i < requests.size(); ++i) {
        auto& request = requests[i];
        sentRequestCtx[i].SentRequestId = request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
    }

    RequestsInProgress.AddWriteRequest(requestId, sentRequestCtx);
}

}   // namespace NCloud::NBlockStore::NStorage
