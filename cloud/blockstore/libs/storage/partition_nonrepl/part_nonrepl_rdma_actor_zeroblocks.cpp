#include "part_nonrepl_rdma_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
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

using TResponse = TEvService::TEvZeroBlocksResponse;

////////////////////////////////////////////////////////////////////////////////

class TRdmaRequestZeroBlocksHandler final
    : public TRdmaDeviceRequestHandlerBase<TRdmaRequestZeroBlocksHandler>
{
    using TBase =
        TRdmaDeviceRequestHandlerBase<TRdmaRequestZeroBlocksHandler>;

public:
    using TRequestContext = TDeviceRequestRdmaContext;
    using TResponseProto = NProto::TZeroDeviceBlocksResponse;

    using TBase::TBase;

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted>
    CreateCompletionEvent(const NProto::TError& error)
    {
        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted>(error);
        auto& counters = *completion->Stats.MutableUserWriteCounters();
        counters.SetBlocksCount(GetRequestBlockCount());
        return completion;
    }

    std::unique_ptr<IEventBase> CreateResponse(const NProto::TError& error)
    {
        return std::make_unique<TEvService::TEvZeroBlocksResponse>(error);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::HandleZeroBlocks(
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
    bool ok = InitRequests<TEvService::TZeroBlocksMethod>(
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests);

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestContext = std::make_shared<TRdmaRequestZeroBlocksHandler>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        requestId,
        SelfId(),
        msg->Record.GetBlocksCount(),
        deviceRequests.size());

    struct TDeviceRequestInfo
    {
        NRdma::IClientEndpointPtr Endpoint;
        NRdma::TClientRequestPtr ClientRequest;
    };

    TVector<TDeviceRequestInfo> requests;
    TRequestContext sentRequestCtx;

    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);

        sentRequestCtx.emplace_back(r.DeviceIdx);

        NProto::TZeroDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlocksCount(r.DeviceBlockRange.Size());
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());
        if (AssignIdToWriteAndZeroRequestsEnabled) {
            deviceRequest.SetVolumeRequestId(
                msg->Record.GetHeaders().GetVolumeRequestId());
        }

        auto context = std::make_unique<TDeviceRequestRdmaContext>();
        context->DeviceIdx = r.DeviceIdx;

        auto [req, err] = ep->AllocateRequest(
            requestContext,
            std::move(context),
            NRdma::TProtoMessageSerializer::MessageByteSize(deviceRequest, 0),
            4_KB);

        if (HasError(err)) {
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                "Failed to allocate rdma memory for ZeroDeviceBlocksRequest"
                ", error: %s",
                FormatError(err).c_str());

            NotifyDeviceTimedOutIfNeeded(ctx, r.Device.GetDeviceUUID());

            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TResponse>(std::move(err)));

            return;
        }

        NRdma::TProtoMessageSerializer::Serialize(
            req->RequestBuffer,
            TBlockStoreProtocol::ZeroDeviceBlocksRequest,
            0,   // flags
            deviceRequest);

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
