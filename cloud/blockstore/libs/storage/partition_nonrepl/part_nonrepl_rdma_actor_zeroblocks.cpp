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

class TRdmaRequestZeroBlocksHandler
    : public TRdmaDeviceRequestHandler<TRdmaRequestZeroBlocksHandler>
{
    using TBase = TRdmaDeviceRequestHandler<TRdmaRequestZeroBlocksHandler>;

public:
    TRdmaRequestZeroBlocksHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : TRdmaDeviceRequestHandler(
              actorSystem,
              std::move(partConfig),
              std::move(requestInfo),
              requestId,
              parentActorId,
              requestBlockCount,
              requestCount)
    {}

    NProto::TError ProcessSubResponse(
        const TDeviceRequestContext& dCtx,
        TStringBuf buffer)
    {
        Y_UNUSED(dCtx);
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);

        if (HasError(err)) {
            return err;
        }

        const auto& concreteProto =
            static_cast<NProto::TZeroDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            return err;
        }

        return {};
    }

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted>
    CreateCompletionEvent()
    {
        return TBase::CreateCompletionEvent<
            TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted>();
    }

    std::unique_ptr<TEvService::TEvZeroBlocksResponse> CreateResponse(
        NProto::TError err)
    {
        return std::make_unique<TEvService::TEvZeroBlocksResponse>(
            std::move(err));
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
        deviceRequests.size(),
        msg->Record.GetBlocksCount(),
        SelfId(),
        requestId);

    struct TDeviceRequestInfo
    {
        NRdma::IClientEndpointPtr Endpoint;
        NRdma::TClientRequestPtr ClientRequest;
    };

    TVector<TDeviceRequestInfo> requests;

    const bool assignVolumeRequestId =
        AssignIdToWriteAndZeroRequestsEnabled &&
        !msg->Record.GetHeaders().GetIsBackgroundRequest();

    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);

        NProto::TZeroDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlocksCount(r.DeviceBlockRange.Size());
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());
        if (assignVolumeRequestId) {
            deviceRequest.SetVolumeRequestId(
                msg->Record.GetHeaders().GetVolumeRequestId());
            deviceRequest.SetMultideviceRequest(deviceRequests.size() > 1);
        }

        auto context = std::make_unique<TDeviceRequestContext>();
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

    for (auto& request: requests) {
        request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
    }

    RequestsInProgress.AddWriteRequest(requestId);
}

}   // namespace NCloud::NBlockStore::NStorage
