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

class TRdmaRequestContext: public NRdma::IClientHandler
{
private:
    TActorSystem* ActorSystem;
    TNonreplicatedPartitionConfigPtr PartConfig;
    TRequestInfoPtr RequestInfo;
    TAdaptiveLock Lock;
    size_t ResponseCount;
    NProto::TError Error;
    const ui32 RequestBlockCount;
    NActors::TActorId ParentActorId;
    ui64 RequestId;

public:
    TRdmaRequestContext(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : ActorSystem(actorSystem)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , ResponseCount(requestCount)
        , RequestBlockCount(requestBlockCount)
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
            static_cast<NProto::TZeroDeviceBlocksResponse&>(*result.Proto);
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

        if (status == NRdma::RDMA_PROTO_OK) {
            HandleResult(buffer);
        } else {
            Error = NRdma::ParseError(buffer);
        }

        if (--ResponseCount != 0) {
            return;
        }

        // Got all device responses. Do processing.

        ProcessError(*ActorSystem, *PartConfig, Error);

        auto response = std::make_unique<TResponse>(Error);
        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            response.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());

        using TCompletionEvent =
            TEvNonreplPartitionPrivate::TEvZeroBlocksCompleted;
        auto completion = std::make_unique<TCompletionEvent>(std::move(Error));
        auto& counters = *completion->Stats.MutableUserWriteCounters();
        completion->TotalCycles = RequestInfo->GetTotalCycles();

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

    auto requestContext = std::make_shared<TRdmaRequestContext>(
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

    const bool assignVolumeRequestId = AssignIdToWriteAndZeroRequestsEnabled;

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

        auto [req, err] = ep->AllocateRequest(
            requestContext,
            nullptr,
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
