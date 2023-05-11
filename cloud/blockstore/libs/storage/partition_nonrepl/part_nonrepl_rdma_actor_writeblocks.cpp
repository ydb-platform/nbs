#include "part_nonrepl_rdma_actor.h"
#include "part_nonrepl_common.h"

#include "part_nonrepl_util.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/rdma/protobuf.h>
#include <cloud/blockstore/libs/rdma/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceRequestInfo
{
    NRdma::IClientEndpointPtr Endpoint;
    NRdma::TClientRequestPtr ClientRequest;
    std::unique_ptr<TRdmaContext> DeviceRequestContext;
};

////////////////////////////////////////////////////////////////////////////////

struct TRdmaRequestContext: NRdma::IClientHandler
{
    TActorSystem* ActorSystem;
    TNonreplicatedPartitionConfigPtr PartConfig;
    TRequestInfoPtr RequestInfo;
    TAtomic Responses;
    bool ReplyLocal;
    NProto::TError Error;
    const ui32 RequestBlockCount;
    NActors::TActorId ParentActorId;
    ui64 RequestId;

    TRdmaRequestContext(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            TAtomicBase requests,
            bool replyLocal,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : ActorSystem(actorSystem)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , Responses(requests)
        , ReplyLocal(replyLocal)
        , RequestBlockCount(requestBlockCount)
        , ParentActorId(parentActorId)
        , RequestId(requestId)
    {}

    IEventBasePtr MakeResponse() const
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
        }

        const auto& concreteProto =
            static_cast<NProto::TWriteDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            Error = concreteProto.GetError();
            return;
        }
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        auto buffer = req->ResponseBuffer.Head(responseBytes);

        if (status == NRdma::RDMA_PROTO_OK) {
            HandleResult(buffer);
        } else {
            HandleError(PartConfig, buffer, Error);
        }

        auto* dr = static_cast<TRdmaContext*>(req->Context);
        dr->Endpoint->FreeRequest(std::move(req));

        delete dr;

        if (AtomicDecrement(Responses) == 0) {
            ProcessError(*ActorSystem, *PartConfig, Error);

            auto response = MakeResponse();
            TAutoPtr<IEventHandle> event(
                new IEventHandle(
                    RequestInfo->Sender,
                    {},
                    response.get(),
                    0,
                    RequestInfo->Cookie));
            response.release();
            ActorSystem->Send(event);

            using TCompletionEvent =
                TEvNonreplPartitionPrivate::TEvWriteBlocksCompleted;
            auto completion =
                std::make_unique<TCompletionEvent>(std::move(Error));
            auto& counters = *completion->Stats.MutableUserWriteCounters();
            completion->TotalCycles = RequestInfo->GetTotalCycles();

            counters.SetBlocksCount(RequestBlockCount);
            TAutoPtr<IEventHandle> completionEvent(
                new IEventHandle(
                    ParentActorId,
                    {},
                    completion.get(),
                    0,
                    RequestId));

            completion.release();
            ActorSystem->Send(completionEvent);

            delete this;
        }
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

    auto replyError = [=] (
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
        if (buffer.Size() % PartConfig->GetBlockSize() != 0) {
            replyError(
                ctx,
                *requestInfo,
                E_ARGUMENT,
                TStringBuilder() << "buffer not divisible by blockSize: "
                    << buffer.Size() << " % " << PartConfig->GetBlockSize()
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
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests
    );

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestContext = std::make_unique<TRdmaRequestContext>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        deviceRequests.size(),
        false,
        blockRange.Size(),
        SelfId(),
        requestId);

    auto* serializer = TBlockStoreProtocol::Serializer();

    TDeviceRequestBuilder builder(
        deviceRequests,
        PartConfig->GetBlockSize(),
        msg->Record);

    TVector<TDeviceRequestInfo> requests;

    for (auto& r: deviceRequests) {
        auto& ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_VERIFY(ep);
        auto dr = std::make_unique<TRdmaContext>();
        dr->Endpoint = ep;
        dr->RequestHandler = &*requestContext;

        NProto::TWriteDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());
        // TODO: remove after NBS-3886
        deviceRequest.SetSessionId(msg->Record.GetHeaders().GetClientId());

        auto [req, err] = ep->AllocateRequest(
            &*dr,
            serializer->MessageByteSize(
                deviceRequest,
                r.DeviceBlockRange.Size() * PartConfig->GetBlockSize()),
            4_KB);

        if (HasError(err)) {
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                "Failed to allocate rdma memory for WriteDeviceBlocksRequest, "
                " error: %s",
                FormatError(err).c_str());

            for (auto& request: requests) {
                request.Endpoint->FreeRequest(std::move(request.ClientRequest));
            }

            using TResponse = TEvService::TEvWriteBlocksResponse;
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TResponse>(std::move(err)));

            return;
        }

        TVector<IOutputStream::TPart> parts;
        builder.BuildNextRequest(&parts);

        serializer->Serialize(
            req->RequestBuffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            deviceRequest,
            TContIOVector(parts.data(), parts.size()));

        requests.push_back({ep, std::move(req), std::move(dr)});
    }

    for (auto& request: requests) {
        request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
        request.DeviceRequestContext.release();
    }

    RequestsInProgress.AddWriteRequest(requestId);
    requestContext.release();
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

    auto replyError = [=] (
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
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests
    );

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestContext = std::make_unique<TRdmaRequestContext>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        deviceRequests.size(),
        true,
        blockRange.Size(),
        SelfId(),
        requestId);

    auto* serializer = TBlockStoreProtocol::Serializer();
    const auto& sglist = guard.Get();

    TVector<TDeviceRequestInfo> requests;

    ui64 blocks = 0;
    for (auto& r: deviceRequests) {
        auto& ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_VERIFY(ep);
        auto dr = std::make_unique<TRdmaContext>();
        dr->Endpoint = ep;
        dr->RequestHandler = &*requestContext;

        NProto::TWriteDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());
        // TODO: remove after NBS-3886
        deviceRequest.SetSessionId(msg->Record.GetHeaders().GetClientId());

        auto [req, err] = ep->AllocateRequest(
            &*dr,
            serializer->MessageByteSize(
                deviceRequest,
                r.DeviceBlockRange.Size() * PartConfig->GetBlockSize()),
            4_KB);

        if (HasError(err)) {
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                "Failed to allocate rdma memory for WriteDeviceBlocksRequest"
                ", error: %s",
                FormatError(err).c_str());

            for (auto& request: requests) {
                request.Endpoint->FreeRequest(std::move(request.ClientRequest));
            }

            using TResponse = TEvService::TEvWriteBlocksLocalResponse;
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TResponse>(std::move(err)));

            return;
        }

        serializer->Serialize(
            req->RequestBuffer,
            TBlockStoreProtocol::WriteDeviceBlocksRequest,
            deviceRequest,
            // XXX (cast)
            TContIOVector(
                const_cast<IOutputStream::TPart*>(
                    reinterpret_cast<const IOutputStream::TPart*>(
                        sglist.begin() + blocks
                )),
                r.DeviceBlockRange.Size()
            ));

        blocks += r.DeviceBlockRange.Size();

        requests.push_back({ep, std::move(req), std::move(dr)});
    }

    for (auto& request: requests) {
        request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
        request.DeviceRequestContext.release();
    }

    RequestsInProgress.AddWriteRequest(requestId);
    requestContext.release();
}

}   // namespace NCloud::NBlockStore::NStorage
