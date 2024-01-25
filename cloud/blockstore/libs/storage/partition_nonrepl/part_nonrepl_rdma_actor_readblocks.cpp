#include "part_nonrepl_rdma_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

using TResponse = TEvService::TEvReadBlocksResponse;

////////////////////////////////////////////////////////////////////////////////

class TRdmaRequestContext: public NRdma::IClientHandler
{
private:
    TActorSystem* ActorSystem;
    TNonreplicatedPartitionConfigPtr PartConfig;
    TRequestInfoPtr RequestInfo;
    TAdaptiveLock Lock;
    size_t ResponseCount;
    NProto::TReadBlocksResponse Response;
    NActors::TActorId ParentActorId;
    ui64 RequestId;

public:
    TRdmaRequestContext(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            ui32 blockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : ActorSystem(actorSystem)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , ResponseCount(requestCount)
        , ParentActorId(parentActorId)
        , RequestId(requestId)
    {
        TRequestScope timer(*RequestInfo);

        auto& buffers = *Response.MutableBlocks()->MutableBuffers();
        buffers.Reserve(blockCount);
        for (ui32 i = 0; i < blockCount; ++i) {
            buffers.Add()->resize(PartConfig->GetBlockSize(), 0);
        }
    }

    void HandleResult(const TDeviceReadRequestContext& dr, TStringBuf buffer)
    {
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);

        if (HasError(err)) {
            *Response.MutableError() = std::move(err);
            return;
        }

        const auto& concreteProto =
            static_cast<NProto::TReadDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            *Response.MutableError() = concreteProto.GetError();
            return;
        }

        auto& blocks = *Response.MutableBlocks()->MutableBuffers();

        ui64 offset = 0;
        ui64 b = 0;
        while (offset < result.Data.Size()) {
            ui64 targetBlock = dr.StartIndexOffset + b;
            Y_ABORT_UNLESS(targetBlock < static_cast<ui64>(blocks.size()));
            ui64 bytes = Min(
                result.Data.Size() - offset,
                blocks[targetBlock].Size());
            Y_ABORT_UNLESS(bytes);

            memcpy(
                const_cast<char*>(blocks[targetBlock].Data()),
                result.Data.data() + offset,
                bytes);
            offset += bytes;
            ++b;
        }
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        TRequestScope timer(*RequestInfo);

        auto guard = Guard(Lock);

        auto* dr = static_cast<TDeviceReadRequestContext*>(req->Context.get());
        auto buffer = req->ResponseBuffer.Head(responseBytes);

        if (status == NRdma::RDMA_PROTO_OK) {
            HandleResult(*dr, std::move(buffer));
        } else {
            HandleError(
                PartConfig,
                std::move(buffer),
                *Response.MutableError());
        }

        if (--ResponseCount != 0) {
            return;
        }

        // Got all device responses. Do processing.

        ProcessError(*ActorSystem, *PartConfig, *Response.MutableError());
        auto error = Response.GetError();

        ui32 blocks = Response.GetBlocks().BuffersSize();

        auto response = std::make_unique<TResponse>();
        response->Record = std::move(Response);
        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            response.release(),
            0,
            RequestInfo->Cookie);

        ActorSystem->Send(event.release());

        using TCompletionEvent =
            TEvNonreplPartitionPrivate::TEvReadBlocksCompleted;
        auto completion = std::make_unique<TCompletionEvent>(std::move(error));
        auto& counters = *completion->Stats.MutableUserReadCounters();
        completion->TotalCycles = RequestInfo->GetTotalCycles();

        timer.Finish();
        completion->ExecCycles = RequestInfo->GetExecCycles();

        counters.SetBlocksCount(blocks);
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

void TNonreplicatedPartitionRdmaActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TReadBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    bool ok = InitRequests<TEvService::TReadBlocksMethod>(
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests
    );

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestContext = std::make_shared<TRdmaRequestContext>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        deviceRequests.size(),
        blockRange.Size(),
        SelfId(),
        requestId);

    auto error = SendReadRequests(
        ctx,
        requestInfo->CallContext,
        msg->Record.GetHeaders(),
        std::move(requestContext),
        deviceRequests);

    if (HasError(error)) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TResponse>(std::move(error)));

        return;
    }

    RequestsInProgress.AddReadRequest(requestId);
}

}   // namespace NCloud::NBlockStore::NStorage
