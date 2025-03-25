#include "part_nonrepl_rdma_actor.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRdmaRequestReadBlocksContext: public IRdmaDeviceRequestHandler
{
private:
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TRequestInfoPtr RequestInfo;
    const ui64 RequestId;
    const bool CheckVoidBlocks;

    TAdaptiveLock Lock;
    NProto::TReadBlocksResponse Response;

    ui32 VoidBlockCount = 0;

public:
    TRdmaRequestReadBlocksContext(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            ui32 blockCount,
            NActors::TActorId parentActorId,
            ui64 requestId,
            bool checkVoidBlocks)
        : IRdmaDeviceRequestHandler(requestCount, actorSystem, parentActorId, 0)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , RequestId(requestId)
        , CheckVoidBlocks(checkVoidBlocks)
    {
        TRequestScope timer(*RequestInfo);

        auto& buffers = *Response.MutableBlocks()->MutableBuffers();
        buffers.Reserve(blockCount);
        for (ui32 i = 0; i < blockCount; ++i) {
            buffers.Add()->resize(PartConfig->GetBlockSize(), 0);
        }
    }

    void ProcessResponseProto(
        const TDeviceRequestContext& dCtx,
        TStringBuf buffer) override
    {
        const auto& dr = static_cast<const TDeviceReadRequestContext&>(dCtx);
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);

        if (HasError(err)) {
            Error = std::move(err);
            return;
        }

        const auto& concreteProto =
            static_cast<NProto::TReadDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            Error = concreteProto.GetError();
            return;
        }

        auto& blocks = *Response.MutableBlocks()->MutableBuffers();

        ui64 offset = 0;
        ui64 b = 0;
        bool isAllZeroes = CheckVoidBlocks;
        while (offset < result.Data.size()) {
            ui64 targetBlock = dr.StartIndexOffset + b;
            Y_ABORT_UNLESS(targetBlock < static_cast<ui64>(blocks.size()));
            ui64 bytes =
                Min(result.Data.size() - offset, blocks[targetBlock].size());
            Y_ABORT_UNLESS(bytes);

            char* dst = const_cast<char*>(blocks[targetBlock].data());
            const char* src = result.Data.data() + offset;

            if (isAllZeroes) {
                isAllZeroes = IsAllZeroes(src, bytes);
            }
            memcpy(dst, src, bytes);

            offset += bytes;
            ++b;
        }

        if (isAllZeroes) {
            VoidBlockCount += dr.BlockCount;
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

        if (!ProcessResponse(*dr, status, buffer)) {
            return;
        }

        // Got all device responses. Do processing.

        ProcessError(*ActorSystem, *PartConfig, Error);
        *Response.MutableError() = Error;
        auto error = Response.GetError();

        const ui32 blockCount = Response.GetBlocks().BuffersSize();
        const bool allZeroes = VoidBlockCount == blockCount;

        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
        response->Record = std::move(Response);
        response->Record.SetAllZeroes(allZeroes);
        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            response.release(),
            0,
            RequestInfo->Cookie);

        ActorSystem->Send(event.release());

        timer.Finish();

        using TCompletionEvent =
            TEvNonreplPartitionPrivate::TEvReadBlocksCompleted;
        auto completion = CreateCompletionEvent<TCompletionEvent>(
            std::move(Error),
            *RequestInfo);

        completion->NonVoidBlockCount = allZeroes ? 0 : blockCount;
        completion->VoidBlockCount = allZeroes ? blockCount : 0;
        auto& counters = *completion->Stats.MutableUserReadCounters();
        counters.SetBlocksCount(blockCount);

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
        *msg,
        ctx,
        *requestInfo,
        blockRange,
        &deviceRequests);

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestContext = std::make_shared<TRdmaRequestReadBlocksContext>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        deviceRequests.size(),
        blockRange.Size(),
        SelfId(),
        requestId,
        Config->GetOptimizeVoidBuffersTransferForReadsEnabled());

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
            std::make_unique<TEvService::TEvReadBlocksResponse>(
                std::move(error)));

        return;
    }

    RequestsInProgress.AddReadRequest(requestId);
}

}   // namespace NCloud::NBlockStore::NStorage
