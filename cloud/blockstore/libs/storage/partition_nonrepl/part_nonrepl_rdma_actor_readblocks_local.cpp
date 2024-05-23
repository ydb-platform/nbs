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
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRdmaRequestReadBlocksLocalContext: public NRdma::IClientHandler
{
private:
    TActorSystem* ActorSystem;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TRequestInfoPtr RequestInfo;
    const ui32 RequestBlockCount;
    const NActors::TActorId ParentActorId;
    const ui64 RequestId;
    const bool SkipVoidBlocksToOptimizeNetworkTransfer;

    TAdaptiveLock Lock;
    size_t ResponseCount;
    TGuardedSgList SgList;
    NProto::TError Error;

    ui32 VoidBlockCount = 0;
    ui32 NonVoidBlockCount = 0;

public:
    TRdmaRequestReadBlocksLocalContext(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            TGuardedSgList sglist,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId,
            bool skipVoidBlocksToOptimizeNetworkTransfer)
        : ActorSystem(actorSystem)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , RequestBlockCount(requestBlockCount)
        , ParentActorId(parentActorId)
        , RequestId(requestId)
        , SkipVoidBlocksToOptimizeNetworkTransfer(
              skipVoidBlocksToOptimizeNetworkTransfer)
        , ResponseCount(requestCount)
        , SgList(std::move(sglist))
    {
    }

    void HandleResult(const TDeviceReadRequestContext& dr, TStringBuf buffer)
    {
        if (auto guard = SgList.Acquire()) {
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

            TSgList data = guard.Get();

            if (concreteProto.GetAllZeroes()) {
                // Fill in with zeros all blocks corresponding to the current
                // response.
                for (size_t i = 0; i < dr.BlockCount; ++i) {
                    size_t targetBlock = dr.StartIndexOffset + i;
                    char* targetBlockData =
                        const_cast<char*>(data[targetBlock].Data());
                    memset(targetBlockData, 0, data[targetBlock].Size());
                }
                VoidBlockCount += dr.BlockCount;
                STORAGE_CHECK_PRECONDITION(
                    SkipVoidBlocksToOptimizeNetworkTransfer);
                return;
            }

            ui64 offset = 0;
            ui64 b = 0;
            while (offset < result.Data.Size()) {
                ui64 targetBlock = dr.StartIndexOffset + b;
                Y_ABORT_UNLESS(targetBlock < data.size());
                ui64 bytes =
                    Min(result.Data.Size() - offset, data[targetBlock].Size());
                Y_ABORT_UNLESS(bytes);

                memcpy(
                    const_cast<char*>(data[targetBlock].Data()),
                    result.Data.data() + offset,
                    bytes);
                offset += bytes;
                ++b;
            }
            if (SkipVoidBlocksToOptimizeNetworkTransfer) {
                NonVoidBlockCount += dr.BlockCount;
            }
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
            HandleResult(*dr, buffer);
        } else {
            HandleError(PartConfig, buffer, Error);
        }

        if (--ResponseCount != 0) {
            return;
        }

        // Got all device responses. Do processing.

        ProcessError(*ActorSystem, *PartConfig, Error);

        auto response =
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(Error);
        response->Record.SetAllZeroes(VoidBlockCount == RequestBlockCount);
        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            response.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());

        using TCompletionEvent =
            TEvNonreplPartitionPrivate::TEvReadBlocksCompleted;
        auto completion = std::make_unique<TCompletionEvent>(std::move(Error));
        auto& counters = *completion->Stats.MutableUserReadCounters();
        completion->TotalCycles = RequestInfo->GetTotalCycles();

        timer.Finish();
        completion->ExecCycles = RequestInfo->GetExecCycles();
        completion->NonVoidBlockCount = NonVoidBlockCount;
        completion->VoidBlockCount = VoidBlockCount;

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

void TNonreplicatedPartitionRdmaActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TReadBlocksLocalMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    bool ok = InitRequests<TEvService::TReadBlocksLocalMethod>(
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

    auto requestContext = std::make_shared<TRdmaRequestReadBlocksLocalContext>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        deviceRequests.size(),
        std::move(msg->Record.Sglist),
        msg->Record.GetBlocksCount(),
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
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
                std::move(error)));

        return;
    }

    RequestsInProgress.AddReadRequest(requestId);
}

}   // namespace NCloud::NBlockStore::NStorage
