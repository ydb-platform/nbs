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
    const ui32 RequestStartIndex;
    const ui32 RequestBlockCount;
    const NActors::TActorId ParentActorId;
    const ui64 RequestId;
    const bool CheckVoidBlocks;
    const bool ShouldReportBlockRangeOnFailure;

    TAdaptiveLock Lock;
    size_t ResponseCount;
    TGuardedSgList SgList;
    NProto::TError Error;

    ui32 VoidBlockCount = 0;

    // Indices of devices that participated in the request.
    TStackVec<ui32, 2> DeviceIndices;

    // Indices of devices where requests have resulted in errors.
    TStackVec<ui32, 2> ErrorDeviceIndices;

public:
    TRdmaRequestReadBlocksLocalContext(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            TGuardedSgList sglist,
            ui32 requestStartIndex,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId,
            bool checkVoidBlocks,
            bool shouldReportBlockRangeOnFailure)
        : ActorSystem(actorSystem)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , RequestStartIndex(requestStartIndex)
        , RequestBlockCount(requestBlockCount)
        , ParentActorId(parentActorId)
        , RequestId(requestId)
        , CheckVoidBlocks(checkVoidBlocks)
        , ShouldReportBlockRangeOnFailure(shouldReportBlockRangeOnFailure)
        , ResponseCount(requestCount)
        , SgList(std::move(sglist))
    {}

    void HandleResult(
        const TDeviceReadRequestContext& reqCtx,
        TStringBuf buffer)
    {
        auto guard = SgList.Acquire();
        if (!guard) {
            Error = MakeError(E_CANCELLED, "can't acquire sglist");
            return;
        }
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

        ui64 offset = 0;
        ui64 b = 0;
        bool isAllZeroes = CheckVoidBlocks;
        while (offset < result.Data.size()) {
            ui64 targetBlock = reqCtx.StartIndexOffset + b;
            Y_ABORT_UNLESS(targetBlock < data.size());
            ui64 bytes =
                Min(result.Data.size() - offset, data[targetBlock].Size());
            Y_ABORT_UNLESS(bytes);

            char* dst = const_cast<char*>(data[targetBlock].Data());
            const char* src = result.Data.data() + offset;

            if (isAllZeroes) {
                isAllZeroes = IsAllZeroes(src, bytes);
            }

            // may be nullptr for overlay disks
            if (dst) {
                memcpy(dst, src, bytes);
            }

            offset += bytes;
            ++b;
        }

        if (isAllZeroes) {
            VoidBlockCount += reqCtx.BlockCount;
        }
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        TRequestScope timer(*RequestInfo);

        auto guard = Guard(Lock);

        auto* reqCtx =
            static_cast<TDeviceReadRequestContext*>(req->Context.get());
        auto buffer = req->ResponseBuffer.Head(responseBytes);

        DeviceIndices.emplace_back(reqCtx->DeviceIdx);

        if (status == NRdma::RDMA_PROTO_OK) {
            HandleResult(*reqCtx, buffer);
        } else {
            Error = NRdma::ParseError(buffer);
            ConvertRdmaErrorIfNeeded(status, Error);
            if (NeedToNotifyAboutDeviceRequestError(Error)) {
                ErrorDeviceIndices.emplace_back(reqCtx->DeviceIdx);
            }
        }

        if (--ResponseCount != 0) {
            return;
        }

        // Got all device responses. Do processing.

        ProcessError(*ActorSystem, *PartConfig, Error);

        const bool allZeroes = VoidBlockCount == RequestBlockCount;
        auto response =
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(Error);
        response->Record.SetAllZeroes(allZeroes);
        if (ShouldReportBlockRangeOnFailure) {
            const auto blockRange =
                TBlockRange64::WithLength(RequestStartIndex, RequestBlockCount);
            response->Record.FailInfo.FailedRanges.push_back(
                DescribeRange(blockRange).c_str());
        }
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
        completion->DeviceIndices = DeviceIndices;
        completion->ErrorDeviceIndices = ErrorDeviceIndices;

        timer.Finish();
        completion->ExecCycles = RequestInfo->GetExecCycles();
        completion->NonVoidBlockCount = allZeroes ? 0 : RequestBlockCount;
        completion->VoidBlockCount = allZeroes ? RequestBlockCount : 0;

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
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        SelfId(),
        requestId,
        Config->GetOptimizeVoidBuffersTransferForReadsEnabled(),
        msg->Record.ShouldReportFailedRangesOnFailure);

    auto [sentRequestCtx, error] = SendReadRequests(
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

    RequestsInProgress.AddReadRequest(requestId, sentRequestCtx);
}

}   // namespace NCloud::NBlockStore::NStorage
