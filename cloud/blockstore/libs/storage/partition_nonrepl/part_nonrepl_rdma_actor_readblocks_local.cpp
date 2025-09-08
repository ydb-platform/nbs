#include "part_nonrepl_rdma_actor.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
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

class TRdmaRequestReadBlocksLocalHandler final
    : public TRdmaDeviceRequestHandlerBase<TRdmaRequestReadBlocksLocalHandler>
{
    using TBase =
        TRdmaDeviceRequestHandlerBase<TRdmaRequestReadBlocksLocalHandler>;

private:
    const TBlockRange64 BlockRange;
    const bool ShouldReportBlockRangeOnFailure;
    const bool CheckVoidBlocks;
    TGuardedSgList SgList;

    TVector<NProto::TChecksum> Checksums;
    ui32 VoidBlockCount = 0;

public:
    using TRequestContext = TDeviceReadRequestContext;
    using TResponseProto = NProto::TReadDeviceBlocksResponse;

    TRdmaRequestReadBlocksLocalHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            TGuardedSgList sglist,
            TBlockRange64 blockRange,
            NActors::TActorId parentActorId,
            ui64 requestId,
            bool checkVoidBlocks,
            bool shouldReportBlockRangeOnFailure)
        : TBase(
              actorSystem,
              std::move(partConfig),
              std::move(requestInfo),
              requestId,
              parentActorId,
              blockRange.Size(),
              requestCount)
        , BlockRange(blockRange)
        , ShouldReportBlockRangeOnFailure(shouldReportBlockRangeOnFailure)
        , CheckVoidBlocks(checkVoidBlocks)
        , SgList(std::move(sglist))
    {
        Checksums.resize(requestCount);
    }

    NProto::TError ProcessSubResponseProto(
        const TRequestContext& ctx,
        TResponseProto& proto,
        TStringBuf responseData)
    {
        auto guard = SgList.Acquire();
        if (!guard) {
            return MakeError(E_CANCELLED, "can't acquire sglist");
        }

        Y_ABORT_UNLESS(ctx.RequestIndex < Checksums.size());
        Checksums[ctx.RequestIndex] = std::move(*proto.MutableChecksum());

        const TSgList& data = guard.Get();

        ui64 offset = 0;
        ui64 b = 0;
        bool isAllZeroes = CheckVoidBlocks;
        while (offset < responseData.size()) {
            ui64 targetBlock = ctx.StartIndexOffset + b;
            Y_ABORT_UNLESS(targetBlock < data.size());
            ui64 bytes =
                Min(responseData.size() - offset, data[targetBlock].Size());
            Y_ABORT_UNLESS(bytes);

            char* dst = const_cast<char*>(data[targetBlock].Data());
            const char* src = responseData.data() + offset;

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
            VoidBlockCount += ctx.BlockCount;
        }

        return {};
    }

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>
    CreateCompletionEvent(const NProto::TError& error)
    {
        const auto requestBlockCount = GetRequestBlockCount();
        const bool allZeroes = VoidBlockCount == requestBlockCount;

        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>(error);
        completion->NonVoidBlockCount = allZeroes ? 0 : requestBlockCount;
        completion->VoidBlockCount = allZeroes ? requestBlockCount : 0;
        auto& counters = *completion->Stats.MutableUserReadCounters();
        counters.SetBlocksCount(requestBlockCount);

        return completion;
    }

    std::unique_ptr<IEventBase> CreateResponse(const NProto::TError& error)
    {
        const bool allZeroes = VoidBlockCount == GetRequestBlockCount();
        auto response =
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        response->Record.SetAllZeroes(allZeroes);
        if (ShouldReportBlockRangeOnFailure) {
            response->Record.FailInfo.FailedRanges.push_back(
                DescribeRange(BlockRange));
        }
        if (auto checksum = CombineChecksums(Checksums);
            checksum.GetByteCount() > 0)
        {
            *response->Record.MutableChecksum() = std::move(checksum);
        }

        return response;
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
        &deviceRequests);

    if (!ok) {
        return;
    }

    const auto requestId = RequestsInProgress.GenerateRequestId();

    auto requestContext = std::make_shared<TRdmaRequestReadBlocksLocalHandler>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        deviceRequests.size(),
        std::move(msg->Record.Sglist),
        blockRange,
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
