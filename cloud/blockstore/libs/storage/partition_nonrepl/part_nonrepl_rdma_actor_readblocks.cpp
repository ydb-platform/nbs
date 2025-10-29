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

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRdmaRequestReadBlocksHandler final
    : public TRdmaDeviceRequestHandlerBase<TRdmaRequestReadBlocksHandler>
{
    using TBase = TRdmaDeviceRequestHandlerBase<TRdmaRequestReadBlocksHandler>;

private:
    const bool CheckVoidBlocks;
    NProto::TReadBlocksResponse Response;
    ui32 VoidBlockCount = 0;
    TVector<NProto::TChecksum> Checksums;

public:
    using TRequestContext = TDeviceReadRequestContext;
    using TResponseProto = NProto::TReadDeviceBlocksResponse;

    TRdmaRequestReadBlocksHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            ui32 blockCount,
            NActors::TActorId parentActorId,
            ui64 requestId,
            bool checkVoidBlocks)
        : TBase(
              actorSystem,
              std::move(partConfig),
              std::move(requestInfo),
              requestId,
              parentActorId,
              blockCount,
              requestCount)
        , CheckVoidBlocks(checkVoidBlocks)
    {
        TRequestScope timer(GetRequestInfo());
        Checksums.resize(requestCount);

        auto& buffers = *Response.MutableBlocks()->MutableBuffers();
        buffers.Reserve(blockCount);
        for (ui32 i = 0; i < blockCount; ++i) {
            buffers.Add()->resize(GetPartConfig().GetBlockSize(), 0);
        }
    }

    NProto::TError ProcessSubResponseProto(
        const TRequestContext& ctx,
        TResponseProto& proto,
        TStringBuf data)
    {
        Y_ABORT_UNLESS(ctx.RequestIndex < Checksums.size());
        Checksums[ctx.RequestIndex] = std::move(*proto.MutableChecksum());

        auto& blocks = *Response.MutableBlocks()->MutableBuffers();

        ui64 offset = 0;
        ui64 b = 0;
        bool isAllZeroes = CheckVoidBlocks;
        while (offset < data.size()) {
            ui64 targetBlock = ctx.StartIndexOffset + b;
            Y_ABORT_UNLESS(targetBlock < static_cast<ui64>(blocks.size()));
            ui64 bytes =
                Min(data.size() - offset, blocks[targetBlock].size());
            Y_ABORT_UNLESS(bytes);

            char* dst = const_cast<char*>(blocks[targetBlock].data());
            const char* src = data.data() + offset;

            if (isAllZeroes) {
                isAllZeroes = IsAllZeroes(src, bytes);
            }
            memcpy(dst, src, bytes);

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
        const ui32 blockCount = GetRequestBlockCount();
        const bool allZeroes = VoidBlockCount == blockCount;

        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>(error);

        completion->NonVoidBlockCount = allZeroes ? 0 : blockCount;
        completion->VoidBlockCount = allZeroes ? blockCount : 0;
        auto& counters = *completion->Stats.MutableUserReadCounters();
        counters.SetBlocksCount(blockCount);

        return completion;
    }

    std::unique_ptr<IEventBase> CreateResponse(const NProto::TError& error)
    {
        const ui32 blockCount = Response.GetBlocks().BuffersSize();
        const bool allZeroes = VoidBlockCount == blockCount;

        *Response.MutableError() = error;
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
        response->Record = std::move(Response);
        response->Record.SetAllZeroes(allZeroes);
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

    auto requestContext = std::make_shared<TRdmaRequestReadBlocksHandler>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        deviceRequests.size(),
        blockRange.Size(),
        SelfId(),
        requestId,
        Config->GetOptimizeVoidBuffersTransferForReadsEnabled());

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
            std::make_unique<TEvService::TEvReadBlocksResponse>(
                std::move(error)));

        return;
    }

    RequestsInProgress.AddReadRequest(requestId, sentRequestCtx);
}

}   // namespace NCloud::NBlockStore::NStorage
