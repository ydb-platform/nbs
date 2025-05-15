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

class TRdmaRequestReadBlocksLocalHandler: public IRdmaDeviceRequestHandler
{
private:
    const bool CheckVoidBlocks;
    TGuardedSgList SgList;

    ui32 VoidBlockCount = 0;

public:
    TRdmaRequestReadBlocksLocalHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            TGuardedSgList sglist,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId,
            bool checkVoidBlocks)
        : IRdmaDeviceRequestHandler(
              actorSystem,
              std::move(partConfig),
              std::move(requestInfo),
              requestId,
              parentActorId,
              requestBlockCount,
              requestCount)
        , CheckVoidBlocks(checkVoidBlocks)
        , SgList(std::move(sglist))
    {}

    NProto::TError ProcessSubResponse(
        const TDeviceRequestRdmaContext& reqCtx,
        TStringBuf buffer) override
    {
        const auto& readReqCtx =
            static_cast<const TDeviceReadRequestContext&>(reqCtx);
        auto guard = SgList.Acquire();
        if (!guard) {
            return MakeError(E_CANCELLED, "can't acquire sglist");
        }
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);

        if (HasError(err)) {
            return err;
        }

        const auto& concreteProto =
            static_cast<NProto::TReadDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            return concreteProto.GetError();
        }

        TSgList data = guard.Get();

        ui64 offset = 0;
        ui64 b = 0;
        bool isAllZeroes = CheckVoidBlocks;
        while (offset < result.Data.size()) {
            ui64 targetBlock = readReqCtx.StartIndexOffset + b;
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
            VoidBlockCount += readReqCtx.BlockCount;
        }

        return {};
    }

    std::unique_ptr<IEventBase> CreateCompletionEvent() override
    {
        const auto requestBlockCount = GetRequestBlockCount();
        const bool allZeroes = VoidBlockCount == requestBlockCount;

        auto completion = CreateCompletionEventImpl<
            TEvNonreplPartitionPrivate::TEvReadBlocksCompleted>();
        completion->NonVoidBlockCount = allZeroes ? 0 : requestBlockCount;
        completion->VoidBlockCount = allZeroes ? requestBlockCount : 0;
        auto& counters = *completion->Stats.MutableUserReadCounters();
        counters.SetBlocksCount(requestBlockCount);

        return completion;
    }

    std::unique_ptr<IEventBase> CreateResponse(NProto::TError error) override
    {
        const bool allZeroes = VoidBlockCount == GetRequestBlockCount();
        auto response =
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
                std::move(error));
        response->Record.SetAllZeroes(allZeroes);

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
        &deviceRequests
    );

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
        msg->Record.GetBlocksCount(),
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
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
                std::move(error)));

        return;
    }

    RequestsInProgress.AddReadRequest(requestId, sentRequestCtx);
}

}   // namespace NCloud::NBlockStore::NStorage
