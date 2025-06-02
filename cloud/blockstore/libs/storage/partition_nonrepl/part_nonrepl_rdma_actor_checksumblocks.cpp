#include "part_nonrepl_rdma_actor.h"
#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceChecksumRequestContext: public TDeviceRequestRdmaContext
{
    ui64 RangeStartIndex = 0;
    ui32 RangeSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

using TResponse = TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse;

////////////////////////////////////////////////////////////////////////////////

struct TPartialChecksum
{
    ui64 Value;
    ui64 Size;
};

////////////////////////////////////////////////////////////////////////////////

class TDeviceChecksumRequestHandler final
    : public TRdmaDeviceRequestHandlerBase<TDeviceChecksumRequestHandler>
{
    using TBase = TRdmaDeviceRequestHandlerBase<TDeviceChecksumRequestHandler>;

private:
    TMap<ui64, TPartialChecksum> Checksums;

public:
    using TRequestContext = TDeviceChecksumRequestContext;
    using TResponseProto = NProto::TChecksumDeviceBlocksResponse;

    using TBase::TBase;

    NProto::TError ProcessSubResponseProto(
        const TRequestContext& ctx,
        TResponseProto& proto,
        TStringBuf data)
    {
        Y_UNUSED(data);

        Checksums[ctx.RangeStartIndex] = {
            .Value = proto.GetChecksum(),
            .Size = ctx.RangeSize};

        return {};
    }

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted>
    CreateCompletionEvent(const NProto::TError& error)
    {
        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted>(error);

        auto& counters = *completion->Stats.MutableSysChecksumCounters();
        counters.SetBlocksCount(GetRequestBlockCount());
        return completion;
    }

    std::unique_ptr<IEventBase> CreateResponse(const NProto::TError& error)
    {
        TBlockChecksum checksum;
        for (const auto& [_, partialChecksum]: Checksums) {
            checksum.Combine(partialChecksum.Value, partialChecksum.Size);
        }

        auto response = std::make_unique<
            TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>(error);
        response->Record.SetChecksum(checksum.GetValue());

        return response;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionRdmaActor::HandleChecksumBlocks(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ChecksumBlocks",
        requestInfo->CallContext->RequestId);

    auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    TVector<TDeviceRequest> deviceRequests;
    bool ok = InitRequests<TEvNonreplPartitionPrivate::TChecksumBlocksMethod>(
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

    auto requestContext = std::make_shared<TDeviceChecksumRequestHandler>(
        ctx.ActorSystem(),
        PartConfig,
        requestInfo,
        requestId,
        SelfId(),
        msg->Record.GetBlocksCount(),
        deviceRequests.size());

    struct TDeviceRequestInfo
    {
        NRdma::IClientEndpointPtr Endpoint;
        NRdma::TClientRequestPtr ClientRequest;
    };

    TVector<TDeviceRequestInfo> requests;
    TRequestContext sentRequestCtx;

    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);
        auto dc = std::make_unique<TDeviceChecksumRequestContext>();
        dc->RangeStartIndex = r.BlockRange.Start;
        dc->RangeSize = r.DeviceBlockRange.Size() * PartConfig->GetBlockSize();
        dc->DeviceIdx = r.DeviceIdx;

        sentRequestCtx.emplace_back(r.DeviceIdx);

        NProto::TChecksumDeviceBlocksRequest deviceRequest;
        deviceRequest.MutableHeaders()->CopyFrom(msg->Record.GetHeaders());
        deviceRequest.SetDeviceUUID(r.Device.GetDeviceUUID());
        deviceRequest.SetStartIndex(r.DeviceBlockRange.Start);
        deviceRequest.SetBlocksCount(r.DeviceBlockRange.Size());
        deviceRequest.SetBlockSize(PartConfig->GetBlockSize());

        auto [req, err] = ep->AllocateRequest(
            requestContext,
            std::move(dc),
            NRdma::TProtoMessageSerializer::MessageByteSize(deviceRequest, 0),
            4_KB);

        if (HasError(err)) {
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
                "Failed to allocate rdma memory for ChecksumDeviceBlocksRequest"
                ", error: %s",
                FormatError(err).c_str());

            NotifyDeviceTimedOutIfNeeded(ctx, r.Device.GetDeviceUUID());

            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<TResponse>(std::move(err)));

            return;
        }

        NRdma::TProtoMessageSerializer::Serialize(
            req->RequestBuffer,
            TBlockStoreProtocol::ChecksumDeviceBlocksRequest,
            0,   // flags
            deviceRequest);

        requests.push_back({std::move(ep), std::move(req)});
    }

    for (size_t i = 0; i < requests.size(); ++i) {
        auto& request = requests[i];
        sentRequestCtx[i].SentRequestId = request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
    }

    RequestsInProgress.AddReadRequest(requestId, sentRequestCtx);
}

}   // namespace NCloud::NBlockStore::NStorage
