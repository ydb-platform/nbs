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

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceChecksumRequestContext: public TDeviceRequestContext
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

class TDeviceChecksumRequestHandler
    : public TRdmaDeviceRequestHandler<TDeviceChecksumRequestHandler>
{
    using TBase = TRdmaDeviceRequestHandler<TDeviceChecksumRequestHandler>;

private:
    TMap<ui64, TPartialChecksum> Checksums;

public:
    TDeviceChecksumRequestHandler(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : TRdmaDeviceRequestHandler(
              actorSystem,
              std::move(partConfig),
              std::move(requestInfo),
              requestId,
              parentActorId,
              requestBlockCount,
              requestCount)

    {}

    NProto::TError ProcessSubResponse(
        const TDeviceRequestContext& dCtx,
        TStringBuf buffer)
    {
        const auto& dc =
            static_cast<const TDeviceChecksumRequestContext&>(dCtx);
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);

        if (HasError(err)) {
            return err;
        }

        const auto& concreteProto =
            static_cast<NProto::TChecksumDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            return concreteProto.GetError();
        }

        Checksums[dc.RangeStartIndex] = {
            .Value = concreteProto.GetChecksum(),
            .Size = dc.RangeSize};
        return {};
    }

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted>
    CreateCompletionEvent() const
    {
        auto completion = TBase::CreateCompletionEvent<
            TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted>();

        auto& counters = *completion->Stats.MutableSysChecksumCounters();
        counters.SetBlocksCount(GetRequestBlockCount());
        return completion;
    }

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>
    CreateResponse(NProto::TError err) const
    {
        TBlockChecksum checksum;
        for (const auto& [_, partialChecksum]: Checksums) {
            checksum.Combine(partialChecksum.Value, partialChecksum.Size);
        }

        auto response = std::make_unique<
            TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>(
            std::move(err));
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

    for (auto& r: deviceRequests) {
        auto ep = AgentId2Endpoint[r.Device.GetAgentId()];
        Y_ABORT_UNLESS(ep);
        auto dc = std::make_unique<TDeviceChecksumRequestContext>();
        dc->RangeStartIndex = r.BlockRange.Start;
        dc->RangeSize = r.DeviceBlockRange.Size() * PartConfig->GetBlockSize();
        dc->DeviceIdx = r.DeviceIdx;

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

    for (auto& request: requests) {
        request.Endpoint->SendRequest(
            std::move(request.ClientRequest),
            requestInfo->CallContext);
    }

    RequestsInProgress.AddReadRequest(requestId);
}

}   // namespace NCloud::NBlockStore::NStorage
