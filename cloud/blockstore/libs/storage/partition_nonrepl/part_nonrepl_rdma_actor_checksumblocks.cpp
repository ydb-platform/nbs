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

class TRdmaRequestContext: public IRdmaDeviceRequestHandler
{
private:
    TNonreplicatedPartitionConfigPtr PartConfig;
    TRequestInfoPtr RequestInfo;
    TAdaptiveLock Lock;
    size_t ResponseCount;
    TMap<ui64, TPartialChecksum> Checksums;
    NProto::TError Error;
    TStackVec<TString, 2> ErrDevices;
    const ui32 RequestBlockCount;
    ui64 RequestId;

public:
    TRdmaRequestContext(
            TActorSystem* actorSystem,
            TNonreplicatedPartitionConfigPtr partConfig,
            TRequestInfoPtr requestInfo,
            size_t requestCount,
            ui32 requestBlockCount,
            NActors::TActorId parentActorId,
            ui64 requestId)
        : IRdmaDeviceRequestHandler(actorSystem, parentActorId)
        , PartConfig(std::move(partConfig))
        , RequestInfo(std::move(requestInfo))
        , ResponseCount(requestCount)
        , RequestBlockCount(requestBlockCount)
        , RequestId(requestId)
    {}

    void HandleResult(const TDeviceChecksumRequestContext& dc, TStringBuf buffer)
    {
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, err] = serializer->Parse(buffer);

        if (HasError(err)) {
            Error = std::move(err);
            return;
        }

        const auto& concreteProto =
            static_cast<NProto::TChecksumDeviceBlocksResponse&>(*result.Proto);
        if (HasError(concreteProto.GetError())) {
            Error = concreteProto.GetError();
            return;
        }

        Checksums[dc.RangeStartIndex] = {
            concreteProto.GetChecksum(),
            dc.RangeSize};
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        TRequestScope timer(*RequestInfo);

        auto guard = Guard(Lock);

        auto* dc =
            static_cast<TDeviceChecksumRequestContext*>(req->Context.get());
        auto buffer = req->ResponseBuffer.Head(responseBytes);

        if (status == NRdma::RDMA_PROTO_OK) {
            HandleResult(*dc, buffer);
        } else {
            auto err = NRdma::ParseError(buffer);
            if (NeedToNotifyAboutError(err)) {
                ErrDevices.emplace_back(dc->DeviceUUID);
                SendDeviceTimedout(std::move(dc->DeviceUUID));
            }
            Error = std::move(err);
        }

        if (--ResponseCount != 0) {
            return;
        }

        // Got all device responses. Do processing.

        ProcessError(*ActorSystem, *PartConfig, Error);

        TBlockChecksum checksum;
        for (const auto& [_, partialChecksum]: Checksums) {
            checksum.Combine(partialChecksum.Value, partialChecksum.Size);
        }

        auto response = std::make_unique<TResponse>(Error);
        response->Record.SetChecksum(checksum.GetValue());
        auto event = std::make_unique<IEventHandle>(
            RequestInfo->Sender,
            TActorId(),
            response.release(),
            0,
            RequestInfo->Cookie);
        ActorSystem->Send(event.release());

        using TCompletionEvent =
            TEvNonreplPartitionPrivate::TEvChecksumBlocksCompleted;
        auto completion = std::make_unique<TCompletionEvent>(std::move(Error));
        auto& counters = *completion->Stats.MutableSysChecksumCounters();
        completion->TotalCycles = RequestInfo->GetTotalCycles();
        std::ranges::move(
            ErrDevices,
            std::back_inserter(completion->ErrorDevices));

        timer.Finish();
        completion->ExecCycles = RequestInfo->GetExecCycles();

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

    auto requestContext = std::make_shared<TRdmaRequestContext>(
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
        dc->DeviceUUID = r.Device.GetDeviceUUID();

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

    TRequestData requestData;
    for (const auto& r: deviceRequests){
        requestData.DeviceIndices.emplace_back(r.DeviceIdx);
    }

    RequestsInProgress.AddReadRequest(requestId, requestData);
}

}   // namespace NCloud::NBlockStore::NStorage
