#pragma once

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/core/device_operation_tracker.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_common.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/public.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceRequestRdmaContext: public NRdma::TNullContext
{
    // Index of the device in the partition config.
    const ui32 DeviceIdx;

    explicit TDeviceRequestRdmaContext(ui32 deviceIdx)
        : DeviceIdx(deviceIdx)
    {}
};

struct IClientHandlerWithTracking: public NRdma::IClientHandler
{
    virtual void OnRequestStarted(
        ui32 deviceIdx,
        TDeviceOperationTracker::ERequestType requestType) = 0;
};
using IClientHandlerWithTrackingPtr =
    std::shared_ptr<IClientHandlerWithTracking>;

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TRdmaDeviceRequestHandlerBase: public IClientHandlerWithTracking
{
    using TDeviceRequestResult =
        TEvNonreplPartitionPrivate::TOperationCompleted::TDeviceRequestResult;

private:
    NActors::TActorSystem* ActorSystem;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TRequestInfoPtr RequestInfo;
    const ui64 RequestId;
    const NActors::TActorId VolumeActorId;
    const NActors::TActorId ParentActorId;
    const ui32 RequestBlockCount;
    const ui64 DeviceOperationId;
    ui32 ResponseCount;

    // Which device the requests were made to.  The vector contains a maximum of
    // two elements, because a user request can lie on the border of only two
    // devices. Each element contains the index of the device in the partition
    // config.
    TStackVec<ui32, 2> DeviceIndexes;

    // Results of requests for each device.
    TStackVec<TDeviceRequestResult, 2> RequestResults;

    TAdaptiveLock Lock;
    NProto::TError Error;

public:
    explicit TRdmaDeviceRequestHandlerBase(
        NActors::TActorSystem* actorSystem,
        TNonreplicatedPartitionConfigPtr partConfig,
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        NActors::TActorId volumeActorId,
        NActors::TActorId parentActorId,
        ui32 requestBlockCount,
        ui32 responseCount);

    ~TRdmaDeviceRequestHandlerBase() override = default;

    bool HandleSubResponse(
        TDeviceRequestRdmaContext& reqCtx,
        ui32 status,
        TStringBuf buffer);

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override;

    ui64 GetRequestBlockCount() const
    {
        return RequestBlockCount;
    }

    const TNonreplicatedPartitionConfig& GetPartConfig() const
    {
        return *PartConfig;
    }

    TRequestInfo& GetRequestInfo()
    {
        return *RequestInfo;
    }

    void OnRequestStarted(
        ui32 deviceIdx,
        TDeviceOperationTracker::ERequestType requestType) override;

private:
    void OnRequestFinished(ui32 deviceIdx);

    TDerived& GetDerived()
    {
        return static_cast<TDerived&>(*this);
    }

    // Default implementation
    template <typename TProto>
    static NProto::TError ProcessSubResponseProto(
        const TDeviceRequestRdmaContext& ctx,
        TProto& proto,
        TStringBuf responseData)
    {
        Y_UNUSED(ctx, proto, responseData);

        return {};
    }

    NProto::TError ProcessSubResponse(
        const TDeviceRequestRdmaContext& ctx,
        TStringBuf buffer)
    {
        auto* serializer = TBlockStoreProtocol::Serializer();
        auto [result, error] = serializer->Parse(buffer);

        if (HasError(error)) {
            return error;
        }

        auto& proto =
            static_cast<typename TDerived::TResponseProto&>(*result.Proto);
        if (HasError(proto.GetError())) {
            return proto.GetError();
        }

        return GetDerived().ProcessSubResponseProto(
            static_cast<const typename TDerived::TRequestContext&>(ctx),
            proto,
            result.Data);
    }
};

////////////////////////////////////////////////////////////////////////////////

void ConvertRdmaErrorIfNeeded(ui32 rdmaStatus, NProto::TError& err);

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
TRdmaDeviceRequestHandlerBase<TDerived>::TRdmaDeviceRequestHandlerBase(
        NActors::TActorSystem* actorSystem,
        TNonreplicatedPartitionConfigPtr partConfig,
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        NActors::TActorId volumeActorId,
        NActors::TActorId parentActorId,
        ui32 requestBlockCount,
        ui32 responseCount)
    : ActorSystem(actorSystem)
    , PartConfig(std::move(partConfig))
    , RequestInfo(std::move(requestInfo))
    , RequestId(requestId)
    , VolumeActorId(volumeActorId)
    , ParentActorId(parentActorId)
    , RequestBlockCount(requestBlockCount)
    , DeviceOperationId(TDeviceOperationTracker::GenerateId(responseCount))
    , ResponseCount(responseCount)
{}

template <typename TDerived>
void TRdmaDeviceRequestHandlerBase<TDerived>::OnRequestStarted(
    ui32 deviceIdx,
    TDeviceOperationTracker::ERequestType requestType)
{
    if (!DeviceOperationId) {
        // Tracking of this request is disabled.
        return;
    }

    const auto& devices = PartConfig->GetDevices();

    auto startEvent = std::make_unique<
        TEvVolumePrivate::TEvDiskRegistryDeviceOperationStarted>(
        devices[deviceIdx].GetAgentId(),
        requestType,
        DeviceOperationId + DeviceIndexes.size());
    DeviceIndexes.push_back(deviceIdx);
    ActorSystem->Send(VolumeActorId, startEvent.release());
}

template <typename TDerived>
void TRdmaDeviceRequestHandlerBase<TDerived>::OnRequestFinished(
    ui32 deviceIdx)
{
    if (!DeviceOperationId) {
        // Tracking of this request is disabled.
        return;
    }

    std::optional<size_t> requestIndex = 0;
    for (size_t i = 0; i < DeviceIndexes.size(); ++i) {
        if (DeviceIndexes[i] == deviceIdx) {
            requestIndex = i;
            break;
        }
    }
    Y_DEBUG_ABORT_UNLESS(requestIndex.has_value());

    auto finishEvent = std::make_unique<
        TEvVolumePrivate::TEvDiskRegistryDeviceOperationFinished>(
        DeviceOperationId + requestIndex.value_or(0));

    ActorSystem->Send(VolumeActorId, finishEvent.release());
}

template <typename TDerived>
bool TRdmaDeviceRequestHandlerBase<TDerived>::HandleSubResponse(
    TDeviceRequestRdmaContext& reqCtx,
    ui32 status,
    TStringBuf buffer)
{
    OnRequestFinished(reqCtx.DeviceIdx);
    RequestResults.emplace_back(reqCtx.DeviceIdx);

    if (status == NRdma::RDMA_PROTO_OK) {
        auto err = ProcessSubResponse(reqCtx, buffer);
        if (HasError(err)) {
            RequestResults.back().Error = err;
            Error = std::move(err);
        }
    } else {
        Error = NRdma::ParseError(buffer);
        ConvertRdmaErrorIfNeeded(status, Error);
        RequestResults.back().Error = Error;
    }

    --ResponseCount;
    return ResponseCount == 0;
}

template <typename TDerived>
void TRdmaDeviceRequestHandlerBase<TDerived>::HandleResponse(
    NRdma::TClientRequestPtr req,
    ui32 status,
    size_t responseBytes)
{
    TRequestScope timer(*RequestInfo);
    auto guard = Guard(Lock);

    auto buffer = req->ResponseBuffer.Head(responseBytes);
    auto* reqCtx = static_cast<TDeviceRequestRdmaContext*>(req->Context.get());

    if (!HandleSubResponse(*reqCtx, status, buffer)) {
        return;
    }

    ProcessError(*ActorSystem, *PartConfig, Error);

    auto response = GetDerived().CreateResponse(Error);
    auto completion = GetDerived().CreateCompletionEvent(Error);

    completion->RequestResults.assign(
        std::make_move_iterator(RequestResults.begin()),
        std::make_move_iterator(RequestResults.end()));

    completion->TotalCycles = RequestInfo->GetTotalCycles();
    completion->ExecCycles = RequestInfo->GetExecCycles();

    timer.Finish();

    ActorSystem
        ->Send(RequestInfo->Sender, response.release(), 0, RequestInfo->Cookie);

    ActorSystem->Send(ParentActorId, completion.release(), 0, RequestId);
}

}   // namespace NCloud::NBlockStore::NStorage
