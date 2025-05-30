#pragma once

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_common.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/public.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceRequestRdmaContext: public NRdma::TNullContext
{
    ui32 DeviceIdx = 0;

    TDeviceRequestRdmaContext() = default;
    explicit TDeviceRequestRdmaContext(ui32 deviceIdx)
        : DeviceIdx(deviceIdx)
    {}
};

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TRdmaDeviceRequestHandlerBase: public NRdma::IClientHandler
{
    using TDeviceRequestResult =
        TEvNonreplPartitionPrivate::TOperationCompleted::TDeviceRequestResult;

private:
    NActors::TActorSystem* ActorSystem;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TRequestInfoPtr RequestInfo;
    const ui64 RequestId;
    const NActors::TActorId ParentActorId;
    const ui32 RequestBlockCount;
    ui32 ResponseCount;

    // Results of requests for each device.
    TStackVec<TDeviceRequestResult, 2> RequestsResult;

    TAdaptiveLock Lock;
    NProto::TError Error;

public:
    explicit TRdmaDeviceRequestHandlerBase(
        NActors::TActorSystem* actorSystem,
        TNonreplicatedPartitionConfigPtr partConfig,
        TRequestInfoPtr requestInfo,
        ui64 requestId,
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

protected:
    template <typename TCompletionEvent>
    std::unique_ptr<TCompletionEvent> CreateConcreteCompletionEvent()
    {
        auto completion = std::make_unique<TCompletionEvent>(Error);

        completion->RequestsResult.assign(
            std::make_move_iterator(RequestsResult.begin()),
            std::make_move_iterator(RequestsResult.end()));

        completion->TotalCycles = RequestInfo->GetTotalCycles();
        completion->ExecCycles = RequestInfo->GetExecCycles();

        return completion;
    }

private:
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
        NActors::TActorId parentActorId,
        ui32 requestBlockCount,
        ui32 responseCount)
    : ActorSystem(actorSystem)
    , PartConfig(std::move(partConfig))
    , RequestInfo(std::move(requestInfo))
    , RequestId(requestId)
    , ParentActorId(parentActorId)
    , RequestBlockCount(requestBlockCount)
    , ResponseCount(responseCount)
{}

template <typename TDerived>
bool TRdmaDeviceRequestHandlerBase<TDerived>::HandleSubResponse(
    TDeviceRequestRdmaContext& reqCtx,
    ui32 status,
    TStringBuf buffer)
{
    RequestsResult.emplace_back(reqCtx.DeviceIdx);

    if (status == NRdma::RDMA_PROTO_OK) {
        auto err = ProcessSubResponse(reqCtx, buffer);
        if (HasError(err)) {
            RequestsResult.back().Error = err;
            Error = std::move(err);
        }
    } else {
        Error = NRdma::ParseError(buffer);
        ConvertRdmaErrorIfNeeded(status, Error);
        RequestsResult.back().Error = Error;
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

    auto response = GetDerived().CreateResponse(std::move(Error));
    auto completion = GetDerived().CreateCompletionEvent();

    timer.Finish();

    ActorSystem
        ->Send(RequestInfo->Sender, response.release(), 0, RequestInfo->Cookie);

    ActorSystem->Send(ParentActorId, completion.release(), 0, RequestId);
}

}   // namespace NCloud::NBlockStore::NStorage
