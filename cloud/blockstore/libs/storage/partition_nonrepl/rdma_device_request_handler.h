#pragma once

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
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
class TRdmaDeviceRequestHandler: public NRdma::IClientHandler
{
private:
    NActors::TActorSystem* ActorSystem;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const TRequestInfoPtr RequestInfo;
    const ui64 RequestId;
    const NActors::TActorId ParentActorId;
    const ui32 RequestBlockCount;
    ui32 ResponseCount;

    // Indices of devices that participated in the request.
    TStackVec<
        TEvNonreplPartitionPrivate::TOperationCompleted::TDeviceRequestResult,
        2>
        RequestsResult;

    TAdaptiveLock Lock;
    NProto::TError Error;

public:
    explicit TRdmaDeviceRequestHandler(
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

    ~TRdmaDeviceRequestHandler() override = default;

    bool HandleSubResponse(
        TDeviceRequestRdmaContext& reqCtx,
        ui32 status,
        TStringBuf buffer)
    {
        RequestsResult.emplace_back(reqCtx.DeviceIdx);

        if (status == NRdma::RDMA_PROTO_OK) {
            auto err = static_cast<TDerived*>(this)->ProcessSubResponse(
                reqCtx,
                buffer);
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

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        TRequestScope timer(*RequestInfo);
        auto guard = Guard(Lock);

        auto buffer = req->ResponseBuffer.Head(responseBytes);
        auto* reqCtx =
            static_cast<TDeviceRequestRdmaContext*>(req->Context.get());

        if (!HandleSubResponse(*reqCtx, status, buffer)) {
            return;
        }

        ProcessError(*ActorSystem, *PartConfig, Error);

        auto completion =
            static_cast<const TDerived*>(this)->CreateCompletionEvent();
        auto completionEvent = std::make_unique<NActors::IEventHandle>(
            ParentActorId,
            NActors::TActorId(),
            completion.release(),
            0,
            RequestId);

        auto response =
            static_cast<TDerived*>(this)->CreateResponse(std::move(Error));

        auto event = std::make_unique<NActors::IEventHandle>(
            RequestInfo->Sender,
            NActors::TActorId(),
            response.release(),
            0,
            RequestInfo->Cookie);
        timer.Finish();
        ActorSystem->Send(event.release());
        ActorSystem->Send(completionEvent.release());
    }

    template <typename TCompletionEvent>
    std::unique_ptr<TCompletionEvent> CreateCompletionEvent()
    {
        auto completion = std::make_unique<TCompletionEvent>(Error);

        completion->RequestsResult.assign(
            std::make_move_iterator(RequestsResult.begin()),
            std::make_move_iterator(RequestsResult.end()));

        completion->TotalCycles = RequestInfo->GetTotalCycles();
        completion->ExecCycles = RequestInfo->GetExecCycles();

        return completion;
    }

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

private:
    void ConvertRdmaErrorIfNeeded(ui32 rdmaStatus, NProto::TError& err)
    {
        if (rdmaStatus == NRdma::RDMA_PROTO_FAIL &&
            err.GetCode() == E_CANCELLED)
        {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_INSTANT_RETRIABLE);
            err = MakeError(
                E_REJECTED,
                std::move(*err.MutableMessage()) + ", converted to E_REJECTED",
                flags);
        }
    }
};

}   // namespace NCloud::NBlockStore::NStorage
