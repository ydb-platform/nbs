#pragma once

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_common.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/public.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceRequestContext: public NRdma::TNullContext
{
    ui32 DeviceIdx = 0;
};

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
    TStackVec<ui32, 2> AllDevices;
    TStackVec<ui32, 2> ErrDevices;
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
        TDeviceRequestContext& dCtx,
        ui32 status,
        TStringBuf buffer)
    {
        AllDevices.emplace_back(dCtx.DeviceIdx);

        if (status == NRdma::RDMA_PROTO_OK) {
            auto err =
                static_cast<TDerived*>(this)->ProcessSubResponse(dCtx, buffer);
            if (HasError(err)) {
                Error = std::move(err);
            }
        } else {
            auto err = NRdma::ParseError(buffer);
            if (err.GetCode() == E_RDMA_UNAVAILABLE ||
                err.GetCode() == E_TIMEOUT)
            {
                ErrDevices.emplace_back(dCtx.DeviceIdx);
            }
            Error = std::move(err);
        }
        return --ResponseCount == 0;
    }

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        TRequestScope timer(*RequestInfo);
        auto guard = Guard(Lock);

        auto buffer = req->ResponseBuffer.Head(responseBytes);
        auto* dCtx = static_cast<TDeviceRequestContext*>(req->Context.get());

        if (!HandleSubResponse(*dCtx, status, buffer)) {
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
    std::unique_ptr<TCompletionEvent> CreateCompletionEvent() const
    {
        auto completion = std::make_unique<TCompletionEvent>(Error);

        completion->DeviceIndices = AllDevices;
        completion->ErrorDeviceIndices = ErrDevices;

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
};
}   // namespace NCloud::NBlockStore::NStorage
