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

class IRdmaDeviceRequestHandler: public NRdma::IClientHandler
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

    // Indices of devices that participated in the request.
    TStackVec<TDeviceRequestResult, 2> RequestsResult;

    TAdaptiveLock Lock;
    NProto::TError Error;

public:
    explicit IRdmaDeviceRequestHandler(
        NActors::TActorSystem* actorSystem,
        TNonreplicatedPartitionConfigPtr partConfig,
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        NActors::TActorId parentActorId,
        ui32 requestBlockCount,
        ui32 responseCount);

    ~IRdmaDeviceRequestHandler() override = default;

    bool HandleSubResponse(
        TDeviceRequestRdmaContext& reqCtx,
        ui32 status,
        TStringBuf buffer);

    void HandleResponse(
        NRdma::TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override;

    template <typename TCompletionEvent>
    std::unique_ptr<TCompletionEvent> CreateCompletionEventImpl()
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

    void SendEvent(
        NActors::TActorId recipient,
        std::unique_ptr<NActors::IEventBase> ev,
        ui64 cookie = 0) const;

private:
    virtual std::unique_ptr<NActors::IEventBase> CreateResponse(
        NProto::TError error) = 0;

    virtual std::unique_ptr<NActors::IEventBase> CreateCompletionEvent() = 0;

    virtual NProto::TError ProcessSubResponse(
        const TDeviceRequestRdmaContext& reqCtx,
        TStringBuf buffer) = 0;
};

}   // namespace NCloud::NBlockStore::NStorage
