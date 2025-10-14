#pragma once

#include "part_nonrepl_actor.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TDiskAgentBaseRequestActor
    : public NActors::TActorBootstrapped<TDiskAgentBaseRequestActor>
{
protected:
    using EStatus = TEvNonreplPartitionPrivate::TOperationCompleted::EStatus;

    const TRequestInfoPtr RequestInfo;
    const TVector<TDeviceRequest> DeviceRequests;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const NActors::TActorId VolumeActorId;
    const NActors::TActorId Part;
    TChildLogTitle LogTitle;
    ui64 DeviceOperationId;
    bool ShouldTrackOperations;

private:
    const TString RequestName;
    const ui64 RequestId;
    const TRequestTimeoutPolicy TimeoutPolicy;

    TInstant StartTime;

public:
    TDiskAgentBaseRequestActor(
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        TString requestName,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        NActors::TActorId volumeActorId,
        const NActors::TActorId& part,
        TChildLogTitle logTitle,
        ui64 deviceOperationId,
        bool shouldTrackOperations);

    void Bootstrap(const NActors::TActorContext& ctx);

protected:
    struct TCompletionEventAndBody
    {
        template <typename T>
        explicit TCompletionEventAndBody(T event)
            : Body(event.get())
            , Event(std::move(event))
        {}

        TEvNonreplPartitionPrivate::TOperationCompleted* Body = nullptr;
        NActors::IEventBasePtr Event;
    };

    virtual bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) = 0;
    virtual void SendRequest(const NActors::TActorContext& ctx) = 0;
    virtual NActors::IEventBasePtr MakeResponse(NProto::TError error) = 0;
    virtual TCompletionEventAndBody MakeCompletionResponse(ui32 blocks) = 0;

    void HandleError(
        const NActors::TActorContext& ctx,
        NProto::TError error,
        EStatus status);

    void Done(
        const NActors::TActorContext& ctx,
        NActors::IEventBasePtr response,
        EStatus status);

private:
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev);

    void HandleCancelRequest(
        const TEvNonreplPartitionPrivate::TEvCancelRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
