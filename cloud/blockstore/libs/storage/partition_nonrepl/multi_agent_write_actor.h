#pragma once

#include "part_nonrepl_events_private.h"
#include "util/random/shuffle.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/mirror_request_actor.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_common.h>

#include <cloud/storage/core/protos/error.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

// The TMultiAgentWriteActor class is used to write to N replicas for the mirror
// disk. It should process all errors from replicas into E_REJECTED. Because
// sooner or later the disk agent will return to work, or the replica will be
// replaced. All replicas are equivalent and are placed in the Replicas.

template <typename TMethod>
class TMultiAgentWriteActor final
    : public NActors::TActorBootstrapped<TMultiAgentWriteActor<TMethod>>
{
private:
    using TBase = NActors::TActorBootstrapped<TMultiAgentWriteActor<TMethod>>;
    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;

    const TRequestInfoPtr RequestInfo;
    const TVector<NActors::TActorId> Replicas;
    typename TMethod::TRequest::ProtoRecordType Request;
    const TBlockRange64 Range;
    const TString DiskId;
    const NActors::TActorId ParentActorId;
    const ui64 NonreplicatedRequestCounter;
    const bool AssignVolumeRequestId = true;

    TVector<TEvNonreplPartitionPrivate::TGetDeviceForRangeResponse>
        DevicesAndRanges;

public:
    TMultiAgentWriteActor(
        TRequestInfoPtr requestInfo,
        TVector<NActors::TActorId> replicas,
        typename TMethod::TRequest::ProtoRecordType request,
        TBlockRange64 range,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter,
        const bool assignVolumeRequestId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendDiscoveryRequests(const NActors::TActorContext& ctx);
    void SendWriteRequest(const NActors::TActorContext& ctx);
    void Fallback(const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleGetDeviceRangeResponse(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void HandleWriteDeviceBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TMultiAgentWriteActor<TMethod>::TMultiAgentWriteActor(
        TRequestInfoPtr requestInfo,
        TVector<NActors::TActorId> replicas,
        typename TMethod::TRequest::ProtoRecordType request,
        TBlockRange64 range,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter,
        const bool assignVolumeRequestId)
    : RequestInfo(std::move(requestInfo))
    , Replicas(std::move(replicas))
    , Request(std::move(request))
    , Range(range)
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , NonreplicatedRequestCounter(nonreplicatedRequestCounter)
    , AssignVolumeRequestId(assignVolumeRequestId)
{
    Y_DEBUG_ABORT_UNLESS(!Replicas.empty());
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::Bootstrap(
    const NActors::TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "MultiAgentWrite",
        RequestInfo->CallContext->RequestId);

    SendDiscoveryRequests(ctx);
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::SendDiscoveryRequests(
    const NActors::TActorContext& ctx)
{
    using EPurpose =
        TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::EPurpose;

    ui64 count = 0;
    for (const auto& actorId: Replicas) {
        ctx.Send(
            actorId,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest>(
                EPurpose::ForWriting,
                Range),
            0,
            count++);
    }
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::SendWriteRequest(
    const NActors::TActorContext& ctx)
{
    // Randomizing the selection of the disk-agent that will do the job.
    Shuffle(DevicesAndRanges.begin(), DevicesAndRanges.end());

    auto request =
        std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
    auto& rec = request->Record;

    *rec.MutableHeaders() = Request.GetHeaders();
    rec.SetBlockSize(DevicesAndRanges[0].PartConfig->GetBlockSize());
    if constexpr (IsLocalMethod<TMethod>) {
        auto guard = Request.Sglist.Acquire();
        if (!guard) {
            Done(
                ctx,
                MakeError(
                    E_CANCELLED,
                    "failed to acquire sglist in TMultiAgentWriteActor"));
            return;
        }
        SgListCopy(
            guard.Get(),
            ResizeIOVector(
                *rec.MutableBlocks(),
                Range.Size(),
                rec.GetBlockSize()));

    } else {
        rec.MutableBlocks()->Swap(Request.MutableBlocks());
    }

    TDuration maxRequestTimeout;
    for (const auto& deviceInfo: DevicesAndRanges) {
        maxRequestTimeout =
            std::max(maxRequestTimeout, deviceInfo.RequestTimeout);

        auto* replicationTarget = rec.AddReplicationTargets();
        replicationTarget->SetNodeId(deviceInfo.Device.GetNodeId());
        replicationTarget->SetDeviceUUID(deviceInfo.Device.GetDeviceUUID());
        replicationTarget->SetStartIndex(deviceInfo.DeviceBlockRange.Start);
    }

    if (AssignVolumeRequestId) {
        rec.SetVolumeRequestId(Request.GetHeaders().GetVolumeRequestId());
        rec.SetMultideviceRequest(false);
    }

    auto event = std::make_unique<NActors::IEventHandle>(
        MakeDiskAgentServiceId(DevicesAndRanges[0].Device.GetNodeId()),
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());

    ctx.Schedule(maxRequestTimeout, new NActors::TEvents::TEvWakeup());
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::Fallback(const NActors::TActorContext& ctx)
{
    // Delegate all work to original actor.
    NCloud::Register<TMirrorRequestActor<TMethod>>(
        ctx,
        std::move(RequestInfo),
        Replicas,
        std::move(Request),
        DiskId,
        ParentActorId,
        NonreplicatedRequestCounter);
    TBase::Die(ctx);
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::Done(
    const NActors::TActorContext& ctx,
    NProto::TError error)
{
    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "MultiAgentWrite",
        RequestInfo->CallContext->RequestId);

    auto response = std::make_unique<typename TMethod::TResponse>();
    *response->Record.MutableError() = std::move(error);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted>(
            NonreplicatedRequestCounter,
            RequestInfo->GetTotalCycles(),
            false   // FollowerGotNonRetriableError
        );
    NCloud::Send(ctx, ParentActorId, std::move(completion));

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleGetDeviceRangeResponse(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    DevicesAndRanges.push_back(*msg);

    if (HasError(msg->GetError())) {
        // For partition we can't perform direct disk-agent write.
        // Let's fallback to TMirrorRequestActor<> actor.
        Fallback(ctx);
        return;
    }

    if (DevicesAndRanges.size() == Replicas.size()) {
        SendWriteRequest(ctx);
    }
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleWriteDeviceBlocksResponse(
    const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NProto::TError error = msg->GetError();

    ProcessError(
        *NActors::TActorContext::ActorSystem(),
        *DevicesAndRanges[0].PartConfig,
        error);
    ProcessMirrorActorError(error);

    bool subResponsesOk = msg->Record.GetReplicationResponses().size() ==
                              static_cast<int>(Replicas.size()) &&
                          AllOf(
                              msg->Record.GetReplicationResponses(),
                              [](const NProto::TError& subResponseError)
                              { return subResponseError.GetCode() == S_OK; });
    if (error.GetCode() == S_OK && !subResponsesOk) {
        // The disk agent returned a response that does not contain a subrequest
        // responses, which means that the disk agent cannot process multi-agent
        // requests. In this case, we need to turn off the feature and repeat
        // write request.
        error = MakeError(E_REJECTED, "inconsistent response");
        NCloud::Send(
            ctx,
            ParentActorId,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvInconsistentDiskAgent>(
                DevicesAndRanges[0].PartConfig->GetName()));
    }

    Done(ctx, std::move(error));
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleUndelivery(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] MultiAgentWrite. Request undelivered",
        DiskId.c_str());

    Done(ctx, MakeError(E_REJECTED, "MultiAgentWrite. Request undelivered"));
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleWriteTimeout(
    const NActors::TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(
        E_TIMEOUT,
        TStringBuilder() << "Range " << DescribeRange(Range)
                         << " write timeout");
    Done(ctx, std::move(error));
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Done(ctx, MakeError(E_REJECTED, "Dead"));
}

template <typename TMethod>
STFUNC(TMultiAgentWriteActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse,
            HandleGetDeviceRangeResponse);
        HFunc(
            TEvDiskAgent::TEvWriteDeviceBlocksResponse,
            HandleWriteDeviceBlocksResponse);
        HFunc(TEvDiskAgent::TEvWriteDeviceBlocksRequest, HandleUndelivery);
        HFunc(NActors::TEvents::TEvWakeup, HandleWriteTimeout);
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
