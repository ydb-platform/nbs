#pragma once

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
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
// The replica for executing the request is selected by the round robin and
// TEvMultiAgentWriteRequest send to it.

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
    const size_t RoundRobinSeed;

    TVector<TEvNonreplPartitionPrivate::TGetDeviceForRangeResponse>
        DevicesAndRanges;
    size_t DiscoveryResponseCount = 0;

public:
    TMultiAgentWriteActor(
        TRequestInfoPtr requestInfo,
        TVector<NActors::TActorId> replicas,
        typename TMethod::TRequest::ProtoRecordType request,
        TBlockRange64 range,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter,
        size_t roundRobinSeed);

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

    void HandleMultiAgentWriteDeviceBlocksResponse(
        const TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse::TPtr& ev,
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
        size_t roundRobinSeed)
    : RequestInfo(std::move(requestInfo))
    , Replicas(std::move(replicas))
    , Request(std::move(request))
    , Range(range)
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , NonreplicatedRequestCounter(nonreplicatedRequestCounter)
    , RoundRobinSeed(roundRobinSeed)
{
    Y_DEBUG_ABORT_UNLESS(!Replicas.empty());
    DevicesAndRanges.resize(Replicas.size());
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
    // Select disk-agent that will do the job.
    const size_t executeOnReplica = RoundRobinSeed % Replicas.size();
    Rotate(
        DevicesAndRanges.begin(),
        DevicesAndRanges.begin() + executeOnReplica,
        DevicesAndRanges.end());

    auto request = std::make_unique<
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest>();
    request->CallContext = RequestInfo->CallContext;

    auto& rec = request->Record;

    *rec.MutableHeaders() = Request.GetHeaders();
    rec.BlockSize = DevicesAndRanges[0].PartConfig->GetBlockSize();
    rec.Range = Range;
    rec.DevicesAndRanges = DevicesAndRanges;

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
            ResizeIOVector(*rec.MutableBlocks(), Range.Size(), rec.BlockSize));

    } else {
        rec.MutableBlocks()->Swap(Request.MutableBlocks());
    }

    NCloud::Send(ctx, Replicas[executeOnReplica], std::move(request));
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::Fallback(const NActors::TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "Fallback to TMirrorRequestActor diskId: %s Range: %s",
        DiskId.Quote().c_str(),
        Range.Print().c_str());

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
    const size_t replicaIdx = ev->Cookie;

    DevicesAndRanges[replicaIdx] = *msg;
    ++DiscoveryResponseCount;

    if (HasError(msg->GetError())) {
        // For partition we can't perform direct disk-agent write.
        // Let's fallback to TMirrorRequestActor<> actor.
        Fallback(ctx);
        return;
    }

    if (DiscoveryResponseCount == Replicas.size()) {
        SendWriteRequest(ctx);
    }
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleMultiAgentWriteDeviceBlocksResponse(
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NProto::TError error = msg->GetError();

    if (error.GetCode() == E_ARGUMENT) {
        Fallback(ctx);
        return;
    }

    ProcessMirrorActorError(error);

    if (msg->Record.InconsistentResponse) {
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
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse,
            HandleMultiAgentWriteDeviceBlocksResponse);
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
