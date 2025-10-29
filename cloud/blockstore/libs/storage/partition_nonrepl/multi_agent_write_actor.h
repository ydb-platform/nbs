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

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

// The TMultiAgentWriteActor class is used to write to N replicas for the mirror
// disk. It should process all errors from replicas into E_REJECTED. Because
// sooner or later the disk agent will return to work, or the replica will be
// replaced.
// Firstly, to determine if it is possible to write directly, it sends a
// TEvGetDeviceForRangeRequest to all the replicas. If there are multiple
// replicas available for multi-agent writing, the TEvMultiAgentWriteRequest
// will be sent to one agent selected through a round-robin process. An ordinary
// write will be sent to the replicas that are unable to perform multi-agent
// writes.

template <typename TMethod>
class TMultiAgentWriteActor final
    : public NActors::TActorBootstrapped<TMultiAgentWriteActor<TMethod>>
{
private:
    using TBase = NActors::TActorBootstrapped<TMultiAgentWriteActor<TMethod>>;
    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;

    struct TReplicaDiscovery
    {
        NActors::TActorId ActorId;
        bool ReadyForMultiAgentWrite = false;
        TEvNonreplPartitionPrivate::TGetDeviceForRangeResponse DiscoveryResult;
    };

    const TRequestInfoPtr RequestInfo;
    typename TMethod::TRequest::ProtoRecordType Request;
    const TBlockRange64 Range;
    const TString DiskId;
    const NActors::TActorId ParentActorId;
    const ui64 NonreplicatedRequestCounter;
    const size_t RoundRobinSeed;

    TVector<TReplicaDiscovery> ReplicasDiscovery;
    size_t DiscoveryResponseCount = 0;

    TVector<TCallContextPtr> ForkedCallContexts;
    NProto::TError ReplicasCollectiveResponse;
    size_t RemainResponseCount = 0;

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
    void SendMultiagentWriteRequest(const NActors::TActorContext& ctx);
    void SendOrdinaryWriteRequests(const NActors::TActorContext& ctx);
    void Fallback(const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);
    void UpdateResponse(NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleGetDeviceRangeResponse(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void HandleMultiAgentWriteDeviceBlocksResponse(
        const TEvNonreplPartitionPrivate::TEvMultiAgentWriteResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleOrdinaryWriteResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleMultiAgentWriteUndelivery(
        const TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleOrdinaryWriteUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
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
    , Request(std::move(request))
    , Range(range)
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , NonreplicatedRequestCounter(nonreplicatedRequestCounter)
    , RoundRobinSeed(roundRobinSeed)
{
    Y_DEBUG_ABORT_UNLESS(!replicas.empty());

    ReplicasDiscovery.reserve(replicas.size());
    for (const auto& actorId: replicas) {
        ReplicasDiscovery.push_back(TReplicaDiscovery{.ActorId = actorId});
    }
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
    for (const auto& replica: ReplicasDiscovery) {
        ctx.Send(
            replica.ActorId,
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
    size_t readyForMultiagentWrite = CountIf(
        ReplicasDiscovery,
        [](const TReplicaDiscovery& replica)
        { return replica.ReadyForMultiAgentWrite; });

    if (readyForMultiagentWrite < 2) {
        // At most one partition can perform direct disk-agent write.
        // Let's fallback to ordinary request for all replicas.
        for (auto& replica: ReplicasDiscovery) {
            replica.ReadyForMultiAgentWrite = false;
        }
    }

    SendOrdinaryWriteRequests(ctx);
    SendMultiagentWriteRequest(ctx);
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::SendMultiagentWriteRequest(
    const NActors::TActorContext& ctx)
{
    if (ReplicasDiscovery.empty()) {
        return;
    }

    // Select disk-agent that will do the job.
    const size_t executeOnReplica = RoundRobinSeed % ReplicasDiscovery.size();
    Rotate(
        ReplicasDiscovery.begin(),
        ReplicasDiscovery.begin() + executeOnReplica,
        ReplicasDiscovery.end());

    auto request = std::make_unique<
        TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest>();
    request->CallContext = RequestInfo->CallContext;

    auto& rec = request->Record;

    *rec.MutableHeaders() = Request.GetHeaders();
    rec.BlockSize =
        ReplicasDiscovery[0].DiscoveryResult.PartConfig->GetBlockSize();
    rec.Range = Range;
    if constexpr (IsExactlyWriteMethod<TMethod>) {
        rec.MutableChecksums()->CopyFrom(Request.GetChecksums());
    }
    for (const auto& discovery: ReplicasDiscovery) {
        rec.DevicesAndRanges.push_back(discovery.DiscoveryResult);
    }

    if constexpr (IsLocalMethod<TMethod>) {
        auto guard = Request.Sglist.Acquire();
        if (!guard) {
            UpdateResponse(MakeError(
                E_CANCELLED,
                "failed to acquire sglist in TMultiAgentWriteActor"));
            Done(ctx);
            return;
        }
        SgListCopy(
            guard.Get(),
            ResizeIOVector(*rec.MutableBlocks(), Range.Size(), rec.BlockSize));

    } else {
        rec.MutableBlocks()->Swap(Request.MutableBlocks());
    }

    auto event = std::make_unique<NActors::IEventHandle>(
        ReplicasDiscovery[0].ActorId,
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        RequestInfo->Cookie,   // cookie
        &ctx.SelfID            // forwardOnNondelivery
    );
    ctx.Send(std::move(event));
    ++RemainResponseCount;
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::SendOrdinaryWriteRequests(
    const NActors::TActorContext& ctx)
{
    for (const auto& replica: ReplicasDiscovery) {
        if (replica.ReadyForMultiAgentWrite) {
            continue;
        }

        auto request = std::make_unique<typename TMethod::TRequest>();
        auto& callContext = *RequestInfo->CallContext;
        if (!callContext.LWOrbit.Fork(request->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                callContext.LWOrbit,
                TMethod::Name,
                callContext.RequestId);
        }
        ForkedCallContexts.push_back(request->CallContext);
        request->Record = Request;

        auto event = std::make_unique<NActors::IEventHandle>(
            replica.ActorId,
            ctx.SelfID,
            request.release(),
            NActors::IEventHandle::FlagForwardOnNondelivery,
            RequestInfo->Cookie,   // cookie
            &ctx.SelfID            // forwardOnNondelivery
        );
        ctx.Send(std::move(event));
        ++RemainResponseCount;
    }

    EraseIf(
        ReplicasDiscovery,
        [](const TReplicaDiscovery& replica)
        { return !replica.ReadyForMultiAgentWrite; });
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
    TVector<NActors::TActorId> replicas;
    for (const auto& replica: ReplicasDiscovery) {
        replicas.push_back(replica.ActorId);
    }
    NCloud::Register<TMirrorRequestActor<TMethod>>(
        ctx,
        std::move(RequestInfo),
        std::move(replicas),
        std::move(Request),
        DiskId,
        ParentActorId,
        NonreplicatedRequestCounter);
    TBase::Die(ctx);
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::Done(const NActors::TActorContext& ctx)
{
    auto& callContext = *RequestInfo->CallContext;
    for (auto& cc: ForkedCallContexts) {
        callContext.LWOrbit.Join(cc->LWOrbit);
    }

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "MultiAgentWrite",
        RequestInfo->CallContext->RequestId);

    auto response = std::make_unique<typename TMethod::TResponse>();
    *response->Record.MutableError() = std::move(ReplicasCollectiveResponse);

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

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::UpdateResponse(NProto::TError error)
{
    ProcessMirrorActorError(error);

    if (!HasError(ReplicasCollectiveResponse)) {
        ReplicasCollectiveResponse = std::move(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleGetDeviceRangeResponse(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const size_t replicaIdx = ev->Cookie;

    ReplicasDiscovery[replicaIdx].ReadyForMultiAgentWrite =
        !HasError(msg->GetError());
    ReplicasDiscovery[replicaIdx].DiscoveryResult = std::move(*msg);

    ++DiscoveryResponseCount;
    if (DiscoveryResponseCount == ReplicasDiscovery.size()) {
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

    UpdateResponse(std::move(error));

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
                ReplicasDiscovery[0].DiscoveryResult.PartConfig->GetName()));
    }

    --RemainResponseCount;
    if (!RemainResponseCount) {
        Done(ctx);
    }
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleOrdinaryWriteResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "[%s] %s got error from nonreplicated partition: %s",
            DiskId.c_str(),
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());
    }

    UpdateResponse(std::move(msg->Record.GetError()));

    --RemainResponseCount;
    if (!RemainResponseCount) {
        Done(ctx);
    }
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleMultiAgentWriteUndelivery(
    const TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] TMultiAgentWriteRequest request undelivered to nonrepl partition",
        DiskId.c_str());

    UpdateResponse(MakeError(
        E_REJECTED,
        "TMultiAgentWriteRequestrequest undelivered to nonrepl partition"));

    --RemainResponseCount;
    if (!RemainResponseCount) {
        Done(ctx);
    }
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandleOrdinaryWriteUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s request undelivered to some nonrepl partitions",
        DiskId.c_str(),
        TMethod::Name);

    UpdateResponse(MakeError(
        E_REJECTED,
        TStringBuilder() << TMethod::Name
                         << " request undelivered to some nonrepl partitions"));

    --RemainResponseCount;
    if (!RemainResponseCount) {
        Done(ctx);
    }
}

template <typename TMethod>
void TMultiAgentWriteActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateResponse(MakeError(E_REJECTED, "Dead"));
    Done(ctx);
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
        HFunc(TMethod::TResponse, HandleOrdinaryWriteResponse);
        HFunc(
            TEvNonreplPartitionPrivate::TEvMultiAgentWriteRequest,
            HandleMultiAgentWriteUndelivery);
        HFunc(TMethod::TRequest, HandleOrdinaryWriteUndelivery);
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
