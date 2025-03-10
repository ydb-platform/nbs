#pragma once

#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void ProcessMirrorActorError(NProto::TError& error);

////////////////////////////////////////////////////////////////////////////////

// The TMirrorRequestActor class is used for two tasks:
// 1. To write to N replicas for the mirror disk. In this mode, it should
// process all errors from replicas into E_REJECT. Because sooner or later the
// disk agent will return to work, or the replica will be replaced. All replicas
// are equivalent and are placed in the LeaderPartitions.
// 2. To migrate a new device, in this case it has two replicas, one is leader
// and the second is follower. If we receive a fatal error from the follower
// partition, we do not respond to the client with an error, but at the same
// time stop the migration. And if we receive a non-fatal error, response it to the
// client. The client will retry the request.

template <typename TMethod>
class TMirrorRequestActor final
    : public NActors::TActorBootstrapped<TMirrorRequestActor<TMethod>>
{
private:
    using TBase = NActors::TActorBootstrapped<TMirrorRequestActor<TMethod>>;
    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;

    const TRequestInfoPtr RequestInfo;
    const TVector<NActors::TActorId> LeaderPartitions;
    const NActors::TActorId FollowerPartition;
    const typename TMethod::TRequest::ProtoRecordType Request;
    const TString DiskId;
    const NActors::TActorId ParentActorId;
    const ui64 NonreplicatedRequestCounter;

    TVector<TCallContextPtr> ForkedCallContexts;
    ui32 Responses = 0;
    TResponseProto LeadersCollectiveResponse;
    TResponseProto FollowerResponse;

public:
    TMirrorRequestActor(
        TRequestInfoPtr requestInfo,
        TVector<NActors::TActorId> leaderPartitions,
        NActors::TActorId followerPartition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendRequests(const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);
    size_t GetTotalPartitionCount() const;
    void UpdateResponse(
        const NActors::TActorId& sender,
        TResponseProto&& response);
    bool HasFollower() const;

private:
    STFUNC(StateWork);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TMirrorRequestActor<TMethod>::TMirrorRequestActor(
        TRequestInfoPtr requestInfo,
        TVector<NActors::TActorId> leaderPartitions,
        NActors::TActorId followerPartition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter)
    : RequestInfo(std::move(requestInfo))
    , LeaderPartitions(std::move(leaderPartitions))
    , FollowerPartition(followerPartition)
    , Request(std::move(request))
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , NonreplicatedRequestCounter(nonreplicatedRequestCounter)
{
    Y_DEBUG_ABORT_UNLESS(GetTotalPartitionCount() > 0);
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::Bootstrap(const NActors::TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    SendRequests(ctx);
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::SendRequests(const NActors::TActorContext& ctx)
{
    auto sendRequest = [&](const NActors::TActorId& actorId) {
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
            actorId,
            ctx.SelfID,
            request.release(),
            NActors::IEventHandle::FlagForwardOnNondelivery,
            RequestInfo->Cookie,   // cookie
            &ctx.SelfID            // forwardOnNondelivery
        );

        ctx.Send(std::move(event));
    };

    for (const auto& actorId: LeaderPartitions) {
        sendRequest(actorId);
    }
    if (HasFollower()) {
        sendRequest(FollowerPartition);
    }
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::Done(const NActors::TActorContext& ctx)
{
    const bool isFollowerResponseError =
        HasFollower() && HasError(FollowerResponse);
    const bool isFollowerResponseFatal =
        isFollowerResponseError &&
        GetErrorKind(FollowerResponse.GetError()) == EErrorKind::ErrorFatal;

    if (isFollowerResponseError && !isFollowerResponseFatal) {
        UpdateResponse({}, std::move(FollowerResponse));
    }

    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(LeadersCollectiveResponse);

    auto& callContext = *RequestInfo->CallContext;
    for (auto& cc: ForkedCallContexts) {
        callContext.LWOrbit.Join(cc->LWOrbit);
    }

    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    auto completion =
        std::make_unique<TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted>(
            NonreplicatedRequestCounter,
            RequestInfo->GetTotalCycles(),
            isFollowerResponseFatal);
    NCloud::Send(ctx, ParentActorId, std::move(completion));

    TBase::Die(ctx);
}

template <typename TMethod>
size_t TMirrorRequestActor<TMethod>::GetTotalPartitionCount() const
{
    return LeaderPartitions.size() + (HasFollower() ? 1 : 0);
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::UpdateResponse(
    const NActors::TActorId& sender,
    TResponseProto&& response)
{
    if (sender == FollowerPartition) {
        FollowerResponse = std::move(response);
        return;
    }

    if (!HasFollower()) {
        ProcessMirrorActorError(*response.MutableError());
    }

    if (!HasError(LeadersCollectiveResponse)) {
        LeadersCollectiveResponse = std::move(response);
    }
}

template <typename TMethod>
bool TMirrorRequestActor<TMethod>::HasFollower() const
{
    return FollowerPartition != NActors::TActorId();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorRequestActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s request undelivered to some nonrepl partitions",
        DiskId.c_str(),
        TMethod::Name);

    LeadersCollectiveResponse.MutableError()->CopyFrom(MakeError(
        E_REJECTED,
        TStringBuilder() << TMethod::Name
                         << " request undelivered to some nonrepl partitions"));

    if (++Responses < GetTotalPartitionCount()) {
        return;
    }

    Done(ctx);
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record)) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "[%s] %s got error from nonreplicated partition: %s",
            DiskId.c_str(),
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());
    }

    UpdateResponse(ev->Sender, std::move(msg->Record));

    if (++Responses < GetTotalPartitionCount()) {
        return;
    }

    Done(ctx);
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LeadersCollectiveResponse.MutableError()->CopyFrom(
        MakeError(E_REJECTED, "Dead"));

    Done(ctx);
}

template <typename TMethod>
STFUNC(TMirrorRequestActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
