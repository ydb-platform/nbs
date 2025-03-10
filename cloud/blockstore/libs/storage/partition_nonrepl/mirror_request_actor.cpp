#include "mirror_request_actor.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

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
    : public TActorBootstrapped<TMirrorRequestActor<TMethod>>
{
private:
    using TBase = TActorBootstrapped<TMirrorRequestActor<TMethod>>;
    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;

    const TRequestInfoPtr RequestInfo;
    const TVector<TActorId> LeaderPartitions;
    const TActorId FollowerPartition;
    const typename TMethod::TRequest::ProtoRecordType Request;
    const TString DiskId;
    const TActorId ParentActorId;
    const ui64 NonreplicatedRequestCounter;

    TVector<TCallContextPtr> ForkedCallContexts;
    ui32 Responses = 0;
    TResponseProto LeadersCollectiveResponse;
    TResponseProto FollowerResponse;

public:
    TMirrorRequestActor(
        TRequestInfoPtr requestInfo,
        TVector<TActorId> leaderPartitions,
        TActorId followerPartition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        TActorId parentActorId,
        ui64 nonreplicatedRequestCounter);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendRequests(const TActorContext& ctx);
    void Done(const TActorContext& ctx);
    size_t GetTotalPartitionCount() const;
    void UpdateResponse(
        const TActorId& sender,
        TResponseProto&& response);
    bool HasFollower() const;

private:
    STFUNC(StateWork);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TMirrorRequestActor<TMethod>::TMirrorRequestActor(
        TRequestInfoPtr requestInfo,
        TVector<TActorId> leaderPartitions,
        TActorId followerPartition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        TActorId parentActorId,
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
void TMirrorRequestActor<TMethod>::Bootstrap(const TActorContext& ctx)
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
void TMirrorRequestActor<TMethod>::SendRequests(const TActorContext& ctx)
{
    auto sendRequest = [&](const TActorId& actorId) {
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

        auto event = std::make_unique<IEventHandle>(
            actorId,
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
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
void TMirrorRequestActor<TMethod>::Done(const TActorContext& ctx)
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
    const TActorId& sender,
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
    return FollowerPartition != TActorId();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorRequestActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
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
    const TActorContext& ctx)
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
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
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
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
IActorPtr CreateDeviceMigrationRequestActorImpl(
    TRequestInfoPtr requestInfo,
    TActorId srcActorId,
    TActorId dstActorId,
    typename TMethod::TRequest::ProtoRecordType request,
    TString diskId,
    TActorId parentActorId,
    ui64 nonreplicatedRequestCounter)
{
    return std::make_unique<TMirrorRequestActor<TMethod>>(
        std::move(requestInfo),
        srcActorId ? TVector{srcActorId} : TVector<TActorId>(),
        dstActorId,
        std::move(request),
        std::move(diskId),
        parentActorId,
        nonreplicatedRequestCounter);

}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ProcessMirrorActorError(NProto::TError& error)
{
    if (HasError(error) && error.GetCode() != E_REJECTED) {
        // We believe that all errors of the mirrored disk can be fixed by
        // repeating the request.
        error = MakeError(E_REJECTED, FormatError(error));
    }
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateMirrorRequestActor(
    TRequestInfoPtr requestInfo,
    TVector<TActorId> replicaIds,
    NProto::TWriteBlocksRequest request,
    TString diskId,
    TActorId parentActorId,
    ui64 nonreplicatedRequestCounter)
{
    return std::make_unique<
        TMirrorRequestActor<TEvService::TWriteBlocksMethod>>(
        std::move(requestInfo),
        std::move(replicaIds),
        TActorId{},   // followerPartition
        std::move(request),
        std::move(diskId),
        parentActorId,
        nonreplicatedRequestCounter);
}

IActorPtr CreateMirrorRequestActor(
    TRequestInfoPtr requestInfo,
    TVector<TActorId> replicaIds,
    NProto::TWriteBlocksLocalRequest request,
    TString diskId,
    TActorId parentActorId,
    ui64 nonreplicatedRequestCounter)
{
    return std::make_unique<
        TMirrorRequestActor<TEvService::TWriteBlocksLocalMethod>>(
        std::move(requestInfo),
        std::move(replicaIds),
        TActorId{},   // followerPartition
        std::move(request),
        std::move(diskId),
        parentActorId,
        nonreplicatedRequestCounter);
}

IActorPtr CreateMirrorRequestActor(
    TRequestInfoPtr requestInfo,
    TVector<TActorId> replicaIds,
    NProto::TZeroBlocksRequest request,
    TString diskId,
    TActorId parentActorId,
    ui64 nonreplicatedRequestCounter)
{
    return std::make_unique<
        TMirrorRequestActor<TEvService::TZeroBlocksMethod>>(
        std::move(requestInfo),
        std::move(replicaIds),
        TActorId{},   // followerPartition
        std::move(request),
        std::move(diskId),
        parentActorId,
        nonreplicatedRequestCounter);
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateDeviceMigrationRequestActor(
    TRequestInfoPtr requestInfo,
    TActorId srcActorId,
    TActorId dstActorId,
    NProto::TWriteBlocksRequest request,
    TString diskId,
    TActorId parentActorId,
    ui64 nonreplicatedRequestCounter)
{
    return CreateDeviceMigrationRequestActorImpl<
        TEvService::TWriteBlocksMethod>(
        std::move(requestInfo),
        srcActorId,
        dstActorId,
        std::move(request),
        std::move(diskId),
        parentActorId,
        nonreplicatedRequestCounter);
}

IActorPtr CreateDeviceMigrationRequestActor(
    TRequestInfoPtr requestInfo,
    TActorId srcActorId,
    TActorId dstActorId,
    NProto::TWriteBlocksLocalRequest request,
    TString diskId,
    TActorId parentActorId,
    ui64 nonreplicatedRequestCounter)
{
    return CreateDeviceMigrationRequestActorImpl<
        TEvService::TWriteBlocksLocalMethod>(
        std::move(requestInfo),
        srcActorId,
        dstActorId,
        std::move(request),
        std::move(diskId),
        parentActorId,
        nonreplicatedRequestCounter);
}

IActorPtr CreateDeviceMigrationRequestActor(
    TRequestInfoPtr requestInfo,
    TActorId srcActorId,
    TActorId dstActorId,
    NProto::TZeroBlocksRequest request,
    TString diskId,
    TActorId parentActorId,
    ui64 nonreplicatedRequestCounter)
{
    return CreateDeviceMigrationRequestActorImpl<TEvService::TZeroBlocksMethod>(
        std::move(requestInfo),
        srcActorId,
        dstActorId,
        std::move(request),
        std::move(diskId),
        parentActorId,
        nonreplicatedRequestCounter);
}

}   // namespace NCloud::NBlockStore::NStorage
