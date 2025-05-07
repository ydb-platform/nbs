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

// The TMirrorRequestActor class is used to write to N replicas for the mirror
// disk. It should process all errors from replicas into E_REJECTED. Because
// sooner or later the disk agent will return to work, or the replica will be
// replaced. All replicas are equivalent and are placed in the Replicas.

template <typename TMethod>
class TMirrorRequestActor final
    : public NActors::TActorBootstrapped<TMirrorRequestActor<TMethod>>
{
private:
    using TBase = NActors::TActorBootstrapped<TMirrorRequestActor<TMethod>>;
    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;

    const TRequestInfoPtr RequestInfo;
    const TVector<NActors::TActorId> Replicas;
    const typename TMethod::TRequest::ProtoRecordType Request;
    const TString DiskId;
    const NActors::TActorId ParentActorId;
    const ui64 NonreplicatedRequestCounter;

    TVector<TCallContextPtr> ForkedCallContexts;
    ui32 Responses = 0;
    TResponseProto ReplicasCollectiveResponse;

public:
    TMirrorRequestActor(
        TRequestInfoPtr requestInfo,
        TVector<NActors::TActorId> replicas,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendRequests(const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);
    void UpdateResponse(TResponseProto&& response);

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
        TVector<NActors::TActorId> replicas,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter)
    : RequestInfo(std::move(requestInfo))
    , Replicas(std::move(replicas))
    , Request(std::move(request))
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , NonreplicatedRequestCounter(nonreplicatedRequestCounter)
{
    Y_DEBUG_ABORT_UNLESS(!Replicas.empty());
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
    for (const auto& actorId: Replicas) {
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
    }
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::Done(const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(ReplicasCollectiveResponse);

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
            false   // FollowerGotNonRetriableError
        );
    NCloud::Send(ctx, ParentActorId, std::move(completion));

    TBase::Die(ctx);
}

template <typename TMethod>
void TMirrorRequestActor<TMethod>::UpdateResponse(TResponseProto&& response)
{
    ProcessMirrorActorError(*response.MutableError());

    if (!HasError(ReplicasCollectiveResponse)) {
        ReplicasCollectiveResponse = std::move(response);
    }
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

    *ReplicasCollectiveResponse.MutableError() = MakeError(
        E_REJECTED,
        TStringBuilder() << TMethod::Name
                         << " request undelivered to some nonrepl partitions");

    if (++Responses < Replicas.size()) {
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

    UpdateResponse(std::move(msg->Record));

    if (++Responses < Replicas.size()) {
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

    *ReplicasCollectiveResponse.MutableError() = MakeError(E_REJECTED, "Dead");

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
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
