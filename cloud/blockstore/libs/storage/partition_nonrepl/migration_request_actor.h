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

// The TMigrationRequestActor class is used to migrate a new device. It has two
// replicas, one is leader and the second is follower. If we receive a fatal
// error from the follower partition, we do not respond to the client with an
// error, but at the same time stop the migration. And if we receive a non-fatal
// error, response it to the client. The client will retry the request.

template <typename TMethod>
class TMigrationRequestActor final
    : public NActors::TActorBootstrapped<TMigrationRequestActor<TMethod>>
{
    static constexpr ui64 LeaderCookie = 1;
    static constexpr ui64 FollowerCookie = 2;

private:
    using TBase = NActors::TActorBootstrapped<TMigrationRequestActor<TMethod>>;
    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;

    const TRequestInfoPtr RequestInfo;
    const NActors::TActorId LeaderPartition;
    const NActors::TActorId FollowerPartition;
    const typename TMethod::TRequest::ProtoRecordType Request;
    const TString DiskId;
    const NActors::TActorId ParentActorId;
    const ui64 NonreplicatedRequestCounter;

    TVector<TCallContextPtr> ForkedCallContexts;
    ui32 PendingRequests = 0;
    TResponseProto LeaderResponse;
    TResponseProto FollowerResponse;

public:
    TMigrationRequestActor(
        TRequestInfoPtr requestInfo,
        NActors::TActorId leaderPartition,  // may be empty
        NActors::TActorId followerPartition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendRequest(
        const NActors::TActorContext& ctx,
        const NActors::TActorId& recipient,
        ui64 cookie);
    void Done(const NActors::TActorContext& ctx);

    void UpdateResponse(
        const NActors::TActorId& sender,
        TResponseProto&& response);

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
TMigrationRequestActor<TMethod>::TMigrationRequestActor(
        TRequestInfoPtr requestInfo,
        NActors::TActorId leaderPartition,
        NActors::TActorId followerPartition,
        typename TMethod::TRequest::ProtoRecordType request,
        TString diskId,
        NActors::TActorId parentActorId,
        ui64 nonreplicatedRequestCounter)
    : RequestInfo(std::move(requestInfo))
    , LeaderPartition(leaderPartition)
    , FollowerPartition(followerPartition)
    , Request(std::move(request))
    , DiskId(std::move(diskId))
    , ParentActorId(parentActorId)
    , NonreplicatedRequestCounter(nonreplicatedRequestCounter)
{
    Y_DEBUG_ABORT_UNLESS(FollowerPartition);
}

template <typename TMethod>
void TMigrationRequestActor<TMethod>::Bootstrap(const NActors::TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    PendingRequests = 1;

    if (LeaderPartition) {
        PendingRequests = 2;
        SendRequest(ctx, LeaderPartition, LeaderCookie);
    }

    SendRequest(ctx, FollowerPartition, FollowerCookie);
}

template <typename TMethod>
void TMigrationRequestActor<TMethod>::SendRequest(
    const NActors::TActorContext& ctx,
    const NActors::TActorId& recipient,
    ui64 cookie)
{
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
        recipient,
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        cookie,
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(std::move(event));
}

template <typename TMethod>
void TMigrationRequestActor<TMethod>::Done(const NActors::TActorContext& ctx)
{
    const bool isFollowerResponseError = HasError(FollowerResponse);
    const bool isFollowerResponseFatal =
        isFollowerResponseError &&
        GetErrorKind(FollowerResponse.GetError()) == EErrorKind::ErrorFatal;

    auto response = std::make_unique<typename TMethod::TResponse>();

    if (isFollowerResponseError && !isFollowerResponseFatal) {
        response->Record = std::move(FollowerResponse);
    } else {
        response->Record = std::move(LeaderResponse);
    }

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

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMigrationRequestActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(
        E_REJECTED,
        TStringBuilder() << TMethod::Name << " request undelivered to "
                         << (ev->Cookie == LeaderCookie ? "leader" : "follower")
                         << " nonrepl partitions");

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "[%s] %s",
        DiskId.c_str(),
        error.GetMessage().c_str());

    switch (ev->Cookie) {
        case LeaderCookie:
            *LeaderResponse.MutableError() = std::move(error);
            break;
        case FollowerCookie:
            *FollowerResponse.MutableError() = std::move(error);
            break;
        default: {
            auto message = ReportUnexpectedCookie(
                TStringBuilder() << __PRETTY_FUNCTION__ << " #"
                                 << RequestInfo->CallContext->RequestId
                                 << " DiskId: " << DiskId.Quote() << " "
                                 << " Cookie: " << ev->Cookie);

            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER, message);
            TBase::Die(ctx);
            return;
        }
    }

    if (--PendingRequests) {
        return;
    }

    Done(ctx);
}

template <typename TMethod>
void TMigrationRequestActor<TMethod>::HandleResponse(
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

    switch (ev->Cookie) {
        case LeaderCookie:
            LeaderResponse = std::move(msg->Record);
            break;
        case FollowerCookie:
            FollowerResponse = std::move(msg->Record);
            break;
        default: {
            auto message = ReportUnexpectedCookie(
                TStringBuilder() << __PRETTY_FUNCTION__ << " #"
                                 << RequestInfo->CallContext->RequestId
                                 << " DiskId: " << DiskId.Quote() << " "
                                 << " Cookie: " << ev->Cookie);

            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER, message);

            TBase::Die(ctx);

            return;
        }
    }

    if (--PendingRequests) {
        return;
    }

    Done(ctx);
}

template <typename TMethod>
void TMigrationRequestActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LeaderResponse.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));

    Done(ctx);
}

template <typename TMethod>
STFUNC(TMigrationRequestActor<TMethod>::StateWork)
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
