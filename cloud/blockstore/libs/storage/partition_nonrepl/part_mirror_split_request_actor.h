#pragma once

#include "part_mirror_split_request_helpers.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage::NSplitRequest {

template <typename TMethod>
class TSplittedRequestActor final
    : public NActors::TActorBootstrapped<TSplittedRequestActor<TMethod>>
{
private:
    using TRequestRecordType = TMethod::TRequest::ProtoRecordType;
    using TResponseRecordType = TMethod::TResponse::ProtoRecordType;

private:
    const TRequestInfoPtr RequestInfo;
    TSplittedRequest<TMethod> Requests;
    const NActors::TActorId ParentActorId;
    const ui64 BlockSize;
    const ui64 RequestIdentityKey;

    ui32 PendingRequests = 0;
    TVector<TUnifyResponsesContext<TMethod>> Responses;

    using TResponseProto = typename TMethod::TResponse::ProtoRecordType;
    using TBase = NActors::TActorBootstrapped<TSplittedRequestActor<TMethod>>;

public:
    TSplittedRequestActor(
            TRequestInfoPtr requestInfo,
            TSplittedRequest<TMethod> requests,
            NActors::TActorId parentActorId,
            ui64 blockSize,
            ui64 requestIdentityKey)
        : RequestInfo(std::move(requestInfo))
        , Requests(std::move(requests))
        , ParentActorId(parentActorId)
        , BlockSize(blockSize)
        , RequestIdentityKey(requestIdentityKey)
    {}

    void Bootstrap(const NActors::TActorContext& ctx)
    {
        Responses.resize(Requests.size());
        for (size_t i = 0; i < Requests.size(); ++i) {
            Responses[i].BlocksCountRequested =
                Requests[i].BlockRangeForRequest.Size();
            auto req = std::make_unique<typename TMethod::TRequest>();
            req->Record = std::move(Requests[i].Request);
            NCloud::Send(
                ctx,
                ParentActorId,
                std::move(req),
                RequestInfo->Cookie + i + 1);
            ++PendingRequests;
        }

        TBase::Become(&TBase::TThis::StateWork);
    }

private:
    void ReplyAndDie(const NActors::TActorContext& ctx, NProto::TError error)
    {
        auto response =
            std::make_unique<typename TMethod::TResponse>(std::move(error));
        if (!HasError(response->GetError())) {
            response->Record = UnifyResponses<TMethod>(Responses, BlockSize);
        }

        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvMirroredReadCompleted>(
            RequestIdentityKey,
            false
            );
        NCloud::Send(ctx, ParentActorId, std::move(completion));

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        TBase::Die(ctx);
    }

    void OnActorResponse(
        const TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto* msg = ev->Get();
        if (HasError(msg->GetError())) {
            ReplyAndDie(ctx, std::move(msg->GetError()));
            return;
        }

        auto responseIdx = ev->Cookie - RequestInfo->Cookie - 1;
        Responses[responseIdx].Response = std::move(msg->Record);

        if (--PendingRequests == 0) {
            ReplyAndDie(ctx, {});
        }
    }

private:
    STFUNC(StateWork)
    {
        TRequestScope timer(*RequestInfo);

        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TMethod::TResponse, HandleActorResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION_WORKER);
                break;
        }
    }

    void HandleActorResponse(
        const TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        OnActorResponse(ev, ctx);
    }

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NSplitRequest
