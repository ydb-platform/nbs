#pragma once

#include "part_mirror_split_request_helpers.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TSplitReadBlocksActor final
    : public NActors::TActorBootstrapped<TSplitReadBlocksActor<TMethod>>
{
private:
    using TRequestProtoType = TMethod::TRequest::ProtoRecordType;
    using TResponseProtoType = TMethod::TResponse::ProtoRecordType;

private:
    const TRequestInfoPtr RequestInfo;
    TVector<TRequestProtoType> Requests;
    const NActors::TActorId ParentActorId;
    const ui64 BlockSize;
    const ui64 RequestIdentityKey;

    ui32 PendingRequests = 0;
    TVector<TResponseProtoType> Responses;

    using TBase = NActors::TActorBootstrapped<TSplitReadBlocksActor<TMethod>>;

public:
    TSplitReadBlocksActor(
            TRequestInfoPtr requestInfo,
            TVector<TRequestProtoType> requests,
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
            auto req = std::make_unique<typename TMethod::TRequest>();
            req->Record = std::move(Requests[i]);
            NCloud::Send(ctx, ParentActorId, std::move(req), i);
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
            response->Record = MergeReadResponses(Responses);
        }

        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvMirroredReadCompleted>(
            RequestIdentityKey,
            false);
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
            ReplyAndDie(ctx, msg->GetError());
            return;
        }

        const auto responseIdx = ev->Cookie;
        Responses[responseIdx] = std::move(msg->Record);

        if (--PendingRequests == 0) {
            ReplyAndDie(ctx, {});
        }
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TMethod::TResponse, HandleActorResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION_WORKER,
                    __PRETTY_FUNCTION__);
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

}   // namespace NCloud::NBlockStore::NStorage
