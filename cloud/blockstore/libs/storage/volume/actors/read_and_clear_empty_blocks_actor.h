#pragma once

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <util/system/types.h>

#include <bitset>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
class TReadAndClearEmptyBlocksActor final
    : public NActors::TActorBootstrapped<TReadAndClearEmptyBlocksActor<TMethod>>
{
private:
    using TBase =
        NActors::TActorBootstrapped<TReadAndClearEmptyBlocksActor<TMethod>>;
    using TActorId = NActors::TActorId;
    using TActorContext = NActors::TActorContext;

    typename TMethod::TRequest::ProtoRecordType Request;
    typename TMethod::TResponse::ProtoRecordType Response;

    const TRequestInfoPtr RequestInfo;
    const NBlobMarkers::TBlockMarks UsedBlocks;
    const TActorId PartActorId;
    const ui64 VolumeTabletId;
    const TActorId VolumeActorId;
    TChildLogTitle LogTitle;

public:
    TReadAndClearEmptyBlocksActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        const TCompressedBitmap& usedBlocks,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId,
        TChildLogTitle logTitle);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReadBlocks(const TActorContext& ctx);

    bool HandleError(const TActorContext& ctx, NProto::TError error);

    void Done(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
TReadAndClearEmptyBlocksActor<TMethod>::TReadAndClearEmptyBlocksActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        const TCompressedBitmap& usedBlocks,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId,
        TChildLogTitle logTitle)
    : Request(std::move(request))
    , RequestInfo(std::move(requestInfo))
    , UsedBlocks(MakeUsedBlockMarks(
          usedBlocks,
          TBlockRange64::WithLength(
              Request.GetStartIndex(),
              Request.GetBlocksCount())))
    , PartActorId(partActorId)
    , VolumeTabletId(volumeTabletId)
    , VolumeActorId(volumeActorId)
    , LogTitle(std::move(logTitle))
{}

template <ReadRequest TMethod>
void TReadAndClearEmptyBlocksActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    GLOBAL_LWTRACK(
        BLOCKSTORE_STORAGE_PROVIDER,
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    ReadBlocks(ctx);
}

template <ReadRequest TMethod>
void TReadAndClearEmptyBlocksActor<TMethod>::ReadBlocks(
    const TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record = Request;

    auto event = std::make_unique<NActors::IEventHandle>(
        PartActorId,
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        0,              // cookie
        &ctx.SelfID);   // forwardOnNondelivery

    ctx.Send(std::move(event));
}

template <ReadRequest TMethod>
void TReadAndClearEmptyBlocksActor<TMethod>::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Response);

    if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>) {
        ClearEmptyBlocks(UsedBlocks, Request.Sglist);
    } else {
        ClearEmptyBlocks(UsedBlocks, response->Record);
    }

    GLOBAL_LWTRACK(
        BLOCKSTORE_STORAGE_PROVIDER,
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
void TReadAndClearEmptyBlocksActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s %s request undelivered to partition",
        LogTitle.GetWithTime().c_str(),
        TMethod::Name);

    Response.MutableError()->CopyFrom(MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " request undelivered to partition"));

    Done(ctx);
}

template <ReadRequest TMethod>
void TReadAndClearEmptyBlocksActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s %s got error from partition: %s",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());
    }

    Response = std::move(msg->Record);

    Done(ctx);
}

template <ReadRequest TMethod>
void TReadAndClearEmptyBlocksActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Response.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));
    Done(ctx);
}

template <ReadRequest TMethod>
STFUNC(TReadAndClearEmptyBlocksActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
