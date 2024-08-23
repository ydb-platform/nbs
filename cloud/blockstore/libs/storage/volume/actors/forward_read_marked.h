#pragma once

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/system/types.h>

#include <bitset>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
class TReadMarkedActor final
    : public NActors::TActorBootstrapped<TReadMarkedActor<TMethod>>
{
private:
    using TActorId = NActors::TActorId;
    using TActorContext = NActors::TActorContext;

    const TRequestInfoPtr RequestInfo;
    typename TMethod::TRequest::ProtoRecordType Request;
    NBlobMarkers::TBlockMarks BlockMarks;
    const bool MaskUnusedBlocks;
    const bool ReplyWithUnencryptedBlockMask;
    const TActorId PartActorId;
    const ui64 VolumeTabletId;
    const TActorId VolumeActorId;

    typename TMethod::TResponse::ProtoRecordType Record;

    using TBase = NActors::TActorBootstrapped<TReadMarkedActor<TMethod>>;

public:
    TReadMarkedActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        const TCompressedBitmap& usedBlocks,
        bool maskUnusedBlocks,
        bool replyWithUnencryptedBlockMask,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId);

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
TReadMarkedActor<TMethod>::TReadMarkedActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        const TCompressedBitmap& usedBlocks,
        bool maskUnusedBlocks,
        bool replyWithUnencryptedBlockMask,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , BlockMarks(MakeBlockMarks(
        usedBlocks,
        TBlockRange64::WithLength(
            Request.GetStartIndex(),
            Request.GetBlocksCount())))
    , MaskUnusedBlocks(maskUnusedBlocks)
    , ReplyWithUnencryptedBlockMask(replyWithUnencryptedBlockMask)
    , PartActorId(partActorId)
    , VolumeTabletId(volumeTabletId)
    , VolumeActorId(volumeActorId)
{}

template <ReadRequest TMethod>
void TReadMarkedActor<TMethod>::Bootstrap(const TActorContext& ctx)
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
void TReadMarkedActor<TMethod>::ReadBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record = Request;

    auto event = std::make_unique<NActors::IEventHandle>(
        PartActorId,
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        0,  // cookie
        &ctx.SelfID);    // forwardOnNondelivery

    ctx.Send(std::move(event));
}

template <ReadRequest TMethod>
void TReadMarkedActor<TMethod>::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Record);

    if (MaskUnusedBlocks) {
        ApplyMask(BlockMarks, Request);
        ApplyMask(BlockMarks, response->Record);
    }

    if (ReplyWithUnencryptedBlockMask) {
        FillUnencryptedBlockMask(BlockMarks, response->Record);
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
void TReadMarkedActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] %s request undelivered to partition",
        VolumeTabletId,
        TMethod::Name);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " request undelivered to partition"));

    Done(ctx);
}

template <ReadRequest TMethod>
void TReadMarkedActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record)) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s got error from partition: %s",
            VolumeTabletId,
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());
    }

    Record = std::move(msg->Record);

    Done(ctx);
}

template <ReadRequest TMethod>
void TReadMarkedActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));
    Done(ctx);
}

template <ReadRequest TMethod>
STFUNC(TReadMarkedActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
