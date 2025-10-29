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

/*
  TWriteAndMarkUsedActor does the writing and saving of indexes of written blocks.
  The saving of indexes can be parallel or consistent.
  The request is completed only if writing and saving indexes are completed.
*/
template <WriteRequest TMethod>
class TWriteAndMarkUsedActor final
    : public NActors::TActorBootstrapped<TWriteAndMarkUsedActor<TMethod>>
{
private:
    using TActorId = NActors::TActorId;
    using TActorContext = NActors::TActorContext;

    enum EStatusFlags
    {
        WriteRequestSent,
        WriteResponseReceived,
        UsedBlockRequestSent,
        UsedBlockResponseReceived,
        //-------------------------
        Count
    };

    const TRequestInfoPtr RequestInfo;
    const typename TMethod::TRequest::ProtoRecordType Request;
    const ui32 BlockSize;
    const bool WriteUsedBlockConsistently;
    const ui64 VolumeRequestId;
    const TActorId PartActorId;
    const ui64 VolumeTabletId;
    const TActorId VolumeActorId;
    TChildLogTitle LogTitle;

    std::bitset<Count> State;
    typename TMethod::TResponse::ProtoRecordType Record;

    using TBase = NActors::TActorBootstrapped<TWriteAndMarkUsedActor<TMethod>>;

public:
    TWriteAndMarkUsedActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        ui32 blockSize,
        bool writeUsedBlockConsistently,
        ui64 volumeRequestId,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId,
        TChildLogTitle logTitle);

    void Bootstrap(const TActorContext& ctx);

private:
    void WriteBlocks(const TActorContext& ctx);
    void MarkUsedBlocks(const TActorContext& ctx);

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

    void HandleUpdateUsedBlocksResponse(
        const TEvVolume::TEvUpdateUsedBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateUsedBlocksUndelivery(
        const TEvVolume::TEvUpdateUsedBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <WriteRequest TMethod>
TWriteAndMarkUsedActor<TMethod>::TWriteAndMarkUsedActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        ui32 blockSize,
        bool writeUsedBlockConsistently,
        ui64 volumeRequestId,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId,
        TChildLogTitle logTitle)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , BlockSize(blockSize)
    , WriteUsedBlockConsistently(writeUsedBlockConsistently)
    , VolumeRequestId(volumeRequestId)
    , PartActorId(partActorId)
    , VolumeTabletId(volumeTabletId)
    , VolumeActorId(volumeActorId)
    , LogTitle(std::move(logTitle))
{}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    GLOBAL_LWTRACK(
        BLOCKSTORE_STORAGE_PROVIDER,
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    WriteBlocks(ctx);
    if (!WriteUsedBlockConsistently) {
        MarkUsedBlocks(ctx);
    }
}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::WriteBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record = Request;

    auto event = std::make_unique<NActors::IEventHandle>(
        PartActorId,
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        VolumeRequestId, // cookie
        &ctx.SelfID      // forwardOnNondelivery
    );
    ctx.Send(std::move(event));
    State.set(WriteRequestSent);
}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::MarkUsedBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvUpdateUsedBlocksRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record.AddStartIndices(GetStartIndex(Request));
    request->Record.AddBlockCounts(CalculateWriteRequestBlockCount(
        Request, BlockSize));
    request->Record.SetUsed(true);

    auto event = std::make_unique<NActors::IEventHandle>(
        VolumeActorId,
        ctx.SelfID,
        request.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,
        VolumeRequestId, // cookie
        &ctx.SelfID      // forwardOnNondelivery
    );
    ctx.Send(std::move(event));
    State.set(UsedBlockRequestSent);
}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::Done(const TActorContext& ctx)
{
    if (!(HasError(Record) || State.all())) {
        return;
    }

    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Record);

    GLOBAL_LWTRACK(
        BLOCKSTORE_STORAGE_PROVIDER,
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    auto ev = std::make_unique<TEvVolumePrivate::TEvWriteOrZeroCompleted>(
        VolumeRequestId,
        response->Record.GetError().GetCode());

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    NCloud::Send(ctx, VolumeActorId, std::move(ev));
    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(ev->Cookie == VolumeRequestId);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s %s request undelivered to partition",
        LogTitle.GetWithTime().c_str(),
        TMethod::Name);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " request undelivered to partition"));

    State.set(WriteResponseReceived);
    Done(ctx);
}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(ev->Cookie == VolumeRequestId);

    auto* msg = ev->Get();
    const bool error = HasError(msg->Record);

    if (error) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s %s got error from partition: %s",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());
    }

    if (!HasError(Record)) {
        Record = std::move(msg->Record);
    }

    if (WriteUsedBlockConsistently && !error) {
        MarkUsedBlocks(ctx);
    }

    State.set(WriteResponseReceived);
    Done(ctx);
}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleUpdateUsedBlocksResponse(
    const TEvVolume::TEvUpdateUsedBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(ev->Cookie == VolumeRequestId);

    auto* msg = ev->Get();

    if (HasError(msg->Record)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s %s failed to update used blocks: %s",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());

        *Record.MutableError() = msg->Record.GetError();
    }

    State.set(UsedBlockResponseReceived);
    Done(ctx);
}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleUpdateUsedBlocksUndelivery(
    const TEvVolume::TEvUpdateUsedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(ev->Cookie == VolumeRequestId);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " used blocks update undelivered"));

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s %s failed to update used blocks: %s",
        LogTitle.GetWithTime().c_str(),
        TMethod::Name,
        FormatError(Record.GetError()).c_str());

    State.set(UsedBlockResponseReceived);
    Done(ctx);
}

template <WriteRequest TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));
    Done(ctx);
}

template <WriteRequest TMethod>
STFUNC(TWriteAndMarkUsedActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        HFunc(
            TEvVolume::TEvUpdateUsedBlocksRequest,
            HandleUpdateUsedBlocksUndelivery);
        HFunc(
            TEvVolume::TEvUpdateUsedBlocksResponse,
            HandleUpdateUsedBlocksResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
