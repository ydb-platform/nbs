#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>

#include <util/system/types.h>

#include <bitset>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

template<class TMethod>
concept ReadRequest = IsReadMethod<TMethod>;

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
class TReadActor final
    : public TActorBootstrapped<TReadActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    typename TMethod::TRequest::ProtoRecordType Request;
    const THashSet<ui64> UnusedBlockIndices;
    bool MaskUnusedBlocks = false;
    bool ReplyUnencryptedBlockMask = false;
    const TActorId PartActorId;
    const ui64 VolumeTabletId;
    const TActorId VolumeActorId;

    typename TMethod::TResponse::ProtoRecordType Record;

    using TBase = TActorBootstrapped<TReadActor<TMethod>>;

public:
    TReadActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        THashSet<ui64> unusedIndices,
        bool maskUnusedBlocks,
        bool replyUnencryptedBlockMask,
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
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
TReadActor<TMethod>::TReadActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        THashSet<ui64> unusedIndices,
        bool maskUnusedBlocks,
        bool replyUnencryptedBlockMask,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , UnusedBlockIndices(std::move(unusedIndices))
    , MaskUnusedBlocks(maskUnusedBlocks)
    , ReplyUnencryptedBlockMask(replyUnencryptedBlockMask)
    , PartActorId(partActorId)
    , VolumeTabletId(volumeTabletId)
    , VolumeActorId(volumeActorId)
{
    TBase::ActivityType = TBlockStoreActivities::VOLUME;
}

template <ReadRequest TMethod>
void TReadActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    ReadBlocks(ctx);
}

template <ReadRequest TMethod>
void TReadActor<TMethod>::ReadBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record = Request;

    auto event = std::make_unique<IEventHandle>(
        PartActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,  // cookie
        &ctx.SelfID);    // forwardOnNondelivery

    ctx.Send(std::move(event));
}

template <ReadRequest TMethod>
void TReadActor<TMethod>::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Record);

    if (MaskUnusedBlocks) {
        ApplyMask(UnusedBlockIndices, GetStartIndex(Request), Request);
        ApplyMask(
            UnusedBlockIndices,
            GetStartIndex(Request),
            response->Record);
    }

    if (ReplyUnencryptedBlockMask) {
        FillUnencryptedBlockMask(
            UnusedBlockIndices,
            GetStartIndex(Request),
            response->Record);
    }

    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
void TReadActor<TMethod>::HandleUndelivery(
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
void TReadActor<TMethod>::HandleResponse(
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
void TReadActor<TMethod>::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));
    Done(ctx);
}

template <ReadRequest TMethod>
STFUNC(TReadActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
