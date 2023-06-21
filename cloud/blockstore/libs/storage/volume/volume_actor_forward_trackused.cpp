#include "volume_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>

#include <cloud/storage/core/libs/common/media.h>

#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TWriteAndMarkUsedActor final
    : public TActorBootstrapped<TWriteAndMarkUsedActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    const typename TMethod::TRequest::ProtoRecordType Request;
    const ui32 BlockSize;
    const bool ShouldWriteAndMarkSerially;
    const ui64 VolumeRequestId;
    const TActorId PartActorId;
    const ui64 VolumeTabletId;
    const TActorId VolumeActorId;

    typename TMethod::TResponse::ProtoRecordType Record;
    bool ResponseReceived = false;
    bool MarkedUsed = false;

    using TBase = TActorBootstrapped<TWriteAndMarkUsedActor<TMethod>>;

public:
    TWriteAndMarkUsedActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        ui32 blockSize,
        bool shouldWriteAndMarkSerially,
        ui64 volumeRequestId,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId);

    void Bootstrap(const TActorContext& ctx);

private:
    void WriteBlocks(const TActorContext& ctx);
    void MarkBlocksUsed(const TActorContext& ctx);

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
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TWriteAndMarkUsedActor<TMethod>::TWriteAndMarkUsedActor(
        TRequestInfoPtr requestInfo,
        typename TMethod::TRequest::ProtoRecordType request,
        ui32 blockSize,
        bool shouldWriteAndMarkSerially,
        ui64 volumeRequestId,
        TActorId partActorId,
        ui64 volumeTabletId,
        TActorId volumeActorId)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , BlockSize(blockSize)
    , ShouldWriteAndMarkSerially(shouldWriteAndMarkSerially)
    , VolumeRequestId(volumeRequestId)
    , PartActorId(partActorId)
    , VolumeTabletId(volumeTabletId)
    , VolumeActorId(volumeActorId)
{
    TBase::ActivityType = TBlockStoreActivities::VOLUME;
}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    WriteBlocks(ctx);
    if (!ShouldWriteAndMarkSerially) {
        MarkBlocksUsed(ctx);
    }
}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::WriteBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record = Request;

    TAutoPtr<IEventHandle> event(
        new IEventHandle(
            PartActorId,
            ctx.SelfID,
            request.get(),
            IEventHandle::FlagForwardOnNondelivery,
            0,  // cookie
            &ctx.SelfID    // forwardOnNondelivery
        )
    );
    request.release();

    ctx.Send(event);
}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::MarkBlocksUsed(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvUpdateUsedBlocksRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record.AddStartIndices(GetStartIndex(Request));
    request->Record.AddBlockCounts(CalculateWriteRequestBlockCount(
        Request, BlockSize));
    request->Record.SetUsed(true);

    TAutoPtr<IEventHandle> event(
        new IEventHandle(
            VolumeActorId,
            ctx.SelfID,
            request.get(),
            IEventHandle::FlagForwardOnNondelivery,
            0,              // cookie
            &ctx.SelfID     // forwardOnNondelivery
        ));
    request.release();

    ctx.Send(event);
}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Record);

    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    using TEvent = TEvVolumePrivate::TEvWriteOrZeroCompleted;
    auto ev = std::make_unique<TEvent>(
        VolumeRequestId,
        response->Record.GetError().GetCode());

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        VolumeActorId,
        std::move(ev)
    );

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleUndelivery(
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

    ResponseReceived = true;

    if (ResponseReceived && MarkedUsed) {
        Done(ctx);
    }

    if (ShouldWriteAndMarkSerially) {
        Done(ctx);
    }

}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleResponse(
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

    if (!HasError(Record)) {
        Record = std::move(msg->Record);
    }

    ResponseReceived = true;

    if (ResponseReceived && MarkedUsed) {
        Done(ctx);
    }

    if (ShouldWriteAndMarkSerially) {
        MarkBlocksUsed(ctx);
    }
}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleUpdateUsedBlocksResponse(
    const TEvVolume::TEvUpdateUsedBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record)) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s failed to update used blocks: %s",
            VolumeTabletId,
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());

        *Record.MutableError() = msg->Record.GetError();
    }

    MarkedUsed = true;

    if (ResponseReceived && MarkedUsed) {
        Done(ctx);
    }
}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandleUpdateUsedBlocksUndelivery(
    const TEvVolume::TEvUpdateUsedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, TStringBuilder()
        << TMethod::Name << " used blocks update undelivered"));

    LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] %s failed to update used blocks: %s",
        VolumeTabletId,
        TMethod::Name,
        FormatError(Record.GetError()).c_str());

    MarkedUsed = true;

    if (ResponseReceived && MarkedUsed) {
        Done(ctx);
    }
}

template <typename TMethod>
void TWriteAndMarkUsedActor<TMethod>::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));
    Done(ctx);
}

template <typename TMethod>
STFUNC(TWriteAndMarkUsedActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        HFunc(
            TEvVolume::TEvUpdateUsedBlocksRequest,
            HandleUpdateUsedBlocksUndelivery);
        HFunc(
            TEvVolume::TEvUpdateUsedBlocksResponse,
            HandleUpdateUsedBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
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

template <typename TMethod>
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

template <typename TMethod>
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

template <typename TMethod>
void TReadActor<TMethod>::ReadBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->CallContext = RequestInfo->CallContext;
    request->Record = Request;

    TAutoPtr<IEventHandle> event(
        new IEventHandle(
            PartActorId,
            ctx.SelfID,
            request.get(),
            IEventHandle::FlagForwardOnNondelivery,
            0,  // cookie
            &ctx.SelfID    // forwardOnNondelivery
        )
    );
    request.release();

    ctx.Send(event);
}

template <typename TMethod>
void TReadActor<TMethod>::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<typename TMethod::TResponse>();
    response->Record = std::move(Record);

    if constexpr (IsReadMethod<TMethod>) {
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

template <typename TMethod>
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

template <typename TMethod>
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

template <typename TMethod>
void TReadActor<TMethod>::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Record.MutableError()->CopyFrom(MakeError(E_REJECTED, "Dead"));
    Done(ctx);
}

template <typename TMethod>
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
bool TVolumeActor::SendRequestToPartitionWithUsedBlockTracking(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    const TActorId& partActorId)
{
    const auto* msg = ev->Get();

    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();
    auto encryptedNonreplicatedVolume =
        IsDiskRegistryMediaKind(State->GetConfig().GetStorageMediaKind()) &&
        volumeConfig.GetEncryptionDesc().GetMode() != NProto::NO_ENCRYPTION;

    if constexpr (IsWriteMethod<TMethod>) {
        if (State->GetTrackUsedBlocks()) {
            auto requestInfo =
                CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

            NCloud::Register<TWriteAndMarkUsedActor<TMethod>>(
                ctx,
                std::move(requestInfo),
                std::move(msg->Record),
                State->GetBlockSize(),
                encryptedNonreplicatedVolume,
                VolumeRequestId,
                partActorId,
                TabletID(),
                SelfId());

            return true;
        }
    }

    if constexpr (IsReadMethod<TMethod>) {
        if (State->GetMaskUnusedBlocks() && State->GetUsedBlocks() ||
            encryptedNonreplicatedVolume)
        {
            THashSet<ui64> unusedIndices;

            FillUnusedIndices(
                msg->Record,
                State->GetUsedBlocks(),
                &unusedIndices);

            if (unusedIndices) {
                auto requestInfo =
                    CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

                NCloud::Register<TReadActor<TMethod>>(
                    ctx,
                    std::move(requestInfo),
                    std::move(msg->Record),
                    std::move(unusedIndices),
                    State->GetMaskUnusedBlocks(),
                    encryptedNonreplicatedVolume,
                    partActorId,
                    TabletID(),
                    SelfId());

                return true;
            }
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

#define GENERATE_IMPL(name, ns)                                                \
template bool TVolumeActor::SendRequestToPartitionWithUsedBlockTracking<       \
    ns::T##name##Method>(                                                      \
        const TActorContext& ctx,                                              \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorId& partActorId);                                          \
// GENERATE_IMPL

GENERATE_IMPL(ReadBlocks,         TEvService)
GENERATE_IMPL(WriteBlocks,        TEvService)
GENERATE_IMPL(ZeroBlocks,         TEvService)
GENERATE_IMPL(CreateCheckpoint,   TEvService)
GENERATE_IMPL(DeleteCheckpoint,   TEvService)
GENERATE_IMPL(GetChangedBlocks,   TEvService)
GENERATE_IMPL(ReadBlocksLocal,    TEvService)
GENERATE_IMPL(WriteBlocksLocal,   TEvService)

GENERATE_IMPL(DescribeBlocks,           TEvVolume)
GENERATE_IMPL(GetUsedBlocks,            TEvVolume)
GENERATE_IMPL(GetPartitionInfo,         TEvVolume)
GENERATE_IMPL(CompactRange,             TEvVolume)
GENERATE_IMPL(GetCompactionStatus,      TEvVolume)
GENERATE_IMPL(DeleteCheckpointData,     TEvVolume)
GENERATE_IMPL(RebuildMetadata,          TEvVolume)
GENERATE_IMPL(GetRebuildMetadataStatus, TEvVolume)
GENERATE_IMPL(ScanDisk,                 TEvVolume)
GENERATE_IMPL(GetScanDiskStatus,        TEvVolume)

#undef GENERATE_IMPL

}   // namespace NCloud::NBlockStore::NStorage
