#include "volume_as_partition_actor.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TVolumeAsPartitionActor::TVolumeAsPartitionActor(
        TChildLogTitle logTitle,
        ui32 originalBlockSize,
        TString diskId)
    : LogTitle(std::move(logTitle))
    , OriginalBlockSize(originalBlockSize)
    , DiskId(std::move(diskId))
{}

TVolumeAsPartitionActor::~TVolumeAsPartitionActor() = default;

void TVolumeAsPartitionActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s VolumeAsPartitionActor created",
        LogTitle.GetWithTime().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId));
}

bool TVolumeAsPartitionActor::CheckRange(TBlockRange64 range) const
{
    return TBlockRange64::WithLength(0, BlockCount).Contains(range);
}

template <typename TEvent>
void TVolumeAsPartitionActor::ForwardRequestToFollower(
    const TEvent& ev,
    const TActorContext& ctx,
    EReplyType replyType)
{
    auto* msg = ev->Get();
    msg->Record.MutableHeaders()->SetExactDiskIdMatch(true);

    const ui64 requestId = RequestsInProgress.AddWriteRequest(
        TRequestCtx{
            .OriginalSender = ev->Sender,
            .OriginalCookie = ev->Cookie,
            .BlockRange = BuildRequestBlockRange(*msg, OriginalBlockSize),
            .ReplyType = replyType});

    NActors::TActorId nondeliveryActor = SelfId();
    auto message = std::make_unique<NActors::IEventHandle>(
        MakeVolumeProxyServiceId(),
        SelfId(),
        ev->ReleaseBase().Release(),
        ev->Flags | NActors::IEventHandle::FlagForwardOnNondelivery,
        requestId,
        &nondeliveryActor);
    ctx.Send(std::move(message));
}

template <typename TMethod>
void TVolumeAsPartitionActor::ForwardResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (auto requestCtx = RequestsInProgress.ExtractRequest(ev->Cookie)) {
        if (requestCtx->Value.ReplyType == EReplyType::Local) {
            ctx.Send(
                requestCtx->Value.OriginalSender,
                new TEvService::TEvWriteBlocksLocalResponse(
                    msg->Record.GetError()),
                0,   // flags
                requestCtx->Value.OriginalCookie);
        } else {
            ctx.Send(
                requestCtx->Value.OriginalSender,
                ev->ReleaseBase().Release(),
                0,   // flags
                requestCtx->Value.OriginalCookie);
        }
    } else {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s RequestCtx for %s not found %lu",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            ev->Cookie);
    }

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

template <typename TMethod>
void TVolumeAsPartitionActor::ReplyUndelivery(
    const TActorContext& ctx,
    ui64 cookie)
{
    if (auto requestCtx = RequestsInProgress.ExtractRequest(cookie)) {
        auto message = TStringBuilder() << "Undelivery " << TMethod::Name << " "
                                        << requestCtx->Value.BlockRange.Print();

        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            message.c_str());

        if (requestCtx->Value.ReplyType == EReplyType::Local) {
            ctx.Send(
                requestCtx->Value.OriginalSender,
                std::make_unique<
                    TEvService::TWriteBlocksLocalMethod::TResponse>(
                    MakeError(E_REJECTED, std::move(message))),
                0,   // flags
                requestCtx->Value.OriginalCookie);
        } else {
            ctx.Send(
                requestCtx->Value.OriginalSender,
                std::make_unique<typename TMethod::TResponse>(
                    MakeError(E_REJECTED, std::move(message))),
                0,   // flags
                requestCtx->Value.OriginalCookie);
        }
    } else {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s RequestCtx for undelivered %s not found %lu",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            cookie);
    }

    if (RequestsInProgress.Empty() && Poisoner) {
        ReplyAndDie(ctx);
    }
}

template <typename TMethod>
void TVolumeAsPartitionActor::ReplyInvalidState(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const bool isError = State == EState::Error;
    auto message = TStringBuilder()
                   << "Can't " << TMethod::Name
                   << " to follower disk. State: " << ToString(State);

    LOG_LOG(
        ctx,
        isError ? NActors::NLog::PRI_ERROR : NActors::NLog::PRI_WARN,
        TBlockStoreComponents::PARTITION,
        "%s %s",
        LogTitle.GetWithTime().c_str(),
        message.c_str());

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<typename TMethod::TResponse>(
            MakeError(isError ? E_ABORTED : E_REJECTED, std::move(message))));
}

template <typename TMethod>
void TVolumeAsPartitionActor::ReplyInvalidRange(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx,
    EReplyType replyType)
{
    auto message =
        TStringBuilder()
        << "Can't " << TMethod::Name << " to follower disk. Block out of range "
        << BuildRequestBlockRange(*ev->Get(), OriginalBlockSize).Print();

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s %s",
        LogTitle.GetWithTime().c_str(),
        message.c_str());

    if (replyType == EReplyType::Local) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TWriteBlocksLocalMethod::TResponse>(
                MakeError(E_ARGUMENT, std::move(message))));
    } else {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<typename TMethod::TResponse>(
                MakeError(E_ARGUMENT, std::move(message))));
    }
}

void TVolumeAsPartitionActor::DoWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx,
    EReplyType replyType)
{
    auto* msg = ev->Get();

    const TBlockRange64 originalRange =
        BuildRequestBlockRange(*msg, OriginalBlockSize);
    const ui64 offset = originalRange.Start * OriginalBlockSize;
    const ui64 size = originalRange.Size() * OriginalBlockSize;
    const bool needReadModifyWrite = ((offset % BlockSize) != 0) || ((size % BlockSize) != 0);

    if (needReadModifyWrite) {
        auto message = TStringBuilder()
                       << "Can't WriteBlocks to follower disk. Need implement "
                          "read-modify-write "
                       << originalRange.Print();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            message.c_str());

        if (replyType == EReplyType::Local) {
            ctx.Send(
                ev->Sender,
                std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                    MakeError(E_NOT_IMPLEMENTED, std::move(message))),
                0,   // flags
                ev->Cookie);
        } else {
            ctx.Send(
                ev->Sender,
                std::make_unique<TEvService::TEvWriteBlocksResponse>(
                    MakeError(E_NOT_IMPLEMENTED, std::move(message))),
                0,   // flags
                ev->Cookie);
        }

        return;
    }

    const TBlockRange64 destRange =
        needReadModifyWrite
            ? TBlockRange64()
            : TBlockRange64::WithLength(offset / BlockSize, size / BlockSize);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s HandleWriteBlocks %s -> %s %s",
        LogTitle.GetWithTime().c_str(),
        originalRange.Print().c_str(),
        destRange.Print().c_str(),
        needReadModifyWrite ? " with RMW" : "");

    if (!CheckRange(destRange)) {
        ReplyInvalidRange<TEvService::TWriteBlocksMethod>(ev, ctx, replyType);
        return;
    }

    if (OriginalBlockSize != BlockSize) {
        NProto::TIOVector data;
        TSgList sglist = ResizeIOVector(data, destRange.Size(), BlockSize);
        const size_t byteCount = CopyToSgList(
            msg->Record.GetBlocks(),
            OriginalBlockSize,
            sglist,
            BlockSize);
        Y_DEBUG_ABORT_UNLESS(byteCount == destRange.Size() * BlockSize);

        msg->Record.SetStartIndex(destRange.Start);
        msg->Record.MutableBlocks()->Swap(&data);
    }

    msg->Record.SetDiskId(DiskId);
    msg->Record.MutableHeaders()->SetClientId(TString(CopyVolumeClientId));

    ForwardRequestToFollower(ev, ctx, replyType);
}

void TVolumeAsPartitionActor::ReplyAndDie(const NActors::TActorContext& ctx)
{
    NCloud::Reply(ctx, *Poisoner, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

void TVolumeAsPartitionActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "%s Volume %s: describe failed: %s",
            LogTitle.GetWithTime().c_str(),
            DiskId.Quote().c_str(),
            FormatError(error).c_str());
        State = EState::Error;
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();
    NProto::TVolume volume;
    VolumeConfigToVolume(volumeConfig, volume);

    BlockCount = volume.GetBlocksCount();
    BlockSize = volume.GetBlockSize();

    /*
    TODO(drbasic) optimize DiskRegistry-based src and destination.
    if (IsDiskRegistryMediaKind(volume.GetStorageMediaKind())) {
        GetDeviceForRangeCompanion.SetDelegate(MakeVolumeProxyServiceId());
    }
    */

    State = EState::Ready;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s TVolumeAsPartitionActor ready to work (blockSizes: %lu -> %lu)",
        LogTitle.GetWithTime().c_str(),
        OriginalBlockSize,
        BlockSize);
}

void TVolumeAsPartitionActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Sender == SelfId()) {
        ReplyUndelivery<TEvService::TWriteBlocksMethod>(ctx, ev->Cookie);
        return;
    }

    if (State != EState::Ready) {
        ReplyInvalidState<TEvService::TWriteBlocksMethod>(ev, ctx);
        return;
    }

    DoWriteBlocks(ev, ctx, EReplyType::Ordinary);
}

void TVolumeAsPartitionActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Sender == SelfId()) {
        ReplyUndelivery<TEvService::TWriteBlocksLocalMethod>(ctx, ev->Cookie);
        return;
    }

    if (State != EState::Ready) {
        ReplyInvalidState<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
        return;
    }

    auto guard = msg->Record.Sglist.Acquire();
    if (!guard) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(MakeError(
                E_CANCELLED,
                "Can't WriteBlocksLocal to follower disk. Failed to acquire "
                "sglist.")));
        return;
    }

    auto writeRequest = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    TSgList dst = ResizeIOVector(
        *msg->Record.MutableBlocks(),
        msg->Record.BlocksCount,
        msg->Record.BlockSize);

    writeRequest->Record.Swap(&msg->Record);
    const size_t bytesWritten = SgListCopy(guard.Get(), dst);
    Y_ABORT_UNLESS(
        bytesWritten ==
        static_cast<size_t>(msg->Record.BlocksCount) * msg->Record.BlockSize);

    NActors::TActorId nondeliveryActor = ev->GetForwardOnNondeliveryRecipient();
    TAutoPtr<::NActors::IEventHandle> newEv(new NActors::IEventHandle(
        ev->Recipient,
        ev->Sender,
        writeRequest.release(),
        ev->Flags,
        ev->Cookie,
        ev->Flags & NActors::IEventHandle::FlagForwardOnNondelivery
            ? &nondeliveryActor
            : nullptr));

    DoWriteBlocks(
        IEventHandle::Downcast<TEvService::TEvWriteBlocksRequest>(
            std::move(newEv)),
        ctx,
        EReplyType::Local);
}

void TVolumeAsPartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Sender == SelfId()) {
        ReplyUndelivery<TEvService::TZeroBlocksMethod>(ctx, ev->Cookie);
        return;
    }

    if (State != EState::Ready) {
        ReplyInvalidState<TEvService::TZeroBlocksMethod>(ev, ctx);
        return;
    }

    const TBlockRange64 originalRange =
        BuildRequestBlockRange(*msg, OriginalBlockSize);
    const ui64 offset = originalRange.Start * OriginalBlockSize;
    const ui64 size = originalRange.Size() * OriginalBlockSize;
    const bool needReadModifyWrite = ((offset % BlockSize) != 0) || ((size % BlockSize) != 0);

    if (needReadModifyWrite) {
        auto message = TStringBuilder() << "Can't ZeroBlocks to follower disk. "
                                           "Need implement read-modify-write "
                                        << originalRange.Print();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            message.c_str());

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvZeroBlocksResponse>(
                MakeError(E_NOT_IMPLEMENTED, std::move(message))));
        return;
    }

    const TBlockRange64 destRange =
        needReadModifyWrite
            ? TBlockRange64()
            : TBlockRange64::WithLength(offset / BlockSize, size / BlockSize);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s HandleZeroBlocks %s -> %s %s",
        LogTitle.GetWithTime().c_str(),
        originalRange.Print().c_str(),
        destRange.Print().c_str(),
        needReadModifyWrite ? " with RMW" : "");

    if (!CheckRange(destRange)) {
        ReplyInvalidRange<TEvService::TZeroBlocksMethod>(
            ev,
            ctx,
            EReplyType::Ordinary);
        return;
    }

    if (OriginalBlockSize != BlockSize) {
        msg->Record.SetStartIndex(destRange.Start);
        msg->Record.SetBlocksCount(destRange.Size());
    }

    msg->Record.SetDiskId(DiskId);
    msg->Record.MutableHeaders()->SetClientId(TString(CopyVolumeClientId));

    ForwardRequestToFollower(ev, ctx, EReplyType::Ordinary);
}

void TVolumeAsPartitionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    State = EState::Zombie;

    Poisoner = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    if (!RequestsInProgress.Empty()) {
        return;
    }

    ReplyAndDie(ctx);
}

STFUNC(TVolumeAsPartitionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocks);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocal);
        HFunc(TEvService::TEvZeroBlocksRequest, HandleZeroBlocks);

        HFunc(
            TEvService::TEvWriteBlocksResponse,
            ForwardResponse<TEvService::TWriteBlocksMethod>);
        HFunc(
            TEvService::TEvWriteBlocksLocalResponse,
            ForwardResponse<TEvService::TWriteBlocksLocalMethod>);
        HFunc(
            TEvService::TEvZeroBlocksResponse,
            ForwardResponse<TEvService::TZeroBlocksMethod>);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest,
            GetDeviceForRangeCompanion.HandleGetDeviceForRange);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
