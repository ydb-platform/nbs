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

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

TVolumeAsPartitionActor::TVolumeAsPartitionActor(
        TString originalDiskId,
        ui32 originalBlockSize,
        TString diskId)
    : OriginalDiskId(std::move(originalDiskId))
    , OriginalBlockSize(originalBlockSize)
    , DiskId(std::move(diskId))
    , PoisonPillHelper(this)
{}

TVolumeAsPartitionActor::~TVolumeAsPartitionActor() = default;

void TVolumeAsPartitionActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::PARTITION,
        "TVolumeAsPartitionActor " << OriginalDiskId.Quote()
                                   << " (blockSize=" << OriginalBlockSize
                                   << ") -> " << DiskId.Quote());

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
void TVolumeAsPartitionActor::ForwardWriteRequest(
    const TEvent& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestId = RequestsInProgress.AddWriteRequest(TRequestCtx{
        .OriginalSender = ev->Sender,
        .OriginalCookie = ev->Cookie,
        .BlockRange = BuildRequestBlockRange(*msg, OriginalBlockSize)});

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
            "Volume %s: describe failed: %s",
            DiskId.Quote().data(),
            FormatError(error).data());
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
    MediaKind = volume.GetStorageMediaKind();
    /*
    if (IsDiskRegistryMediaKind(MediaKind)) {
        GetDeviceForRangeCompanion.SetDelegate(MakeVolumeProxyServiceId());
    }
    */

    State = EState::Ready;

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::PARTITION,
        "TVolumeAsPartitionActor ready to work "
            << OriginalDiskId.Quote() << " (blockSize=" << OriginalBlockSize
            << ") -> " << DiskId.Quote() << "(" << BlockCount << "*"
            << BlockSize << ")");
}

void TVolumeAsPartitionActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Sender == SelfId()) {
        HandleWriteBlocksUndelivery(ev, ctx);
        return;
    }

    auto* msg = ev->Get();

    if (State != EState::Ready) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                State == EState::Describing ? E_REJECTED : E_ABORTED,
                TStringBuilder() << "Can't write to follower disk. State: "
                                 << ToString(State))));
        return;
    }

    const TBlockRange64 originalRange =
        BuildRequestBlockRange(*msg, OriginalBlockSize);
    const ui64 offset = originalRange.Start * OriginalBlockSize;
    const ui64 size = originalRange.Size() * OriginalBlockSize;
    const bool needReadModifyWrite = (offset % BlockSize) || (size % BlockSize);
    const TBlockRange64 destRange =
        needReadModifyWrite
            ? TBlockRange64()
            : TBlockRange64::WithLength(offset / BlockSize, size / BlockSize);

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::PARTITION,
        "TVolumeAsPartitionActor::HandleWriteBlocks "
            << OriginalDiskId.Quote() << " " << originalRange.Print() << " -> "
            << DiskId.Quote() << destRange.Print()
            << (needReadModifyWrite ? " with RMW" : ""));

    if (!CheckRange(destRange)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Can't write to follower disk. Block out of range "
                    << destRange.Print())));
        return;
    }

    if (needReadModifyWrite) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                E_NOT_IMPLEMENTED,
                TStringBuilder() << "Can't write to follower disk. Need "
                                    "implement read-modify-write "
                                 << destRange.Print())));
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

    ForwardWriteRequest(ev, ctx);
}

void TVolumeAsPartitionActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Sender == SelfId()) {
        HandleWriteBlocksLocalUndelivery(ev, ctx);
        return;
    }

    auto* msg = ev->Get();

    if (State != EState::Ready) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                State == EState::Describing ? E_REJECTED : E_ABORTED,
                TStringBuilder() << "Can't write to follower disk. State: "
                                 << ToString(State))));
        return;
    }

    const TBlockRange64 originalRange =
        BuildRequestBlockRange(*msg, OriginalBlockSize);
    const ui64 offset = originalRange.Start * OriginalBlockSize;
    const ui64 size = originalRange.Size() * OriginalBlockSize;
    const bool needReadModifyWrite = (offset % BlockSize) || (size % BlockSize);
    const TBlockRange64 destRange =
        needReadModifyWrite
            ? TBlockRange64()
            : TBlockRange64::WithLength(offset / BlockSize, size / BlockSize);

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::PARTITION,
        "TVolumeAsPartitionActor::HandleWriteBlocksLocal "
            << OriginalDiskId.Quote() << " " << originalRange.Print() << " -> "
            << DiskId.Quote() << destRange.Print()
            << (needReadModifyWrite ? " with RMW" : ""));

    if (!CheckRange(destRange)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Can't write to follower disk. Block out of range "
                    << destRange.Print())));
        return;
    }

    if (needReadModifyWrite) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                E_NOT_IMPLEMENTED,
                TStringBuilder() << "Can't write to follower disk. Need "
                                    "implement read-modify-write "
                                 << destRange.Print())));
        return;
    }

    if (OriginalBlockSize == BlockSize) {
        msg->Record.SetDiskId(DiskId);
        msg->Record.MutableHeaders()->SetClientId(TString(CopyVolumeClientId));
        ForwardMessageToActor(ev, ctx, MakeVolumeProxyServiceId());
        return;
    }

    auto guard = msg->Record.Sglist.Acquire();
    if (!guard) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                E_CANCELLED,
                "Can't write to follower disk. Failed to acquire sglist.")));
    }

    if (OriginalBlockSize > BlockSize) {
        auto sgListOrError = SgListNormalize(guard.Get(), BlockSize);
        if (HasError(sgListOrError)) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvWriteBlocksResponse>(
                    sgListOrError.GetError()));
            return;
        }
        msg->Record.Sglist.SetSgList(std::move(sgListOrError.ExtractResult()));
    } else {
        auto newSgList = ResizeIOVector(
            *msg->Record.MutableBlocks(),
            destRange.Size(),
            BlockSize);
        auto byteCount = SgListCopy(guard.Get(), newSgList);
        Y_DEBUG_ABORT_UNLESS(byteCount == destRange.Size() * BlockSize);
        msg->Record.Sglist.SetSgList(std::move(newSgList));
    }

    msg->Record.SetStartIndex(destRange.Start);
    msg->Record.BlocksCount = destRange.Size();
    msg->Record.BlockSize = BlockSize;
    msg->Record.SetDiskId(DiskId);
    msg->Record.MutableHeaders()->SetClientId(TString(CopyVolumeClientId));

    ForwardWriteRequest(ev, ctx);
}

void TVolumeAsPartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Sender == SelfId()) {
        HandleZeroBlocksUndelivery(ev, ctx);
        return;
    }

    auto* msg = ev->Get();

    if (State != EState::Ready) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                State == EState::Describing ? E_REJECTED : E_ABORTED,
                TStringBuilder() << "Can't write to follower disk. State: "
                                 << ToString(State))));
        return;
    }

    const TBlockRange64 originalRange =
        BuildRequestBlockRange(*msg, OriginalBlockSize);
    const ui64 offset = originalRange.Start * OriginalBlockSize;
    const ui64 size = originalRange.Size() * OriginalBlockSize;
    const bool needReadModifyWrite = (offset % BlockSize) || (size % BlockSize);
    const TBlockRange64 destRange =
        needReadModifyWrite
            ? TBlockRange64()
            : TBlockRange64::WithLength(offset / BlockSize, size / BlockSize);

    LOG_DEBUG_S(
        ctx,
        TBlockStoreComponents::PARTITION,
        "TVolumeAsPartitionActor::HandleZeroBlocks "
            << OriginalDiskId.Quote() << " " << originalRange.Print() << " -> "
            << DiskId.Quote() << destRange.Print()
            << (needReadModifyWrite ? " with RMW" : ""));

    if (!CheckRange(destRange)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Can't write zeroes to follower disk. "
                                    "Block out of range "
                                 << destRange.Print())));
        return;
    }

    if (needReadModifyWrite) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(
                E_NOT_IMPLEMENTED,
                TStringBuilder() << "Can't write zeroes to follower disk. Need "
                                    "implement read-modify-write "
                                 << destRange.Print())));
        return;
    }

    if (OriginalBlockSize != BlockSize) {
        msg->Record.SetStartIndex(destRange.Start);
        msg->Record.SetBlocksCount(destRange.Size());
    }

    msg->Record.SetDiskId(DiskId);
    msg->Record.MutableHeaders()->SetClientId(TString(CopyVolumeClientId));

    ForwardWriteRequest(ev, ctx);
}

void TVolumeAsPartitionActor::HandleWriteBlocksUndelivery(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestCtx = RequestsInProgress.ExtractRequest(ev->Cookie);
    auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>(
        MakeError(E_REJECTED, "Undelivered"));

    ctx.Send(
        requestCtx->Value.OriginalSender,
        response.release(),
        0,   // flags
        requestCtx->Value.OriginalCookie);
}

void TVolumeAsPartitionActor::HandleWriteBlocksLocalUndelivery(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestCtx = RequestsInProgress.ExtractRequest(ev->Cookie);
    auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
        MakeError(E_REJECTED, "Undelivered"));

    ctx.Send(
        requestCtx->Value.OriginalSender,
        response.release(),
        0,   // flags
        requestCtx->Value.OriginalCookie);
}

void TVolumeAsPartitionActor::HandleZeroBlocksUndelivery(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestCtx = RequestsInProgress.ExtractRequest(ev->Cookie);
    auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>(
        MakeError(E_REJECTED, "Undelivered"));

    ctx.Send(
        requestCtx->Value.OriginalSender,
        response.release(),
        0,   // flags
        requestCtx->Value.OriginalCookie);
}

void TVolumeAsPartitionActor::HandleWriteBlocksResponse(
    const TEvService::TEvWriteBlocksResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestCtx = RequestsInProgress.ExtractRequest(ev->Cookie);
    ctx.Send(
        requestCtx->Value.OriginalSender,
        ev.Release()->Release(),
        0,   // flags
        requestCtx->Value.OriginalCookie);
}

void TVolumeAsPartitionActor::HandleWriteBlocksLocalResponse(
    const TEvService::TEvWriteBlocksLocalResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestCtx = RequestsInProgress.ExtractRequest(ev->Cookie);
    ctx.Send(
        requestCtx->Value.OriginalSender,
        ev.Release()->Release(),
        0,   // flags
        requestCtx->Value.OriginalCookie);
}

void TVolumeAsPartitionActor::HandleZeroBlocksResponse(
    const TEvService::TEvZeroBlocksResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto requestCtx = RequestsInProgress.ExtractRequest(ev->Cookie);
    ctx.Send(
        requestCtx->Value.OriginalSender,
        ev.Release()->Release(),
        0,   // flags
        requestCtx->Value.OriginalCookie);
}

void TVolumeAsPartitionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    PoisonPillHelper.HandlePoisonPill(ev, ctx);
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

        HFunc(TEvService::TEvWriteBlocksResponse, HandleWriteBlocksResponse);
        HFunc(
            TEvService::TEvWriteBlocksLocalResponse,
            HandleWriteBlocksLocalResponse);
        HFunc(TEvService::TEvZeroBlocksResponse, HandleZeroBlocksResponse);

        HFunc(TEvents::TEvPoisonPill, PoisonPillHelper.HandlePoisonPill);

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
