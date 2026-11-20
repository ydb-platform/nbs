#include "volume_as_partition_actor.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <type_traits>

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

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TVolumeAsPartitionActor::TVolumeAsPartitionActor(
        TChildLogTitle logTitle,
        ui32 originalBlockSize,
        TString diskId,
        TDuration shutdownTimeout,
        TLeaderFollowerLink link)
    : LogTitle(std::move(logTitle))
    , OriginalBlockSize(originalBlockSize)
    , DiskId(std::move(diskId))
    , ShutdownTimeout(shutdownTimeout)
    , Link(std::move(link))
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

    if (IsRemote()) {
        RequestRemoteResources(ctx);
        return;
    }

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId));
}

bool TVolumeAsPartitionActor::IsRemote() const
{
    return Link.FollowerCellId && Link.LeaderCellId != Link.FollowerCellId;
}

void TVolumeAsPartitionActor::RequestRemoteResources(const TActorContext& ctx)
{
    NCloud::Send(ctx, MakeCellProxyServiceId(),
        std::make_unique<TEvCellProxy::TEvDescribeVolumeRequest>(
            Link.FollowerCellId, DiskId));
}

void TVolumeAsPartitionActor::HandleRemoteError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (State == EState::Zombie) {
        return;
    }
    LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
        "%s Remote follower is unavailable: %s",
        LogTitle.GetWithTime().c_str(), FormatError(error).c_str());
    if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        State = EState::Describing;
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    } else {
        State = EState::Error;
    }
}

void TVolumeAsPartitionActor::HandleRemoteDescribe(
    const TEvCellProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (State == EState::Zombie) {
        return;
    }
    if (HasError(ev->Get()->GetError())) {
        HandleRemoteError(ctx, ev->Get()->GetError());
        return;
    }
    BlockCount = ev->Get()->Volume.GetBlocksCount();
    BlockSize = ev->Get()->Volume.GetBlockSize();
    if (!BlockSize || !BlockCount || ev->Get()->Volume.GetDiskId() != DiskId) {
        HandleRemoteError(ctx, MakeError(E_ARGUMENT, "Invalid follower geometry"));
        return;
    }
    NCloud::Send(ctx, MakeCellProxyServiceId(),
        std::make_unique<TEvCellProxy::TEvGetStorageRequest>(
            Link.FollowerCellId));
}

void TVolumeAsPartitionActor::HandleRemoteStorage(
    const TEvCellProxy::TEvGetStorageResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (State == EState::Zombie) {
        return;
    }
    if (HasError(ev->Get()->GetError()) || !ev->Get()->Storage) {
        HandleRemoteError(ctx, HasError(ev->Get()->GetError())
            ? ev->Get()->GetError()
            : MakeError(E_NOT_IMPLEMENTED, "Cell storage is empty"));
        return;
    }
    RemoteStorage = ev->Get()->Storage;
    State = EState::Ready;
}

bool TVolumeAsPartitionActor::CheckRange(TBlockRange64 range) const
{
    return TBlockRange64::WithLength(0, BlockCount).Contains(range);
}

template <typename TEvent>
void TVolumeAsPartitionActor::ForwardRequestToFollower(
    const TEvent& ev,
    const TActorContext& ctx,
    ERequestType requestType)
{
    auto* msg = ev->Get();
    msg->Record.MutableHeaders()->SetExactDiskIdMatch(true);

    const ui64 requestId = RequestsInProgress.AddWriteRequest(
        BuildRequestBlockRange(*msg, OriginalBlockSize),
        TRequestCtx{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .RequestType = requestType});

    if (IsRemote()) {
        auto* headers = msg->Record.MutableHeaders();
        headers->ClearInternal();
        headers->SetCellId(Link.FollowerCellId);
        headers->SetCopyLinkUUID(Link.LinkUUID);
        headers->SetRequestId(msg->CallContext->RequestId);
        const auto error = SafeExecute<NProto::TError>([&]
        {
            if constexpr (std::is_same_v<
                std::decay_t<decltype(msg->Record)>, NProto::TZeroBlocksRequest>)
            {
                auto request = std::make_shared<NProto::TZeroBlocksRequest>(
                    msg->Record);
                RemoteStorage->ZeroBlocks(msg->CallContext, request).Subscribe(
                    [system = ctx.ActorSystem(), self = SelfId(), requestId,
                     request, storage = RemoteStorage](const auto& future)
                    {
                        Y_UNUSED(request);
                        Y_UNUSED(storage);
                        auto response =
                            std::make_unique<TEvService::TEvZeroBlocksResponse>();
                        response->Record = ExtractResponse(future);
                        system->Send(new IEventHandle(
                            self, {}, response.release(), 0, requestId));
                    });
            } else {
                auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
                request->CopyFrom(msg->Record);
                request->SetBlockSize(BlockSize);
                request->BlocksCount = BuildRequestBlockRange(*msg, BlockSize).Size();
                TSgList sglist;
                for (const auto& buffer: request->GetBlocks().GetBuffers()) {
                    sglist.emplace_back(buffer.data(), buffer.size());
                }
                request->Sglist = TGuardedSgList(std::move(sglist));
                RemoteStorage->WriteBlocksLocal(msg->CallContext, request).Subscribe(
                    [system = ctx.ActorSystem(), self = SelfId(), requestId,
                     request, storage = RemoteStorage](const auto& future)
                    {
                        Y_UNUSED(request);
                        Y_UNUSED(storage);
                        const auto result = ExtractResponse(future);
                        auto response =
                            std::make_unique<TEvService::TEvWriteBlocksResponse>();
                        *response->Record.MutableError() = result.GetError();
                        *response->Record.MutableHeaders() = result.GetHeaders();
                        system->Send(new IEventHandle(
                            self, {}, response.release(), 0, requestId));
                    });
            }
            return MakeError(S_OK);
        });
        if (HasError(error)) {
            if constexpr (std::is_same_v<
                std::decay_t<decltype(msg->Record)>, NProto::TZeroBlocksRequest>)
            {
                NCloud::Send(ctx, SelfId(),
                    std::make_unique<TEvService::TEvZeroBlocksResponse>(error),
                    requestId);
            } else {
                NCloud::Send(ctx, SelfId(),
                    std::make_unique<TEvService::TEvWriteBlocksResponse>(error),
                    requestId);
            }
        }
        return;
    }

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
        if (requestCtx->Value.RequestType == ERequestType::WriteBlocksLocal) {
            NCloud::Reply(
                ctx,
                requestCtx->Value,
                std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                    msg->Record.GetError()));
        } else {
            NCloud::Reply(
                ctx,
                requestCtx->Value,
                NActors::IEventBasePtr(ev->ReleaseBase().Release()));
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
                                        << requestCtx->Range.Print();

        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s %s",
            LogTitle.GetWithTime().c_str(),
            message.c_str());

        if (requestCtx->Value.RequestType == ERequestType::WriteBlocksLocal) {
            NCloud::Reply(
                ctx,
                requestCtx->Value,
                std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                    MakeError(E_REJECTED, std::move(message))));
        } else {
            NCloud::Reply(
                ctx,
                requestCtx->Value,
                std::make_unique<typename TMethod::TResponse>(
                    MakeError(E_REJECTED, std::move(message))));
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
    ERequestType requestType)
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

    if (requestType == ERequestType::WriteBlocksLocal) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
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
    ERequestType requestType)
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

        auto error = MakeError(E_NOT_IMPLEMENTED, std::move(message));
        if (requestType == ERequestType::WriteBlocksLocal) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                    std::move(error)));
        } else {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvWriteBlocksResponse>(
                    std::move(error)));
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
        ReplyInvalidRange<TEvService::TWriteBlocksMethod>(ev, ctx, requestType);
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

    ForwardRequestToFollower(ev, ctx, requestType);
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
    VolumeConfigToVolume(volumeConfig, "", volume);

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

    DoWriteBlocks(ev, ctx, ERequestType::WriteBlocks);
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
        msg->Record.GetBlockSize());

    const size_t bytesToWrite = static_cast<size_t>(msg->Record.BlocksCount) *
                                msg->Record.GetBlockSize();
    writeRequest->Record.Swap(&msg->Record);
    const size_t bytesWritten = SgListCopy(guard.Get(), dst);
    Y_ABORT_UNLESS(bytesWritten == bytesToWrite);

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
        ERequestType::WriteBlocksLocal);
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
            ERequestType::ZeroBlocks);
        return;
    }

    if (OriginalBlockSize != BlockSize) {
        msg->Record.SetStartIndex(destRange.Start);
        msg->Record.SetBlocksCount(destRange.Size());
    }

    msg->Record.SetDiskId(DiskId);
    msg->Record.MutableHeaders()->SetClientId(TString(CopyVolumeClientId));

    ForwardRequestToFollower(
        ev,
        ctx,
        ERequestType::ZeroBlocks);
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
        ctx.Schedule(ShutdownTimeout, new TEvents::TEvWakeup());
        return;
    }

    ReplyAndDie(ctx);
}

void TVolumeAsPartitionActor::CancelRequests(const TActorContext& ctx)
{
    TVector<ui64> requestIds;
    requestIds.reserve(RequestsInProgress.GetRequestCount());
    RequestsInProgress.EnumerateRequests(
        [&](ui64 requestId,
            bool write,
            TBlockRange64 range,
            const TRequestCtx& requestCtx)
        {
            Y_UNUSED(write);
            Y_UNUSED(range);
            Y_UNUSED(requestCtx);
            requestIds.push_back(requestId);
        });

    for (const ui64 requestId: requestIds) {
        auto request = RequestsInProgress.ExtractRequest(requestId);
        Y_DEBUG_ABORT_UNLESS(request);
        if (!request) {
            continue;
        }

        const auto& requestCtx = request->Value;
        // The requester may still be alive, so return E_REJECTED to allow it to
        // retry the request.
        auto error = MakeError(
            E_REJECTED,
            "Follower request timed out during shutdown");

        switch (requestCtx.RequestType) {
            case ERequestType::WriteBlocks:
                NCloud::Reply(
                    ctx,
                    requestCtx,
                    std::make_unique<TEvService::TEvWriteBlocksResponse>(
                        std::move(error)));
                break;
            case ERequestType::WriteBlocksLocal:
                NCloud::Reply(
                    ctx,
                    requestCtx,
                    std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
                        std::move(error)));
                break;
            case ERequestType::ZeroBlocks:
                NCloud::Reply(
                    ctx,
                    requestCtx,
                    std::make_unique<TEvService::TEvZeroBlocksResponse>(
                        std::move(error)));
                break;
        }
    }
}

void TVolumeAsPartitionActor::HandleShutdownTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (State == EState::Describing && IsRemote()) {
        RequestRemoteResources(ctx);
        return;
    }

    if (State != EState::Zombie || !Poisoner) {
        return;
    }

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Cancelling %lu follower requests during shutdown",
        LogTitle.GetWithTime().c_str(),
        RequestsInProgress.GetRequestCount());

    CancelRequests(ctx);
    ReplyAndDie(ctx);
}

STFUNC(TVolumeAsPartitionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(TEvCellProxy::TEvDescribeVolumeResponse, HandleRemoteDescribe);
        HFunc(TEvCellProxy::TEvGetStorageResponse, HandleRemoteStorage);

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
        HFunc(TEvents::TEvWakeup, HandleShutdownTimeout);

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
