#include "read_disk_registry_based_overlay.h"

#include <cloud/storage/core/libs/common/helpers.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    NBlobMarkers::TBlobMarkOnBaseDisk BlobMark;
    TBlockDataRef DataRef;
};

bool operator < (const TRequest& lsv, const TRequest& rsv)
{
    return lsv.BlobMark.BlobId < rsv.BlobMark.BlobId
        || lsv.BlobMark.BlobId == rsv.BlobMark.BlobId
        && lsv.BlobMark.BlobOffset < rsv.BlobMark.BlobOffset;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
TReadDiskRegistryBasedOverlayActor<TMethod>::TReadDiskRegistryBasedOverlayActor(
        TRequestInfoPtr requestInfo,
        TRequest originalRequest,
        const TCompressedBitmap& usedBlocks,
        TActorId volumeActorId,
        TActorId partActorId,
        ui64 volumeTabletId,
        TString baseDiskId,
        TString baseDiskCheckpointId,
        ui32 blockSize,
        EStorageAccessMode mode,
        TDuration longRunningThreshold)
    : RequestInfo(std::move(requestInfo))
    , OriginalRequest(std::move(originalRequest))
    , VolumeActorId(volumeActorId)
    , PartActorId(partActorId)
    , VolumeTabletId(volumeTabletId)
    , BaseDiskId(std::move(baseDiskId))
    , BaseDiskCheckpointId(std::move(baseDiskCheckpointId))
    , BlockSize(blockSize)
    , LongRunningThreshold(longRunningThreshold)
    , Mode(mode)
    , BlockMarks(MakeUsedBlockMarks(
          usedBlocks,
          TBlockRange64::WithLength(
              OriginalRequest.GetStartIndex(),
              OriginalRequest.GetBlocksCount())))
{
    if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>) {
        ReadHandler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(
                OriginalRequest.GetStartIndex(),
                OriginalRequest.GetBlocksCount()),
            OriginalRequest.Sglist,
            blockSize);
    } else {
        ReadHandler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(
                OriginalRequest.GetStartIndex(),
                OriginalRequest.GetBlocksCount()),
            blockSize);
    }
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    TBase::Become(&TBase::TThis::StateWork);

    GLOBAL_LWTRACK(
        BLOCKSTORE_STORAGE_PROVIDER,
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    if (InitRequests()) {
        ReadBlocks(ctx);
    } else {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);
        auto error = MakeError(E_CANCELLED, "failed to acquire sglist", flags);

        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s %s",
            VolumeTabletId,
            TMethod::Name,
            FormatError(error).c_str());

        ReplyAndDie(ctx, error);
    }
}

template <ReadRequest TMethod>
bool TReadDiskRegistryBasedOverlayActor<TMethod>::InitRequests()
{
    TVector<ui64> baseDiskBlockIndices;
    baseDiskBlockIndices.reserve(BlockMarks.size());

    TVector<ui64> overlayDiskIndices;
    overlayDiskIndices.reserve(BlockMarks.size());

    for (size_t i = 0; i < BlockMarks.size(); ++i) {
        if (std::holds_alternative<NBlobMarkers::TEmptyMark>(BlockMarks[i])) {
            baseDiskBlockIndices.push_back(OriginalRequest.GetStartIndex() + i);
        } else {
            overlayDiskIndices.push_back(OriginalRequest.GetStartIndex() + i);
        }
    }

    return InitRequest(baseDiskBlockIndices, true, BaseDiskRequest) &&
        InitRequest(overlayDiskIndices, false, OverlayDiskRequest);
}

template <ReadRequest TMethod>
bool TReadDiskRegistryBasedOverlayActor<TMethod>::InitRequest(
    const TVector<ui64>& blockIndices,
    bool isBaseDisk,
    NProto::TReadBlocksLocalRequest& request)
{
    if (blockIndices.empty()) {
        return true;
    }

    auto sgList = ReadHandler->GetGuardedSgList(blockIndices, isBaseDisk);
    auto sgListGuard = sgList.Acquire();
    if (!sgListGuard) {
        return false;
    }

    TSgList resultSgList(
        blockIndices.back() - blockIndices.front() + 1,
        TBlockDataRef::CreateZeroBlock(BlockSize));
    const auto& sgListRef = sgListGuard.Get();
    for (size_t i = 0; i < blockIndices.size(); ++i) {
        resultSgList[blockIndices[i] - blockIndices.front()] =
            sgListRef[i];
    }

    static_cast<NProto::TReadBlocksRequest&>(request) =
        static_cast<const NProto::TReadBlocksRequest&>(OriginalRequest);
    request.SetStartIndex(blockIndices.front());
    request.SetBlocksCount(resultSgList.size());
    request.BlockSize = BlockSize;
    request.Sglist = sgList.Create(std::move(resultSgList));

    if constexpr (std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>) {
        request.CommitId = OriginalRequest.CommitId;
    }

    return true;
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::SendOverlayDiskRequest(
    const TActorContext& ctx)
{
    ++RequestsInFlight;

    auto request = std::make_unique<
        TEvService::TReadBlocksLocalMethod::TRequest>();
    request->Record = std::move(OverlayDiskRequest);

    if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
        GLOBAL_LWTRACK(
            BLOCKSTORE_STORAGE_PROVIDER,
            ForkFailed,
            RequestInfo->CallContext->LWOrbit,
            TMethod::Name,
            RequestInfo->CallContext->RequestId);
    }

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
void TReadDiskRegistryBasedOverlayActor<TMethod>::SendBaseDiskRequest(
    const TActorContext& ctx)
{
    ++RequestsInFlight;

    auto requestInfo = CreateRequestInfo(
        TBase::SelfId(),
        RequestInfo->Cookie,
        RequestInfo->CallContext);

    NCloud::Register<TDescribeBaseDiskBlocksActor>(
        ctx,
        requestInfo,
        BaseDiskId,
        BaseDiskCheckpointId,
        TBlockRange64::WithLength(
            OriginalRequest.GetStartIndex(),
            OriginalRequest.GetBlocksCount()),
        TBlockRange64::WithLength(
            BaseDiskRequest.GetStartIndex(),
            BaseDiskRequest.GetBlocksCount()),
        std::move(BlockMarks),
        BlockSize);
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::ReadBlocks(
    const TActorContext& ctx)
{
    if (OverlayDiskRequest.GetBlocksCount() != 0) {
        SendOverlayDiskRequest(ctx);
    }

    if (BaseDiskRequest.GetBlocksCount() != 0) {
        SendBaseDiskRequest(ctx);
    }

    if (RequestsInFlight == 0) {
        ReplyAndDie(ctx, MakeError(S_FALSE)); // nothing to read
    }
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<typename TMethod::TResponse>(error);

    if (!HasError(error)) {
        if constexpr (
            std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>)
        {
            ReadHandler->GetLocalResponse(response->Record);
            ClearEmptyBlocks(BlockMarks, OriginalRequest.Sglist);
        } else {
            ReadHandler->GetResponse(response->Record);
            ClearEmptyBlocks(BlockMarks, response->Record);
        }
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    TBase::Die(ctx);
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::HandleReadBlocksLocalResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    --RequestsInFlight;

    const auto* msg = ev->Get();

    if (HasError(msg->Record)) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s got error from overlay disk: %s",
            VolumeTabletId,
            TMethod::Name,
            FormatError(msg->Record.GetError()).c_str());

        ReplyAndDie(ctx, msg->Record.GetError());
    } else if (RequestsInFlight == 0) {
        ReplyAndDie(ctx, msg->Record.GetError());
    }
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::HandleDescribeBlocksCompleted(
    const TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    --RequestsInFlight;

    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s failed to describe base disk: %s",
            VolumeTabletId,
            TMethod::Name,
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    BlockMarks = std::move(msg->BlockMarks);

    TVector<NStorage::TRequest> requests;
    if (auto guard = BaseDiskRequest.Sglist.Acquire()) {
        const auto& sgList = guard.Get();

        const size_t indexOffset =
            BaseDiskRequest.GetStartIndex() - OriginalRequest.GetStartIndex();
        for (size_t i = 0; i < BaseDiskRequest.GetBlocksCount(); ++i) {
            const size_t index = indexOffset + i;
            if (std::holds_alternative<NBlobMarkers::TFreshMarkOnBaseDisk>(BlockMarks[index])) {
                auto& value = std::get<NBlobMarkers::TFreshMarkOnBaseDisk>(BlockMarks[index]);
                ReadHandler->SetBlock(
                    value.BlockIndex,
                    value.RefToData,
                    true); // baseDisk
            } else if (std::holds_alternative<NBlobMarkers::TBlobMarkOnBaseDisk>(BlockMarks[index])) {
                auto& value = std::get<NBlobMarkers::TBlobMarkOnBaseDisk>(BlockMarks[index]);
                requests.push_back(NStorage::TRequest{value, sgList[i]});
            }
        }
    } else {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);
        auto error = MakeError(E_CANCELLED, "failed to acquire sglist", flags);

        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s %s",
            VolumeTabletId,
            TMethod::Name,
            FormatError(error).c_str());

        ReplyAndDie(ctx, error);
        return;
    }

    if (requests.empty()) {
        if (RequestsInFlight == 0) {
            ReplyAndDie(ctx, msg->GetError());
        }
        return;
    }

    Sort(requests);

    auto requestInfo = CreateRequestInfo(
        TBase::SelfId(),
        RequestInfo->Cookie,
        RequestInfo->CallContext);

    auto currentRequest = std::make_unique<
        TEvPartitionCommonPrivate::TEvReadBlobRequest>();
    currentRequest->Deadline = TInstant::Max();
    TSgList currentSgList;

    for (const auto& request: requests) {
        const auto& requestRef = request.BlobMark;
        const auto& dataRef = request.DataRef;

        if (currentRequest->BlobId != requestRef.BlobId) {
            if (currentRequest->BlobOffsets) {
                ++RequestsInFlight;
                currentRequest->Sglist = BaseDiskRequest.Sglist.Create(std::move(currentSgList));
                NCloud::Register<TReadBlobActor>(
                    ctx,
                    requestInfo,
                    TBase::SelfId(),
                    VolumeActorId,
                    VolumeTabletId,
                    BlockSize,
                    false, // shouldCalculateChecksums
                    Mode,
                    std::move(currentRequest),
                    LongRunningThreshold);
                currentRequest = std::make_unique<
                    TEvPartitionCommonPrivate::TEvReadBlobRequest>();
                currentRequest->Deadline = TInstant::Max();
            }
            currentRequest->BlobId = requestRef.BlobId;
            currentRequest->Proxy = NKikimr::MakeBlobStorageProxyID(requestRef.BSGroupId);
            currentRequest->GroupId = requestRef.BSGroupId;
        }

        currentRequest->BlobOffsets.push_back(requestRef.BlobOffset);
        currentSgList.push_back(dataRef);
    }

    if (currentRequest->BlobOffsets) {
        ++RequestsInFlight;
        currentRequest->Sglist = BaseDiskRequest.Sglist.Create(std::move(currentSgList));
        NCloud::Register<TReadBlobActor>(
            ctx,
            requestInfo,
            TBase::SelfId(),
            VolumeActorId,
            VolumeTabletId,
            BlockSize,
            false, // shouldCalculateChecksums
            EStorageAccessMode::Default,
            std::move(currentRequest),
            LongRunningThreshold);
    }
}

template <ReadRequest TMethod>
    void TReadDiskRegistryBasedOverlayActor<TMethod>::HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx)
{
    --RequestsInFlight;

    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME,
            "[%lu] %s got error from base disk: %s",
            VolumeTabletId,
            TMethod::Name,
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError());
    } else if (RequestsInFlight == 0) {
        ReplyAndDie(ctx, msg->GetError());
    }
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::HandleLongRunningBlobOperation(
    const TEvPartitionCommonPrivate::TEvLongRunningOperation::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

template <ReadRequest TMethod>
void TReadDiskRegistryBasedOverlayActor<TMethod>::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

////////////////////////////////////////////////////////////////////////////////

template <ReadRequest TMethod>
STFUNC(TReadDiskRegistryBasedOverlayActor<TMethod>::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {

        IgnoreFunc(
            TEvPartitionCommonPrivate::TEvReadBlobCompleted);

        HFunc(
            NActors::TEvents::TEvPoisonPill,
            HandlePoisonPill);
        HFunc(
            TEvService::TEvReadBlocksLocalResponse,
            HandleReadBlocksLocalResponse);
        HFunc(
            TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);
        HFunc(
            TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted,
            HandleDescribeBlocksCompleted);
        HFunc(
            TEvPartitionCommonPrivate::TEvLongRunningOperation,
            HandleLongRunningBlobOperation);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

template class TReadDiskRegistryBasedOverlayActor<TEvService::TReadBlocksLocalMethod>;
template class TReadDiskRegistryBasedOverlayActor<TEvService::TReadBlocksMethod>;

}   // namespace NCloud::NBlockStore::NStorage
